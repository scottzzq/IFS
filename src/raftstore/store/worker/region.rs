// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.


use std::fmt::{self, Formatter, Display};
use std::error;
use std::fs::File;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use std::str;

use rocksdb::{DB, Writable, WriteBatch};
use kvproto::raft_serverpb::{RaftApplyState, RegionLocalState, PeerState};

use util::worker::Runnable;
use util::codec::bytes::CompactBytesDecoder;
use util::{escape, HandyRwLock, rocksdb};
use util::transport::SendCh;
use raftstore;
use raftstore::store::engine::{Mutable, Snapshot, Iterable};
use raftstore::store::peer_storage::{JOB_STATUS_CANCEL, JOB_STATUS_PENDING, JOB_STATUS_RUNNING};
use raftstore::store::{self, SnapManager, SnapKey, SnapEntry, Msg, keys,Peekable};
use storage::CF_RAFT;

//use super::metrics::*;

/// region related task.
pub enum Task {
    Gen { region_id: u64 },
    Apply {
        region_id: u64,
        status: Arc<AtomicUsize>,
    },
    /// Destroy data between [start_key, end_key).
    ///
    /// The deletion may and may not succeed.
    Destroy {
        region_id: u64,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
    },
}

impl Task {
    pub fn destroy(region_id: u64, start_key: Vec<u8>, end_key: Vec<u8>) -> Task {
        Task::Destroy {
            region_id: region_id,
            start_key: start_key,
            end_key: end_key,
        }
    }
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Gen { region_id, .. } => write!(f, "Snap gen for {}", region_id),
            Task::Apply { region_id, .. } => write!(f, "Snap apply for {}", region_id),
            Task::Destroy { region_id, ref start_key, ref end_key } => {
                write!(f,
                       "Destroy {} [{}, {})",
                       region_id,
                       escape(&start_key),
                       escape(&end_key))
            }
        }
    }
}

quick_error! {
    #[derive(Debug)]
    enum Error {
        Abort {
            description("abort")
            display("abort")
        }
        Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("snap failed {:?}", err)
        }
    }
}

pub trait MsgSender {
    fn send(&self, msg: Msg) -> raftstore::Result<()>;
}

impl MsgSender for SendCh<Msg> {
    fn send(&self, msg: Msg) -> raftstore::Result<()> {
        SendCh::send(self, msg).map_err(|e| box_err!("{:?}", e))
    }
}

#[inline]
fn check_abort(status: &AtomicUsize) -> Result<(), Error> {
    if status.load(Ordering::Relaxed) == JOB_STATUS_CANCEL {
        return Err(Error::Abort);
    }
    Ok(())
}

// TODO: use threadpool to do task concurrently
pub struct Runner<T: MsgSender> {
    db: Arc<DB>,
    batch_size: usize,
    ch: T,
    mgr: SnapManager,
}

impl<T: MsgSender> Runner<T> {
    pub fn new(db: Arc<DB>, ch: T, mgr: SnapManager, batch_size: usize) -> Runner<T> {
        Runner {
            db: db,
            ch: ch,
            mgr: mgr,
            batch_size: batch_size,
        }
    }

    fn generate_snap(&self, region_id: u64) -> Result<(), Error> {
        // // do we need to check leader here?
        let raw_snap = Snapshot::new(self.db.clone());
        let snap = box_try!(store::do_snapshot(self.mgr.clone(), &raw_snap, region_id));
        let msg = Msg::SnapGenRes {
            region_id: region_id,
            snap: Some(snap),
        };
        if let Err(e) = self.ch.send(msg) {
            error!("failed to notify snap result of {}: {:?}", region_id, e);
        }
        Ok(())
    }

    fn handle_gen(&self, region_id: u64) {
        info!("region.rs handle_gen");
        if let Err(e) = self.generate_snap(region_id) {
            if let Err(e) = self.ch.send(Msg::SnapGenRes {
                region_id: region_id,
                snap: None,
            }) {
                panic!("failed to notify snap result of {}: {:?}", region_id, e);
            }
            error!("failed to generate snap: {:?}!!!", e);
            return;
        }
    }

    fn delete_all_in_range(&self,
                           start_key: &[u8],
                           end_key: &[u8],
                           abort: &AtomicUsize)
                           -> Result<(), Error> {
        // let mut wb = WriteBatch::new();
        // let mut size_cnt = 0;
        // for cf in self.db.cf_names() {
        //     try!(check_abort(&abort));
        //     let handle = box_try!(rocksdb::get_cf_handle(&self.db, cf));

        //     let mut it = box_try!(self.db.new_iterator_cf(cf, Some(end_key)));

        //     try!(check_abort(&abort));
        //     it.seek(start_key.into());
        //     while it.valid() {
        //         {
        //             let key = it.key();
        //             if key >= end_key {
        //                 break;
        //             }

        //             box_try!(wb.delete_cf(handle, key));
        //             size_cnt += key.len();
        //             if size_cnt >= self.batch_size {
        //                 // Can't use write_without_wal here.
        //                 // Otherwise it may cause dirty data when applying snapshot.
        //                 box_try!(self.db.write(wb));
        //                 wb = WriteBatch::new();
        //             }
        //         };
        //         try!(check_abort(&abort));
        //         if !it.next() {
        //             break;
        //         }
        //     }
        // }

        // if wb.count() > 0 {
        //     box_try!(self.db.write(wb));
        // }
        Ok(())
    }

    fn apply_snap(&self, region_id: u64, abort: Arc<AtomicUsize>) -> Result<(), Error> {
        info!("[region {}] begin apply snap data", region_id);
        try!(check_abort(&abort));
        // enum PeerState {
        //     Normal       = 0;
        //     Applying     = 1;
        //     Tombstone    = 2;
        // }

        // message RegionLocalState {
        //     optional PeerState state        = 1;
        //     optional metapb.Region region   = 2;
        // }

        // message RegionEpoch {
        //     // Conf change version, auto increment when add or remove peer
        //     optional uint64 conf_ver    = 1 [(gogoproto.nullable) = false];
        //     // Region version, auto increment when split or merge
        //     optional uint64 version     = 2 [(gogoproto.nullable) = false];
        // }

        // message Region {
        //     optional uint64 id                  = 1 [(gogoproto.nullable) = false];
        //     // Region key range [start_key, end_key).
        //     optional bytes  start_key           = 2;
        //     optional bytes  end_key             = 3;
        //     optional RegionEpoch region_epoch   = 4;
        //     repeated Peer   peers               = 5;
        // }
        
        // message Peer {      
        //     optional uint64 id          = 1 [(gogoproto.nullable) = false]; 
        //     optional uint64 store_id    = 2 [(gogoproto.nullable) = false];
        // }
        let region_key = keys::region_state_key(region_id);
        let mut region_state: RegionLocalState = match box_try!(self.db.get_msg(&region_key)) {
            Some(state) => state,
            None => return Err(box_err!("failed to get region_state from {}", escape(&region_key))),
        };

        // clear up origin data.
        let start_key = keys::enc_start_key(region_state.get_region());
        let end_key = keys::enc_end_key(region_state.get_region());
        box_try!(self.delete_all_in_range(&start_key, &end_key, &abort));

        let state_key = keys::apply_state_key(region_id);
        let apply_state: RaftApplyState = match box_try!(self.db.get_msg_cf(CF_RAFT, &state_key)) {
            Some(state) => state,
            None => return Err(box_err!("failed to get raftstate from {}", escape(&state_key))),
        };
        let term = apply_state.get_truncated_state().get_term();
        let idx = apply_state.get_truncated_state().get_index();
        let snap_key = SnapKey::new(region_id, term, idx);
        let snap_file = box_try!(self.mgr.rl().get_snap_file(&snap_key, false));
        self.mgr.wl().register(snap_key.clone(), SnapEntry::Applying);
        defer!({
            self.mgr.wl().deregister(&snap_key, &SnapEntry::Applying);
        });

        if !snap_file.exists() {
            return Err(box_err!("missing snap file {}", snap_file.path().display()));
        }
        try!(check_abort(&abort));
        box_try!(snap_file.validate());
        let mut reader = box_try!(File::open(snap_file.path()));

        let timer = Instant::now();
        // Write the snapshot into the region.
        loop {
            // TODO: avoid too many allocation
            try!(check_abort(&abort));
            let cf = box_try!(reader.decode_compact_bytes());
            if cf.is_empty() {
                break;
            }
            let handle = box_try!(rocksdb::get_cf_handle(&self.db,
                                                         unsafe { str::from_utf8_unchecked(&cf) }));
            let mut wb = WriteBatch::new();
            let mut batch_size = 0;
            loop {
                try!(check_abort(&abort));
                let key = box_try!(reader.decode_compact_bytes());
                if key.is_empty() {
                    box_try!(self.db.write(wb));
                    break;
                }
                batch_size += key.len();
                let value = box_try!(reader.decode_compact_bytes());
                batch_size += value.len();
                box_try!(wb.put_cf(handle, &key, &value));
                if batch_size >= self.batch_size {
                    box_try!(self.db.write(wb));
                    wb = WriteBatch::new();
                    batch_size = 0;
                }
            }
        }

        region_state.set_state(PeerState::Normal);
        box_try!(self.db.put_msg(&region_key, &region_state));
        snap_file.delete();
        info!("[region {}] apply new data takes {:?}",
              region_id,
              timer.elapsed());
        Ok(())
    }

    fn handle_apply(&self, region_id: u64, status: Arc<AtomicUsize>) {
        status.compare_and_swap(JOB_STATUS_PENDING, JOB_STATUS_RUNNING, Ordering::SeqCst);
        //SNAP_COUNTER_VEC.with_label_values(&["apply", "all"]).inc();
        // let apply_histogram = SNAP_HISTOGRAM.with_label_values(&["apply"]);
        // let timer = apply_histogram.start_timer();

        let (is_success, is_aborted) = match self.apply_snap(region_id, status) {
            Ok(()) => (true, false),
            Err(Error::Abort) => {
                warn!("applying snapshot for region {} is aborted.", region_id);
                (false, true)
            }
            Err(e) => {
                error!("failed to apply snap: {:?}!!!", e);
                (false, false)
            }
        };
        let msg = Msg::SnapApplyRes {
            region_id: region_id,
            is_success: is_success,
            is_aborted: is_aborted,
        };
        if let Err(e) = self.ch.send(msg) {
            panic!("failed to notify snap apply result of {}: {:?}",
                   region_id,
                   e);
        }

        // if is_aborted {
        //     SNAP_COUNTER_VEC.with_label_values(&["apply", "abort"]).inc();
        // } else if is_success {
        //     SNAP_COUNTER_VEC.with_label_values(&["apply", "success"]).inc();
        // } else {
        //     SNAP_COUNTER_VEC.with_label_values(&["apply", "fail"]).inc();
        // }
        // timer.observe_duration();
    }

    fn handle_destroy(&mut self, region_id: u64, start_key: Vec<u8>, end_key: Vec<u8>) {
        info!("[region {}] deleting data in [{}, {})",
              region_id,
              escape(&start_key),
              escape(&end_key));
        let status = AtomicUsize::new(JOB_STATUS_PENDING);
        if let Err(e) = self.delete_all_in_range(&start_key, &end_key, &status) {
            error!("failed to delete data in [{}, {}): {:?}",
                   escape(&start_key),
                   escape(&end_key),
                   e);
        }
    }
}

impl<T: MsgSender> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        match task {
            Task::Gen { region_id } => self.handle_gen(region_id),
            Task::Apply { region_id, status } => self.handle_apply(region_id, status),
            Task::Destroy { region_id, start_key, end_key } => {
                self.handle_destroy(region_id, start_key, end_key)
            }
        }
    }
}
