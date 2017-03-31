use rocksdb::{DB, WriteBatch, Writable};
use super::engine::{Snapshot as DbSnapshot, Peekable, Iterable, Mutable};
use util::rocksdb;
use std::mem;
use std::collections::{HashMap, HashSet, VecDeque};
use protobuf::RepeatedField;

use std::cell::RefCell;

use kvproto::metapb::{self, Region};
use kvproto::eraftpb::{Entry, Snapshot, ConfState, HardState};
use kvproto::raft_serverpb::{RaftSnapshotData, RaftLocalState, RegionLocalState, RaftApplyState,
                             PeerState};

use kvproto::eraftpb::{self, ConfChangeType, MessageType};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, ChangePeerRequest, CmdType,
                          AdminCmdType, Request, Response, AdminRequest, AdminResponse,
                          TransferLeaderRequest, TransferLeaderResponse, PutRequest};
use protobuf::{self, Message};

use std::time::Instant;
use super::keys::{self, enc_start_key, enc_end_key};
use util::HandyRwLock;
use util::codec::bytes::BytesEncoder;

use storage::CF_RAFT;

use raft::{self, Storage, RaftState, StorageError, Error as RaftError, Ready};
use raftstore::{Result, Error};
use std::error;
use std::sync::{self, Arc};
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::sync::atomic::{AtomicUsize, Ordering};
use util::worker::Scheduler;
//use super::engine::{Snapshot as DbSnapshot, Peekable, Iterable, Mutable};
use raftstore::store::config::Config;
use super::worker::RegionTask;
use super::{SnapFile, SnapKey, SnapEntry, SnapManager};

use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::os::unix;
use std::path::Path;
use std::mem::transmute;
use std::io::SeekFrom;
use byteorder::{ByteOrder, BigEndian, ReadBytesExt, LittleEndian};
// so that we can force the follower peer to sync the snapshot first.
// When we create a region peer, we should initialize its log term/index > 0,
// so that we can force the follower peer to sync the snapshot first.
pub const RAFT_INIT_LOG_TERM: u64 = 5;
pub const RAFT_INIT_LOG_INDEX: u64 = 5;
const MAX_SNAP_TRY_CNT: usize = 5;

pub const JOB_STATUS_PENDING: usize = 0;
pub const JOB_STATUS_RUNNING: usize = 1;
pub const JOB_STATUS_CANCEL: usize = 2;

pub struct ApplySnapResult {
    // prev_region is the region before snapshot applied.
    pub prev_region: metapb::Region,
    pub region: metapb::Region,
}

pub struct InvokeContext {
    pub raft_state: RaftLocalState,
    pub apply_state: RaftApplyState,
    pub wb: WriteBatch,
    last_term: u64,
    engine: Arc<DB>,
}

impl InvokeContext {
    pub fn new(store: &PeerStorage) -> InvokeContext {
        InvokeContext {
            raft_state: store.raft_state.clone(),
            apply_state: store.apply_state.clone(),
            wb: WriteBatch::new(),
            last_term: store.last_term,
            engine: store.engine.clone(),
        }
    }

    pub fn save_raft(&self, region_id: u64) -> Result<()> {
        let raft_cf = try!(rocksdb::get_cf_handle(&self.engine, CF_RAFT));
        try!(self.wb.put_msg_cf(raft_cf, &keys::raft_state_key(region_id), &self.raft_state));
        Ok(())
    }

    pub fn save_apply(&self, region_id: u64) -> Result<()> {
        let raft_cf = try!(rocksdb::get_cf_handle(&self.engine, CF_RAFT));
        try!(self.wb.put_msg_cf(raft_cf,
                                &keys::apply_state_key(region_id),
                                &self.apply_state));
        Ok(())
    }
}

#[derive(Debug)]
pub enum SnapState {
    Relax,
    Generating,
    Snap(Snapshot),
    Applying(Arc<AtomicUsize>),
    ApplyAborted,
    Failed,
}

impl PartialEq for SnapState {
    fn eq(&self, other: &SnapState) -> bool {
        match (self, other) {
            (&SnapState::Relax, &SnapState::Relax) |
            (&SnapState::Generating, &SnapState::Generating) |
            (&SnapState::Failed, &SnapState::Failed) |
            (&SnapState::ApplyAborted, &SnapState::ApplyAborted) => true,
            (&SnapState::Snap(ref s1), &SnapState::Snap(ref s2)) => s1 == s2,
            (&SnapState::Applying(ref b1), &SnapState::Applying(ref b2)) => {
                b1.load(Ordering::Relaxed) == b2.load(Ordering::Relaxed)
            }
            _ => false,
        }
    }
}

// When we bootstrap the region or handling split new region, we must
// call this to initialize region local state first.
pub fn write_initial_state<T: Mutable>(engine: &DB, w: &T, region_id: u64) -> Result<()> {
    let mut raft_state = RaftLocalState::new();
    raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
    raft_state.mut_hard_state().set_term(RAFT_INIT_LOG_TERM);
    raft_state.mut_hard_state().set_commit(RAFT_INIT_LOG_INDEX);

    let mut apply_state = RaftApplyState::new();
    apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
    apply_state.mut_truncated_state().set_index(RAFT_INIT_LOG_INDEX);
    apply_state.mut_truncated_state().set_term(RAFT_INIT_LOG_TERM);

    let raft_cf = try!(rocksdb::get_cf_handle(engine, CF_RAFT));
    try!(w.put_msg_cf(raft_cf, &keys::raft_state_key(region_id), &raft_state));
    try!(w.put_msg_cf(raft_cf, &keys::apply_state_key(region_id), &apply_state));

    Ok(())
}

fn init_raft_state(engine: &DB, region: &Region) -> Result<RaftLocalState> {
    Ok(match try!(engine.get_msg_cf(CF_RAFT, &keys::raft_state_key(region.get_id()))) {
        Some(s) => s,
        None => {
            let mut raft_state = RaftLocalState::new();
            if !region.get_peers().is_empty() {
                raft_state.set_last_index(RAFT_INIT_LOG_INDEX);
            }
            raft_state
        }
    })
}

fn init_apply_state(engine: &DB, region: &Region) -> Result<RaftApplyState> {
    Ok(match try!(engine.get_msg_cf(CF_RAFT, &keys::apply_state_key(region.get_id()))) {
        Some(s) => s,
        None => {
            let mut apply_state = RaftApplyState::new();
            if !region.get_peers().is_empty() {
                apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
                let state = apply_state.mut_truncated_state();
                state.set_index(RAFT_INIT_LOG_INDEX);
                state.set_term(RAFT_INIT_LOG_TERM);
            }
            apply_state
        }
    })
}

fn init_last_term(engine: &DB,
                  region: &Region,
                  raft_state: &RaftLocalState,
                  apply_state: &RaftApplyState)
                  -> Result<u64> {
    let last_idx = raft_state.get_last_index();
    if last_idx == 0 {
        return Ok(0);
    } else if last_idx == RAFT_INIT_LOG_INDEX {
        return Ok(RAFT_INIT_LOG_TERM);
    } else if last_idx == apply_state.get_truncated_state().get_index() {
        return Ok(apply_state.get_truncated_state().get_term());
    } else {
        assert!(last_idx > RAFT_INIT_LOG_INDEX);
    }
    let last_log_key = keys::raft_log_key(region.get_id(), last_idx);
    Ok(match try!(engine.get_msg_cf::<Entry>(CF_RAFT, &last_log_key)) {
        None => return Err(box_err!("entry at {} doesn't exist, may lose data.", last_idx)),
        Some(e) => e.get_term(),
    })
}


fn storage_error<E>(error: E) -> raft::Error
    where E: Into<Box<error::Error + Send + Sync>>{
    raft::Error::Store(StorageError::Other(error.into()))
}

impl From<Error> for RaftError {
    fn from(err: Error) -> RaftError {
        storage_error(err)
    }
}

impl<T> From<sync::PoisonError<T>> for RaftError {
    fn from(_: sync::PoisonError<T>) -> RaftError {
        storage_error("lock failed")
    }
}

pub struct CacheItem {
    pub offset: u64,
    pub size: u64,
}

pub struct PeerStorage {
    pub engine: Arc<DB>,
    pub region: metapb::Region,
    pub raft_state: RaftLocalState,
    pub apply_state: RaftApplyState,
    pub applied_index_term: u64,
    pub last_term: u64,
  
    pub tag: String,
    region_sched: Scheduler<RegionTask>,
    snap_state: RefCell<SnapState>,
    snap_tried_cnt: AtomicUsize,

    pub volume_file: File,
    pub volume_idx_file: File,
    pub volume_read_file: File,
    pub needle_cache: HashMap<u64, CacheItem>,
    pub volume_file_offset: u64,
}

#[inline]
pub fn first_index(state: &RaftApplyState) -> u64 {
    state.get_truncated_state().get_index() + 1
}

impl PeerStorage{
     pub fn new(engine: Arc<DB>,
               region: &metapb::Region,
               region_sched: Scheduler<RegionTask>,
               tag: String,
               region_id: u64,
               cfg: &Config)
               -> Result<PeerStorage> {
        debug!("creating storage on {} for {:?}", engine.path(), region);
        let raft_state = try!(init_raft_state(&engine, region));
        let apply_state = try!(init_apply_state(&engine, region));
        let last_term = try!(init_last_term(&engine, region, &raft_state, &apply_state));

        let mut volume_file: File;
        let mut volume_idx_file: File;
        let mut volume_read_file: File;
        let mut volume_idx_read_file: File;

        let attr = fs::metadata(&cfg.volume_index_root_path);
        let volume_file_offset:u64;
        let mut needle_cache = HashMap::new();

        match attr {
            Ok(v) => {
                if !v.is_dir() {
                    error!("volume_index_root_path:[{}] is not valid!", cfg.volume_index_root_path);
                    return Err(box_err!("volume_index_root_path is not dir"));
                } else{
                    let mut v_path: String = cfg.volume_index_root_path.clone();
                    v_path.push_str("/");
                    v_path.push_str(&region.get_id().to_string());
                    let volume_path = Path::new(&v_path);
                    match OpenOptions::new().create(true).write(true).open(volume_path) {
                        Ok(f) => {
                            volume_read_file = try!(File::open(volume_path));
                            let metadata = try!(fs::metadata(volume_path));
                            volume_file_offset = metadata.len();
                            volume_file = f;
                            volume_file.seek(SeekFrom::End(0));

                            let mut i_path: String = cfg.volume_index_root_path.clone();
                            i_path.push_str("/");
                            i_path.push_str(&region.get_id().to_string());
                            i_path.push_str(".idx");
                            let volume_idx_path = Path::new(&i_path);
                            match OpenOptions::new().create(true).write(true).open(volume_idx_path) {
                                Ok(f) => {
                                    volume_idx_file = f;
                                    volume_idx_file.seek(SeekFrom::End(0));

                                    volume_idx_read_file = try!(File::open(volume_idx_path));
                                    while true {
                                        let mut header = vec![0;24];
                                        volume_idx_read_file.read_exact(&mut header);
                                        println!("header:[{:?}]", header);
                                        let mut reader = header.as_slice();
                                        let key = try!(reader.read_u64::<BigEndian>());
                                        let offset = try!(reader.read_u64::<BigEndian>());
                                        let size = try!(reader.read_u64::<BigEndian>());
                                        debug!("Init volume idx, key:[{}] offset:[{}] size:[{}]", key, offset, size);
                                        needle_cache.insert(key, CacheItem{offset:offset, size : size});
                                        if size == 0{
                                            break;
                                        }
                                    }
                                },
                                Err(e) => {
                                    error!("create volume_idx_file failed");
                                    return Err(box_err!("create volume_idx_file failed"));
                                },
                            }
                        },
                        Err(e) => {
                            error!("create volume_file failed");
                            return Err(box_err!("create volume_file failed"));
                        },
                    }
                }
            }
            Err(e) => {
                error!("volume_index_root_path:[{}] is not exist!", cfg.volume_index_root_path);
                return Err(box_err!("volume_index_root_path is not exist!"));
            }
        }

        Ok(PeerStorage {
            engine: engine,
            region: region.clone(),
            snap_state: RefCell::new(SnapState::Relax),
            raft_state: raft_state,
            apply_state: apply_state,
            tag: tag,
            applied_index_term: RAFT_INIT_LOG_TERM,
            last_term: last_term,
            region_sched: region_sched,
            snap_tried_cnt: AtomicUsize::new(0),
            volume_file: volume_file,
            volume_idx_file: volume_idx_file,
            volume_read_file: volume_read_file,
            volume_file_offset: volume_file_offset,
            needle_cache: needle_cache,
        })
    }

    pub fn is_initialized(&self) -> bool {
        !self.region.get_peers().is_empty()
    }
    
    //初始化
    pub fn initial_state(&self) -> raft::Result<RaftState> {
        let hard_state = self.raft_state.get_hard_state().clone();
        let mut conf_state = ConfState::new();
        if hard_state == HardState::new() {
            assert!(!self.is_initialized(),
                    "peer for region {:?} is initialized but local state {:?} has empty hard \
                     state",
                    self.region,
                    self.raft_state);

            return Ok(RaftState {
                hard_state: hard_state,
                conf_state: conf_state,
            });
        }
        for p in self.region.get_peers() {
            conf_state.mut_nodes().push(p.get_id());
        }
        Ok(RaftState {
            hard_state: hard_state,
            conf_state: conf_state,
        })
    }

    // Append the given entries to the raft log using previous last index or self.last_index.
    // Return the new last index for later update. After we commit in engine, we can set last_index
    // to the return one.
    pub fn append(&mut self, ctx: &mut InvokeContext, entries: &[Entry]) -> Result<u64> {
        debug!("{} append {} entries", self.tag, entries.len());
        let prev_last_index = ctx.raft_state.get_last_index();
        if entries.len() == 0 {
            return Ok(prev_last_index);
        }
        // enum EntryType {
        //     EntryNormal     = 0;
        //     EntryConfChange = 1;
        // }
        // message Entry {
        //     optional EntryType  entry_type  = 1; 
        //     optional uint64     term        = 2; 
        //     optional uint64     index       = 3; 
        //     optional bytes      data        = 4;
        //     optional uint64     key        = 5;
        // }
        let handle = try!(rocksdb::get_cf_handle(&self.engine, CF_RAFT));
        for entry in entries {
            info!("PeerStorage append origin Entry:[{:?}]", entry);
            if !entry.has_data(){
                try!(ctx.wb.put_msg_cf(handle,
                    &keys::raft_log_key(self.get_region_id(), entry.get_index()),
                    entry));
                continue;
            }

             match entry.get_entry_type() {
                //存储请求，需要转换
                //先将Entry写入到volume中，index写入volume.idx中
                //再将key写入rocksdb中
                eraftpb::EntryType::EntryNormal => {
                    let data = entry.get_data();
                    let cmd_req = try!(protobuf::parse_from_bytes::<RaftCmdRequest>(data));
                    let requests = cmd_req.get_requests();
                    
                    let mut raft_cmd = try!(protobuf::parse_from_bytes::<RaftCmdRequest>(data));
                    let header = raft_cmd.take_header();

                    let mut new_entry = Entry::new();
                    new_entry.set_term(entry.get_term());
                    new_entry.set_index(entry.get_index());
                    new_entry.set_entry_type(entry.get_entry_type());

                    let mut new_reqs = Vec::with_capacity(requests.len());

                    for req in requests {
                        let cmd_type = req.get_cmd_type();
                        match cmd_type {
                            CmdType::Get => {
                                new_reqs.push(req.clone());
                            }
                            CmdType::Put => {
                                let (key, value) = (req.get_put().get_key(), req.get_put().get_value());
                                self.volume_file.write(value);
        
                                let needle_size = value.len() as u64;
                                let needle_offset = self.volume_file_offset;
                                let cache_item = CacheItem{
                                    offset: needle_offset,
                                    size: needle_size,
                                };
                        
                                let mut buf = vec![0;24];
                                BigEndian::write_u64(&mut buf[0..8], key);
                                BigEndian::write_u64(&mut buf[8..16], cache_item.offset);
                                BigEndian::write_u64(&mut buf[16..24], cache_item.size);

                                self.volume_idx_file.write(&buf);
                                self.volume_file_offset += needle_size;
                                self.needle_cache.insert(key, cache_item);                               

                                let mut new_put = PutRequest::new();
                                new_put.set_key(req.get_put().get_key());
                                new_put.set_offset(needle_offset);
                                new_put.set_size(needle_size);

                                let mut new_req = Request::new();
                                new_req.set_cmd_type(CmdType::Put);
                                new_req.set_put(new_put);

                                new_reqs.push(new_req);
                            }
                            CmdType::Delete => { 
                               new_reqs.push(req.clone());
                            }
                            CmdType::Snap => {
                                new_reqs.push(req.clone());
                            }
                            CmdType::Invalid => {
                                new_reqs.push(req.clone());
                            }
                         }
                    }
                    let mut new_raft_cmd = RaftCmdRequest::new();
                    new_raft_cmd.set_header(header);
                    new_raft_cmd.set_requests(RepeatedField::from_vec(new_reqs));

                    new_entry.set_data(try!(new_raft_cmd.write_to_bytes()));
                    info!("PeerStorage append Entry append new :[{:?}]", new_entry);
                    try!(ctx.wb.put_msg_cf(handle,
                                    &keys::raft_log_key(self.get_region_id(), new_entry.get_index()),
                                    &new_entry));
                    
                }
                 //ConfChange直接写入
                 eraftpb::EntryType::EntryConfChange => {
                    info!("PeerStorage append new Entry:[{:?}]", entry);
                    try!(ctx.wb.put_msg_cf(handle,
                                   &keys::raft_log_key(self.get_region_id(), entry.get_index()),
                                   entry));
                 }
            }
        }
        let e = entries.last().unwrap();
        let last_index = e.get_index();
        let last_term = e.get_term();

        // Delete any previously appended log entries which never committed.
        for i in (last_index + 1)..(prev_last_index + 1) {
            try!(ctx.wb.delete_cf(handle, &keys::raft_log_key(self.get_region_id(), i)));
        }

        ctx.raft_state.set_last_index(last_index);
        ctx.last_term = last_term;

        Ok(last_index)
    }

    pub fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        info!("Peer Storage entries, low:[{:?}] high:[{:?}]", low, high);
        try!(self.check_range(low, high));
        let mut ents = Vec::with_capacity((high - low) as usize);
        let mut total_size: u64 = 0;
        let mut next_index = low;
        let mut exceeded_max_size = false;

        let start_key = keys::raft_log_key(self.get_region_id(), low);
        let end_key = keys::raft_log_key(self.get_region_id(), high);

        let mut volume_read_file = self.volume_read_file.try_clone().unwrap();

        try!(self.engine.scan_cf(CF_RAFT,
                                 &start_key,
                                 &end_key,
                                 &mut |_, value| {
            let mut entry = Entry::new();
            try!(entry.merge_from_bytes(value));

            // May meet gap or has been compacted.
            if entry.get_index() != next_index {
                return Ok(false);
            }
            let mut new_entry = Entry::new();
            if entry.has_data(){
                match entry.get_entry_type() {
                    eraftpb::EntryType::EntryNormal => {
                        new_entry.set_term(entry.get_term());
                        new_entry.set_index(entry.get_index());
                        new_entry.set_entry_type(entry.get_entry_type());

                        let data = entry.get_data();
                        let cmd_req = try!(protobuf::parse_from_bytes::<RaftCmdRequest>(data));
                        let requests = cmd_req.get_requests();
                    
                        let mut raft_cmd = try!(protobuf::parse_from_bytes::<RaftCmdRequest>(data));
                        let header = raft_cmd.take_header();

                        let mut new_reqs = Vec::with_capacity(requests.len());

                        for req in requests {
                            let cmd_type = req.get_cmd_type();
                            match cmd_type {
                                CmdType::Get => {
                                    new_reqs.push(req.clone());
                                }
                                CmdType::Put => {
                                    let put_req = req.get_put();
                                    let key = put_req.get_key();
                                    let needle_offset = put_req.get_offset();
                                    let needle_size = put_req.get_size();

                                    let mut res = Vec::new();
                                    res = Vec::<u8>::with_capacity(needle_size as usize);
                                    unsafe { res.set_len(needle_size as usize); }
                                    try!(volume_read_file.seek(SeekFrom::Start(needle_offset)));
                                    let bytes_read = try!(volume_read_file.read(&mut res[..]));
                                    
                                    let mut new_put = PutRequest::new();
                                    new_put.set_key(key);
                                    new_put.set_value(res.to_vec());
            
                                    let mut new_req = Request::new();
                                    new_req.set_cmd_type(CmdType::Put);
                                    new_req.set_put(new_put);
            
                                    new_reqs.push(new_req);
                                }
                                CmdType::Delete => { 
                                   new_reqs.push(req.clone());
                                }
                                CmdType::Snap => {
                                    new_reqs.push(req.clone());
                                }
                                CmdType::Invalid => {
                                    new_reqs.push(req.clone());
                                }
                             }
                        }
                        let mut new_raft_cmd = RaftCmdRequest::new();
                        new_raft_cmd.set_header(header);
                        new_raft_cmd.set_requests(RepeatedField::from_vec(new_reqs));

                        new_entry.set_data(try!(new_raft_cmd.write_to_bytes()));
                    }  
                    eraftpb::EntryType::EntryConfChange => {
                        new_entry = entry;
                    }  
                }                
            }else{
                new_entry = entry;
            }   

            info!("PeerStorage entries() New Entry:[{:?}]", new_entry);
            next_index += 1;
            total_size += new_entry.compute_size() as u64;
            exceeded_max_size = total_size > max_size;

            if !exceeded_max_size || ents.is_empty() {
                ents.push(new_entry);
            }

            Ok(!exceeded_max_size)
        }));

        // If we get the correct number of entries the total size exceeds max_size, returns.
        if ents.len() == (high - low) as usize || exceeded_max_size {
            return Ok(ents);
        }

        // Here means we don't fetch enough entries.
        Err(RaftError::Store(StorageError::Unavailable))
    }


    pub fn term(&self, idx: u64) -> raft::Result<u64> {
        if idx == self.truncated_index() {
            return Ok(self.truncated_term());
        }
        try!(self.check_range(idx, idx + 1));
        if self.truncated_term() == self.last_term || idx == self.last_index() {
            return Ok(self.last_term);
        }
        let key = keys::raft_log_key(self.get_region_id(), idx);
        match try!(self.engine.get_msg_cf::<Entry>(CF_RAFT, &key)) {
            Some(entry) => Ok(entry.get_term()),
            None => Err(RaftError::Store(StorageError::Unavailable)),
        }
    }
        #[inline]
    pub fn first_index(&self) -> u64 {
        first_index(&self.apply_state)
    }

    #[inline]
    pub fn last_index(&self) -> u64 {
        self.raft_state.get_last_index()
    }

     fn check_range(&self, low: u64, high: u64) -> raft::Result<()> {
        if low > high {
            return Err(storage_error(format!("low: {} is greater that high: {}", low, high)));
        } else if low <= self.truncated_index() {
            return Err(RaftError::Store(StorageError::Compacted));
        } else if high > self.last_index() + 1 {
            return Err(storage_error(format!("entries' high {} is out of bound lastindex {}",
                                             high,
                                             self.last_index())));
        }
        Ok(())
    }

     #[inline]
    pub fn applied_index(&self) -> u64 {
        self.apply_state.get_applied_index()
    }

    #[inline]
    pub fn truncated_index(&self) -> u64 {
        self.apply_state.get_truncated_state().get_index()
    }


    #[inline]
    pub fn truncated_term(&self) -> u64 {
        self.apply_state.get_truncated_state().get_term()
    }
    
    #[inline]
    pub fn get_region_id(&self) -> u64 {
        self.region.get_id()
    }
    pub fn get_region(&self) -> &metapb::Region {
        &self.region
    }

    // Apply the peer with given snapshot.
    pub fn apply_snapshot(&self,
                          ctx: &mut InvokeContext,
                          snap: &Snapshot)
                          -> Result<ApplySnapResult> {
        info!("{} begin to apply snapshot", self.tag);

        // message RaftSnapshotData {
        //     optional metapb.Region region   = 1;
        //     optional uint64 file_size       = 2;
        //     repeated KeyValue data          = 3;
        // }
        let mut snap_data = RaftSnapshotData::new();
        try!(snap_data.merge_from_bytes(snap.get_data()));

        let region_id = self.get_region_id();

        let region = snap_data.get_region();
        if region.get_id() != region_id {
            return Err(box_err!("mismatch region id {} != {}", region_id, region.get_id()));
        }

        if self.is_initialized() {
            // we can only delete the old data when the peer is initialized.
            try!(self.clear_meta(&ctx.wb));
        }

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
        try!(write_peer_state(&ctx.wb, region, PeerState::Applying));

        let last_index = snap.get_metadata().get_index();

        ctx.raft_state.set_last_index(last_index);
        ctx.last_term = snap.get_metadata().get_term();
        
        ctx.apply_state.set_applied_index(last_index);
        // The snapshot only contains log which index > applied index, so
        // here the truncate state's (index, term) is in snapshot metadata.
        ctx.apply_state.mut_truncated_state().set_index(last_index);
        ctx.apply_state.mut_truncated_state().set_term(snap.get_metadata().get_term());

        info!("{} apply snapshot for region {:?} with state {:?} ok",
              self.tag,
              region,
              ctx.apply_state);

        Ok(ApplySnapResult {
            prev_region: self.region.clone(),
            region: region.clone(),
        })
    }

    pub fn handle_raft_ready(&mut self, ready: &Ready) -> Result<Option<ApplySnapResult>> {
        info!("peer_storage.rs handle_raft_ready");
        let mut ctx = InvokeContext::new(self);
        let mut apply_snap_res = None;
        let region_id = self.get_region_id();
        if !raft::is_empty_snap(&ready.snapshot) {
            let res = try!(self.apply_snapshot(&mut ctx, &ready.snapshot));
            apply_snap_res = Some(res);
        }
        if !ready.entries.is_empty() {
            // message HardState {
            //     optional uint64 term   = 1; 
            //     optional uint64 vote   = 2; 
            //     optional uint64 commit = 3; 
            // }
            // message RaftLocalState {
            //     optional eraftpb.HardState hard_state        = 1;
            //     optional uint64 last_index                  = 2;
            // }

            // message RaftTruncatedState {
            //     optional uint64 index    = 1;
            //     optional uint64 term     = 2;
            // }
            // message RaftApplyState {
            //     optional uint64 applied_index               = 1;
            //     optional RaftTruncatedState truncated_state = 2;
            // }

            //wb写入entries
            //修改ctx.raft_state.last_index
            //修改ctx.last_term
            //pub struct InvokeContext {
            //    pub raft_state: RaftLocalState,
            //    pub apply_state: RaftApplyState,
            //    pub wb: WriteBatch,
            //    last_term: u64,
            //    engine: Arc<DB>,
            //}
            try!(self.append(&mut ctx, &ready.entries));
        }

        // Last index is 0 means the peer is created from raft message
        // and has not applied snapshot yet, so skip persistent hard state.
        if ctx.raft_state.get_last_index() > 0 {
            if let Some(ref hs) = ready.hs {
                ctx.raft_state.set_hard_state(hs.clone());
            }
        }

        if ctx.raft_state != self.raft_state {
            try!(ctx.save_raft(region_id));
        }

        if ctx.apply_state != self.apply_state {
            try!(ctx.save_apply(region_id));
        }

        if !ctx.wb.is_empty() {
            try!(self.engine.write(ctx.wb));
        }

        self.raft_state = ctx.raft_state;
        self.apply_state = ctx.apply_state;
        self.last_term = ctx.last_term;
        // // If we apply snapshot ok, we should update some infos like applied index too.
        if let Some(res) = apply_snap_res {
            // cleanup data before scheduling apply task
            if self.is_initialized() {
                // if let Err(e) = self.clear_extra_data(&res.region) {
                //     // No need panic here, when applying snapshot, the deletion will be tried
                //     // again. But if the region range changes, like [a, c) -> [a, b) and [b, c),
                //     // [b, c) will be kept in rocksdb until a covered snapshot is applied or
                //     // store is restarted.
                //     error!("{} cleanup data fail, may leave some dirty data: {:?}",
                //            self.tag,
                //            e);
                // }
            }
            self.schedule_applying_snapshot();
            self.region = res.region.clone();
            return Ok(Some(res));
        }
        Ok(None)
    }
  
    /// Delete all meta belong to the region. Results are stored in `wb`.
    pub fn clear_meta(&self, wb: &WriteBatch) -> Result<()> {
        // let t = Instant::now();
        // let mut meta_count = 0;
        // let mut raft_count = 0;
        // let region_id = self.get_region_id();
        // let (meta_start, meta_end) = (keys::region_meta_prefix(region_id),
        //                               keys::region_meta_prefix(region_id + 1));
        // try!(self.engine.scan(&meta_start,
        //                       &meta_end,
        //                       &mut |key, _| {
        //                           try!(wb.delete(key));
        //                           meta_count += 1;
        //                           Ok(true)
        //                       }));

        // let handle = try!(rocksdb::get_cf_handle(&self.engine, CF_RAFT));
        // let (raft_start, raft_end) = (keys::region_raft_prefix(region_id),
        //                               keys::region_raft_prefix(region_id + 1));
        // try!(self.engine.scan_cf(CF_RAFT,
        //                          &raft_start,
        //                          &raft_end,
        //                          &mut |key, _| {
        //                              try!(wb.delete_cf(handle, key));
        //                              raft_count += 1;
        //                              Ok(true)
        //                          }));
        // info!("{} clear peer {} meta keys and {} raft keys, takes {:?}",
        //       self.tag,
        //       meta_count,
        //       raft_count,
        //       t.elapsed());
        Ok(())
    }

    /// Delete all data belong to the region.
    /// If return Err, data may get partial deleted.
    pub fn clear_data(&self) -> Result<()> {
        let (start_key, end_key) = (enc_start_key(self.get_region()),
                                    enc_end_key(self.get_region()));
        let region_id = self.get_region_id();
        //box_try!(self.region_sched.schedule(RegionTask::destroy(region_id, start_key, end_key)));
        Ok(())
    }

    /// Check whether the storage has finished applying snapshot.
    #[inline]
    pub fn is_applying(&self) -> bool {
        match *self.snap_state.borrow() {
            SnapState::Applying(_) |
            SnapState::ApplyAborted => true,
            _ => false,
        }
    }

    /// Check if the storage is applying a snapshot.
    #[inline]
    pub fn is_applying_snap(&self) -> bool {
        match *self.snap_state.borrow() {
            SnapState::Applying(_) => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_canceling_snap(&self) -> bool {
        match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => status.load(Ordering::Relaxed) == JOB_STATUS_CANCEL,
            _ => false,
        }
    }

    /// Cancel applying snapshot, return true if the job can be considered actually cancelled.
    pub fn cancel_applying_snap(&mut self) -> bool {
        match *self.snap_state.borrow() {
            SnapState::Applying(ref status) => {
                status.swap(JOB_STATUS_CANCEL, Ordering::Relaxed) == JOB_STATUS_PENDING
            }
            _ => false,
        }
    }

    #[inline]
    pub fn set_snap_state(&mut self, state: SnapState) {
        *self.snap_state.borrow_mut() = state
    }

    #[inline]
    pub fn is_snap_state(&self, state: SnapState) -> bool {
        *self.snap_state.borrow() == state
    }

    pub fn schedule_applying_snapshot(&mut self) {
        let status = Arc::new(AtomicUsize::new(JOB_STATUS_PENDING));
        self.set_snap_state(SnapState::Applying(status.clone()));
        let task = RegionTask::Apply {
            region_id: self.get_region_id(),
            status: status,
        };
        // TODO: gracefully remove region instead.
        self.region_sched.schedule(task).expect("snap apply job should not fail");
    }

    fn validate_snap(&self, snap: &Snapshot) -> bool {
        let idx = snap.get_metadata().get_index();
        if idx < self.truncated_index() {
            // stale snapshot, should generate again.
            info!("{} snapshot {} < {} is stale, generate again.",
                  self.tag,
                  idx,
                  self.truncated_index());
            return false;
        }

        let mut snap_data = RaftSnapshotData::new();
        if let Err(e) = snap_data.merge_from_bytes(snap.get_data()) {
            error!("{} decode snapshot fail, it may be corrupted: {:?}",
                   self.tag,
                   e);
            return false;
        }
        let snap_epoch = snap_data.get_region().get_region_epoch();
        let latest_epoch = self.get_region().get_region_epoch();
        if snap_epoch.get_conf_ver() < latest_epoch.get_conf_ver() {
            info!("{} snapshot epoch {:?} < {:?}, generate again.",
                  self.tag,
                  snap_epoch,
                  latest_epoch);
            return false;
        }

        true
    }

    pub fn snapshot(&self) -> raft::Result<Snapshot> {
        info!("peer_storage.rs snapshot");
        let mut snap_state = self.snap_state.borrow_mut();
        if let SnapState::Snap(_) = *snap_state {
            match mem::replace(&mut *snap_state, SnapState::Relax) {
                SnapState::Snap(s) => {
                    if self.validate_snap(&s) {
                        return Ok(s);
                    }
                }
                _ => unreachable!(),
            }
        }

        if SnapState::Relax == *snap_state {
            info!("{} requesting snapshot...", self.tag);
            self.snap_tried_cnt.store(0, Ordering::Relaxed);
            *snap_state = SnapState::Generating;
        } else if SnapState::Failed == *snap_state {
            let mut snap_tried_cnt = self.snap_tried_cnt.load(Ordering::Relaxed);
            if snap_tried_cnt >= MAX_SNAP_TRY_CNT {
                return Err(raft::Error::Store(box_err!("failed to get snapshot after {} times",
                                                       snap_tried_cnt)));
            }
            snap_tried_cnt += 1;
            warn!("{} snapshot generating failed, retry {} time",
                  self.tag,
                  snap_tried_cnt);
            self.snap_tried_cnt.store(snap_tried_cnt, Ordering::Relaxed);
            *snap_state = SnapState::Generating;
        } else {
            return Err(raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable));
        }
        let task = RegionTask::Gen { region_id: self.get_region_id() };
        if let Err(e) = self.region_sched.schedule(task) {
            error!("{} failed to schedule task snap generation: {:?}",
                   self.tag,
                   e);
            *snap_state = SnapState::Failed;
        }
        Err(raft::Error::Store(raft::StorageError::SnapshotTemporarilyUnavailable))
    }

} 
impl Storage for PeerStorage {
    fn initial_state(&self) -> raft::Result<RaftState> {
        self.initial_state()
    }
    fn entries(&self, low: u64, high: u64, max_size: u64) -> raft::Result<Vec<Entry>> {
        self.entries(low, high, max_size)
    }
    fn term(&self, idx: u64) -> raft::Result<u64> {
        self.term(idx)
    }
    fn first_index(&self) -> raft::Result<u64> {
        Ok(self.first_index())
    }
    fn last_index(&self) -> raft::Result<u64> {
        Ok(self.last_index())
    }
    fn snapshot(&self) -> raft::Result<Snapshot> {
        self.snapshot()
    }
}

pub fn write_peer_state<T: Mutable>(w: &T,
                                    region: &metapb::Region,
                                    state: PeerState)
                                    -> Result<()> {
    let region_id = region.get_id();
    let mut region_state = RegionLocalState::new();
    region_state.set_state(state);
    region_state.set_region(region.clone());
    try!(w.put_msg(&keys::region_state_key(region_id), &region_state));
    Ok(())
}


// pub struct SnapFile {
//     file: PathBuf,
//     digest: Digest,
//     // File is the file obj represent the tmpfile, string is the actual path to
//     // tmpfile.
//     tmp_file: Option<(File, String)>,
// }
fn build_snap_file(f: &mut SnapFile,
                   snap: &DbSnapshot,
                   region: &metapb::Region)
                   -> raft::Result<()> {
    let t = Instant::now();
    let mut snap_size = 0;
    let mut snap_key_cnt = 0;
    let (begin_key, end_key) = (enc_start_key(region), enc_end_key(region));
    for cf in snap.cf_names() {
        box_try!(f.encode_compact_bytes(cf.as_bytes()));
        try!(snap.scan_cf(cf,
                          &begin_key,
                          &end_key,
                          &mut |key, value| {
            snap_size += key.len();
            snap_size += value.len();
            snap_key_cnt += 1;
            try!(f.encode_compact_bytes(key));
            try!(f.encode_compact_bytes(value));
            Ok(true)
        }));
        // use an empty byte array to indicate that cf reaches an end.
        box_try!(f.encode_compact_bytes(b""));
    }
    // use an empty byte array to indicate that kvpair reaches an end.
    box_try!(f.encode_compact_bytes(b""));
    try!(f.save());

    info!("[region {}] scan snapshot, size {}, key count {}, takes {:?}",
          region.get_id(),
          snap_size,
          snap_key_cnt,
          t.elapsed());
    Ok(())
}

pub fn do_snapshot(mgr: SnapManager, snap: &DbSnapshot, region_id: u64) -> raft::Result<Snapshot> {
    debug!("[region {}] begin to generate a snapshot", region_id);

    // let apply_state: RaftApplyState =
    //     match try!(snap.get_msg_cf(CF_RAFT, &keys::apply_state_key(region_id))) {
    //         None => return Err(box_err!("could not load raft state of region {}", region_id)),
    //         Some(state) => state,
    //     };

    //apply 
    let mut apply_state = RaftApplyState::new();
    apply_state.set_applied_index(RAFT_INIT_LOG_INDEX);
    apply_state.mut_truncated_state().set_index(RAFT_INIT_LOG_INDEX);
    apply_state.mut_truncated_state().set_term(RAFT_INIT_LOG_TERM);
     
    //idx
    // let idx = apply_state.get_applied_index();
    let idx = RAFT_INIT_LOG_INDEX;
    let term = if idx == apply_state.get_truncated_state().get_index() {
        apply_state.get_truncated_state().get_term()
    } else {
        match try!(snap.get_msg_cf::<Entry>(CF_RAFT, &keys::raft_log_key(region_id, idx))) {
            None => return Err(box_err!("entry {} of {} not found.", idx, region_id)),
            Some(entry) => entry.get_term(),
        }
    };

    let key = SnapKey::new(region_id, term, idx);

    mgr.wl().register(key.clone(), SnapEntry::Generating);
    defer!(mgr.wl().deregister(&key, &SnapEntry::Generating));

    let mut state: RegionLocalState = try!(snap.get_msg(&keys::region_state_key(key.region_id))
        .and_then(|res| {
            match res {
                None => Err(box_err!("could not find region info")),
                Some(state) => Ok(state),
            }
        }));

    if state.get_state() != PeerState::Normal {
        return Err(box_err!("snap job for {} seems stale, skip.", region_id));
    }

    let mut snapshot = Snapshot::new();

    // Set snapshot metadata.
    snapshot.mut_metadata().set_index(key.idx);
    snapshot.mut_metadata().set_term(key.term);

    let mut conf_state = ConfState::new();
    for p in state.get_region().get_peers() {
        conf_state.mut_nodes().push(p.get_id());
    }

    snapshot.mut_metadata().set_conf_state(conf_state);

    let mut snap_file = try!(mgr.rl().get_snap_file(&key, true));
    if snap_file.exists() {
        if let Err(e) = snap_file.validate() {
            error!("[region {}] file {} is invalid, will regenerate: {:?}",
                   region_id,
                   snap_file.path().display(),
                   e);
            try!(snap_file.try_delete());
            try!(snap_file.init());
            try!(build_snap_file(&mut snap_file, snap, state.get_region()));
        }
    } else {
        try!(build_snap_file(&mut snap_file, snap, state.get_region()));
    }

    // Set snapshot data.
    let mut snap_data = RaftSnapshotData::new();
    snap_data.set_region(state.take_region());

    let len = try!(snap_file.meta()).len();
    snap_data.set_file_size(len);

    let mut v = vec![];
    box_try!(snap_data.write_to_vec(&mut v));
    snapshot.set_data(v);

    Ok(snapshot)
}
