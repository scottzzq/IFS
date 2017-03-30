use std::sync::Arc;
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::vec::Vec;
use std::default::Default;
use std::time::{Instant, Duration};
use time::{Timespec, Duration as TimeDuration};
use raft::{self, RawNode, StateRole, Ready, ProgressState, Progress, INVALID_ID};
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, ChangePeerRequest, CmdType,
                          AdminCmdType, Request, Response, AdminRequest, AdminResponse,
                          TransferLeaderRequest, TransferLeaderResponse};

use rocksdb::{DB, WriteBatch, Writable, CFHandle};
use kvproto::metapb::{self, Region};
use raftstore::store::peer_storage::{PeerStorage, write_peer_state, ApplySnapResult};
use super::store::Store;
use raftstore::{Result, Error};
use uuid::Uuid;
use kvproto::pdpb::PeerStats;

use kvproto::eraftpb::{self, ConfChangeType, MessageType};
use kvproto::raft_serverpb::{RaftMessage, RaftApplyState, RaftTruncatedState, PeerState,
                             RegionLocalState};
use util::{escape, SlowTimer, rocksdb,clocktime};
use super::transport::Transport;
use storage::CF_RAFT;
use super::keys;
use protobuf::{self, Message};
use super::engine::{Peekable, Mutable, Snapshot};
use super::msg::Callback;
use super::util;
use super::cmd_resp;

use pd::PdClient;

use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::os::unix;
use std::path::Path;
use std::mem::transmute;
use std::io::SeekFrom;
use byteorder::{ByteOrder, BigEndian, ReadBytesExt, LittleEndian};


const TRANSFER_LEADER_ALLOW_LOG_LAG: u64 = 10;

#[derive(Debug)]
pub enum StaleState {
    Valid,
    ToValidate,
}


#[derive(Debug)]
pub enum ExecResult {
    ChangePeer {
        change_type: ConfChangeType,
        peer: metapb::Peer,
        region: metapb::Region,
    },
   // CompactLog { state: RaftTruncatedState },
    // SplitRegion {
    //     left: metapb::Region,
    //     right: metapb::Region,
    // },
}

// When we apply commands in handing ready, we should also need a way to
// let outer store do something after handing ready over.
// We can save these intermediate results in ready result.
// We only need to care administration commands now.
pub struct ReadyResult {
    // We can execute multi commands like 1, conf change, 2 split region, ...
    // in one ready, and outer store should handle these results sequentially too.
    pub exec_results: Vec<ExecResult>,
    // apply_snap_result is set after snapshot applied.
    pub apply_snap_result: Option<ApplySnapResult>,
}

struct ExecContext<'a> {
    pub snap: Snapshot,
    pub apply_state: RaftApplyState,
    pub wb: WriteBatch,
    pub req: &'a RaftCmdRequest,
}

impl<'a> ExecContext<'a> {
    fn save(&self, region_id: u64) -> Result<()> {
        let raft_cf = try!(self.snap.cf_handle(CF_RAFT));
        try!(self.wb.put_msg_cf(raft_cf,
                                &keys::apply_state_key(region_id),
                                &self.apply_state));
        Ok(())
    }
}

pub struct PendingCmd {
    pub uuid: Uuid,
    pub term: u64,
    pub cb: Callback,
}


pub struct CacheItem {
    pub offset: u64,
    pub size: u64,
}


#[derive(Default)]
struct PendingCmdQueue {
    normals: VecDeque<PendingCmd>,
    conf_change: Option<PendingCmd>,
    uuids: HashSet<Uuid>,
}

impl PendingCmdQueue {
    pub fn contains(&self, uuid: &Uuid) -> bool {
        self.uuids.contains(uuid)
    }

    fn remove(&mut self, cmd: &Option<PendingCmd>) {
        if let Some(ref cmd) = *cmd {
            self.uuids.remove(&cmd.uuid);
        }
    }

    fn pop_normal(&mut self, term: u64) -> Option<PendingCmd> {
        self.normals.pop_front().and_then(|cmd| {
            if cmd.term > term {
                self.normals.push_front(cmd);
                return None;
            }
            let res = Some(cmd);
            self.remove(&res);
            res
        })
    }

    fn append_normal(&mut self, cmd: PendingCmd) {
        self.uuids.insert(cmd.uuid);
        self.normals.push_back(cmd);
    }

    fn take_conf_change(&mut self) -> Option<PendingCmd> {
        // conf change will not be affected when changing between follower and leader,
        // so there is no need to check term.
        let cmd = self.conf_change.take();
        self.remove(&cmd);
        cmd
    }

    fn set_conf_change(&mut self, cmd: PendingCmd) {
        self.uuids.insert(cmd.uuid);
        self.conf_change = Some(cmd);
    }
}

pub struct Peer {
    engine: Arc<DB>,
    peer_cache: Rc<RefCell<HashMap<u64, metapb::Peer>>>,
    pub peer: metapb::Peer,
    pub region_id: u64,
    pub raft_group: RawNode<PeerStorage>,
    //proposals: ProposalQueue,
    pending_cmds: PendingCmdQueue,
    // // Record the last instant of each peer's heartbeat response.
    pub peer_heartbeats: HashMap<u64, Instant>,
    // coprocessor_host: CoprocessorHost,
    // /// an inaccurate difference in region size since last reset.
    pub size_diff_hint: u64,
    // /// delete keys' count since last reset.
    // pub delete_keys_hint: u64,

    // pub consistency_state: ConsistencyState,

    pub tag: String,

    // pub last_compacted_idx: u64,
    // // Approximate size of logs that is applied but not compacted yet.
    // pub raft_log_size_hint: u64,
    // // When entry exceed max size, reject to propose the entry.
    pub raft_entry_max_size: u64,

    // // if we remove ourself in ChangePeer remove, we should set this flag, then
    // // any following committed logs in same Ready should be applied failed.
    pending_remove: bool,

    leader_missing_time: Option<Instant>,

    leader_lease_expired_time: Option<Timespec>,

    election_timeout: TimeDuration,

    // pub volume_file: File,
    // pub volume_idx_file: File,
    // pub volume_read_file: File,
    // pub needle_cache: HashMap<u64, CacheItem>,
    // pub volume_file_offset: u64,

    // pub written_bytes: u64,
    // pub written_keys: u64,
}

pub fn find_peer(region: &metapb::Region, store_id: u64) -> Option<&metapb::Peer> {
    for peer in region.get_peers() {
        if peer.get_store_id() == store_id {
            return Some(peer);
        }
    }
    None
}

// a helper function to create peer easily.
pub fn new_peer(store_id: u64, peer_id: u64) -> metapb::Peer {
    let mut peer = metapb::Peer::new();
    peer.set_store_id(store_id);
    peer.set_id(peer_id);
    peer
}
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
impl Peer {
    // If we create the peer actively, like bootstrap/split/merge region, we should
    // use this function to create the peer. The region must contain the peer info
    // for this store.
    pub fn create<T: Transport, C: PdClient>(store: &mut Store<T, C>, region: &metapb::Region) -> Result<Peer> {
        let store_id = store.store_id();
        let peer_id = match find_peer(region, store_id) {
            None => {
                return Err(box_err!("find no peer for store {} in region {:?}", store_id, region))
            }
            Some(peer) => peer.get_id(),
        };
        info!("[region {}] create peer with id {}",
              region.get_id(),
              peer_id);
        Peer::new(store, region, peer_id)
    }

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

    // The peer can be created from another node with raft membership changes, and we only
    // know the region_id and peer_id when creating this replicated peer, the region info
    // will be retrieved later after applying snapshot.
    // 有peer_id和region_id就可以创建出Peer来
    pub fn replicate<T: Transport, C: PdClient>(store: &mut Store<T, C>,
                                                region_id: u64,
                                                peer_id: u64)
                                                -> Result<Peer> {
        // We will remove tombstone key when apply snapshot
        info!("[region {}] replicate peer with id {}", region_id, peer_id);

        let mut region = metapb::Region::new();
        region.set_id(region_id);
        Peer::new(store, &region, peer_id)
    }

    fn new<T: Transport, C: PdClient>(store: &mut Store<T, C>, region: &metapb::Region, peer_id: u64) -> Result<Peer> {
        if peer_id == raft::INVALID_ID {
            return Err(box_err!("invalid peer id"));
        }
        let cfg = store.config();
        let store_id = store.store_id();
        let sched = store.snap_scheduler();
        let tag = format!("[region {}] {}", region.get_id(), peer_id);
        let ps = try!(PeerStorage::new(store.engine(), &region, sched, tag.clone(), region.get_id(), cfg));
        let applied_index = ps.applied_index();

        let raft_cfg = raft::Config {
            id: peer_id,
            peers: vec![],
            election_tick: cfg.raft_election_timeout_ticks,
            heartbeat_tick: cfg.raft_heartbeat_ticks,
            max_size_per_msg: cfg.raft_max_size_per_msg,
            max_inflight_msgs: cfg.raft_max_inflight_msgs,
            applied: applied_index,
            check_quorum: true,
            tag: tag.clone(),
            ..Default::default()
        };

        let raft_group = try!(RawNode::new(&raft_cfg, ps, &[]));

        //peer.load_all_coprocessors();
        // let mut volume_file: File;
        // let mut volume_idx_file: File;
        // let mut volume_read_file: File;
        // let mut volume_idx_read_file: File;

        // let attr = fs::metadata(&cfg.volume_index_root_path);
        // let volume_file_offset:u64;
        // let mut needle_cache = HashMap::new();

        // match attr {
        //     Ok(v) => {
        //         if !v.is_dir() {
        //             error!("volume_index_root_path:[{}] is not valid!", cfg.volume_index_root_path);
        //             return Err(box_err!("volume_index_root_path is not dir"));
        //         } else{
        //             let mut v_path: String = cfg.volume_index_root_path.clone();
        //             v_path.push_str("/");
        //             v_path.push_str(&region.get_id().to_string());
        //             let volume_path = Path::new(&v_path);
        //             match OpenOptions::new().create(true).write(true).open(volume_path) {
        //                 Ok(f) => {
        //                     volume_read_file = try!(File::open(volume_path));
        //                     let metadata = try!(fs::metadata(volume_path));
        //                     volume_file_offset = metadata.len();
        //                     volume_file = f;
        //                     volume_file.seek(SeekFrom::End(0));

        //                     let mut i_path: String = cfg.volume_index_root_path.clone();
        //                     i_path.push_str("/");
        //                     i_path.push_str(&region.get_id().to_string());
        //                     i_path.push_str(".idx");
        //                     let volume_idx_path = Path::new(&i_path);
        //                     match OpenOptions::new().create(true).write(true).open(volume_idx_path) {
        //                         Ok(f) => {
        //                             volume_idx_file = f;
        //                             volume_idx_file.seek(SeekFrom::End(0));

        //                             volume_idx_read_file = try!(File::open(volume_idx_path));
        //                             while true {
        //                                 let mut header = vec![0;24];
        //                                 volume_idx_read_file.read_exact(&mut header);
        //                                 println!("header:[{:?}]", header);
        //                                 let mut reader = header.as_slice();
        //                                 let key = try!(reader.read_u64::<BigEndian>());
        //                                 let offset = try!(reader.read_u64::<BigEndian>());
        //                                 let size = try!(reader.read_u64::<BigEndian>());
        //                                 debug!("Init volume idx, key:[{}] offset:[{}] size:[{}]", key, offset, size);
        //                                 needle_cache.insert(key, CacheItem{offset:offset, size : size});
        //                                 if size == 0{
        //                                     break;
        //                                 }
        //                                 //debug!("Init volume idx, key:[{}] offset:[{}] size:[{}]", key, offset, size);
        //                             }
        //                         },
        //                         Err(e) => {
        //                             error!("create volume_idx_file failed");
        //                             return Err(box_err!("create volume_idx_file failed"));
        //                         },
        //                     }
        //                 },
        //                 Err(e) => {
        //                     error!("create volume_file failed");
        //                     return Err(box_err!("create volume_file failed"));
        //                 },
        //             }
        //         }
        //     }
        //     Err(e) => {
        //         error!("volume_index_root_path:[{}] is not exist!", cfg.volume_index_root_path);
        //         return Err(box_err!("volume_index_root_path is not exist!"));
        //     }
        // }

        let mut peer = Peer {
            engine: store.engine(),
            peer: new_peer(store_id, peer_id),
            region_id: region.get_id(),
            raft_group: raft_group,
            //proposals: Default::default(),
            pending_cmds: Default::default(),
            peer_cache: store.peer_cache(),
            peer_heartbeats: HashMap::new(),
            // coprocessor_host: CoprocessorHost::new(),
            size_diff_hint: 0,
            // delete_keys_hint: 0,
            pending_remove: false,
            leader_missing_time: Some(Instant::now()),
            tag: tag,
            // last_compacted_idx: 0,
            // consistency_state: ConsistencyState {
            //     last_check_time: Instant::now(),
            //     index: INVALID_INDEX,
            //     hash: vec![],
            // },
            // raft_log_size_hint: 0,
            raft_entry_max_size: cfg.raft_entry_max_size,
            leader_lease_expired_time: None,
            election_timeout: TimeDuration::milliseconds(cfg.raft_base_tick_interval as i64) *
                              cfg.raft_election_timeout_ticks as i32,
            // volume_file: volume_file,
            // volume_idx_file: volume_idx_file,
            // volume_read_file: volume_read_file,
            // needle_cache: needle_cache, 
            // volume_file_offset:volume_file_offset,
            // written_bytes: 0,
            // written_keys: 0,
        };

        // If this region has only one peer and I am the one, campaign directly.
        if region.get_peers().len() == 1 && region.get_peers()[0].get_store_id() == store_id {
            try!(peer.raft_group.campaign());
        }

        Ok(peer)
    }

    #[inline]
    fn send<T>(&mut self,
               trans: &T,
               msgs: &[eraftpb::Message])
               -> Result<()>
        where T: Transport
    {
        for msg in msgs {
            try!(self.send_raft_message(msg, trans));
        }
        Ok(())
    }
    pub fn check_peers(&mut self) {
        if !self.is_leader() {
            self.peer_heartbeats.clear();
            return;
        }

        if self.peer_heartbeats.len() == self.region().get_peers().len() {
            return;
        }

        // Insert heartbeats in case that some peers never response heartbeats.
        for peer in self.region().get_peers().to_owned() {
            //防止某些Peer永久不回复心跳包，则可能永远也删除不了
            self.peer_heartbeats.entry(peer.get_id()).or_insert_with(Instant::now);
        }
    }

    pub fn collect_down_peers(&self, max_duration: Duration) -> Vec<PeerStats> {
        let mut down_peers = Vec::new();
        for p in self.region().get_peers() {
            if p.get_id() == self.peer.get_id() {
                continue;
            }
            if let Some(instant) = self.peer_heartbeats.get(&p.get_id()) {
                if instant.elapsed() >= max_duration {
                    let mut stats = PeerStats::new();
                    stats.set_peer(p.clone());
                    stats.set_down_seconds(instant.elapsed().as_secs());
                    down_peers.push(stats);
                }
            }
        }
        down_peers
    }

    pub fn step(&mut self, m: eraftpb::Message) -> Result<()> {
        info!("peer.rs step Message:[{:?}]", m);
        if self.is_leader() && m.get_from() != INVALID_ID {
            self.peer_heartbeats.insert(m.get_from(), Instant::now());
        }
        try!(self.raft_group.step(m));
        Ok(())
    }


    pub fn get_peer_from_cache(&self, peer_id: u64) -> Option<metapb::Peer> {
        if let Some(peer) = self.peer_cache.borrow().get(&peer_id).cloned() {
            return Some(peer);
        }

        // Try to find in region, if found, set in cache.
        for peer in self.get_store().get_region().get_peers() {
            if peer.get_id() == peer_id {
                self.peer_cache.borrow_mut().insert(peer_id, peer.clone());
                return Some(peer.clone());
            }
        }

        None
    }

    #[inline]
    fn next_proposal_index(&self) -> u64 {
        self.raft_group.raft.raft_log.last_index() + 1
    }

    pub fn check_stale_state(&mut self, d: Duration) -> StaleState {
        // Updates the `leader_missing_time` according to the current state.
        if self.leader_id() == raft::INVALID_ID {
            if self.leader_missing_time.is_none() {
                self.leader_missing_time = Some(Instant::now())
            }
        } else if self.is_initialized() {
            // A peer is considered as in the leader missing state if it's uninitialized or
            // if it's initialized but is isolated from its leader.
            // For an uninitialized peer, even if its leader sends heartbeats to it,
            // it cannot successfully receive the snapshot from the leader and apply the snapshot.
            // The raft state machine cannot work in an uninitialized peer to detect
            // if the leader is working.
            self.leader_missing_time = None
        }

        // Checks whether the current peer is stale.
        let duration = match self.leader_missing_time {
            Some(t) => t.elapsed(),
            None => Duration::new(0, 0),
        };
        if duration >= d {
            // Resets the `leader_missing_time` to avoid sending the same tasks to
            // PD worker continuously during the leader missing timeout.
            self.leader_missing_time = None;
            return StaleState::ToValidate;
        }
        StaleState::Valid
    }


    fn send_raft_message<T: Transport>(&mut self, msg: &eraftpb::Message, trans: &T) -> Result<()> {
        let mut send_msg = RaftMessage::new();
        send_msg.set_region_id(self.region_id);
        // TODO: can we use move instead?
        send_msg.set_message(msg.clone());
        // set current epoch
        send_msg.set_region_epoch(self.region().get_region_epoch().clone());
        let mut unreachable = false;

        let from_peer = match self.get_peer_from_cache(msg.get_from()) {
            Some(p) => p,
            None => {
                return Err(box_err!("failed to lookup sender peer {} in region {}",
                                    msg.get_from(),
                                    self.region_id))
            }
        };

        let to_peer = match self.get_peer_from_cache(msg.get_to()) {
            Some(p) => p,
            None => {
                return Err(box_err!("failed to look up recipient peer {} in region {}",
                                    msg.get_to(),
                                    self.region_id))
            }
        };

        let to_peer_id = to_peer.get_id();
        let to_store_id = to_peer.get_store_id();
        let msg_type = msg.get_msg_type();
        debug!("{} send raft msg {:?}[size: {}] from {} to {}",
               self.tag,
               msg_type,
               msg.compute_size(),
               from_peer.get_id(),
               to_peer_id);

        send_msg.set_from_peer(from_peer);
        send_msg.set_to_peer(to_peer);

        if let Err(e) = trans.send(send_msg) {
            warn!("{} failed to send msg to {} in store {}, err: {:?}",
                  self.tag,
                  to_peer_id,
                  to_store_id,
                  e);

            unreachable = true;
        }

        if unreachable {
            self.raft_group.report_unreachable(to_peer_id);
            // if msg_type == eraftpb::MessageType::MsgSnapshot {
            //     self.raft_group.report_snapshot(to_peer_id, SnapshotStatus::Failure);
            // }
        }

        Ok(())
    }

     fn next_lease_expired_time(&self, send_to_quorum_ts: Timespec) -> Timespec {
        // The valid leader lease should be
        // "lease = election_timeout - (quorum_commit_ts - send_to_quorum_ts)"
        // And the expired timestamp for that leader lease is "quorum_commit_ts + lease",
        // which is "send_to_quorum_ts + election_timeout" in short.
        send_to_quorum_ts + self.election_timeout
    }


    fn update_leader_lease(&mut self, ready: &Ready) {
        // Update leader lease when the Raft state changes.
        if ready.ss.is_some() {
            let ss = ready.ss.as_ref().unwrap();
            match ss.raft_state {
                StateRole::Leader => {
                    // The local read can only be performed after a new leader has applied
                    // the first empty entry on its term. After that the lease expiring time
                    // should be updated to
                    //   send_to_quorum_ts + election_timeout
                    // as the comments in `next_lease_expired_time` function explain.
                    // It is recommended to update the lease expiring time right after
                    // this peer becomes leader because it's more convenient to do it here and
                    // it has no impact on the correctness.
                    self.leader_lease_expired_time =
                        Some(self.next_lease_expired_time(clocktime::raw_now()));
                    debug!("{} becomes leader and lease expired time is {:?}",
                           self.tag,
                           self.leader_lease_expired_time);
                }
                StateRole::Follower => {
                    self.leader_lease_expired_time = None;
                }
                _ => {}
            }
        }
    }


    pub fn handle_raft_ready<T: Transport>(&mut self,
                                           trans: &T)
                                           -> Result<Option<ReadyResult>> {
        if !self.raft_group.has_ready() {
            return Ok(None);
        }
        debug!("{} handle raft ready", self.tag);
        let mut ready = self.raft_group.ready();
        
        let is_applying = self.get_store().is_applying_snap();
        if is_applying {
            if !raft::is_empty_snap(&ready.snapshot) {
                if self.get_store().is_canceling_snap() {
                    return Ok(None);
                }
                warn!("{} receiving a new snap {:?} when applying the old one, try to abort.",
                      self.tag,
                      ready.snapshot);
                if !self.mut_store().cancel_applying_snap() {
                    return Ok(None);
                }
            }
            // skip apply
            ready.committed_entries = vec![];
        }

        let t = SlowTimer::new();
        // The leader can write to disk and replicate to the followers concurrently
        // For more details, check raft thesis 10.2.1
        //leader可以先发送
        if self.is_leader() {
            try!(self.send(trans, &ready.messages));
        }
        //调用Storage处理
        let apply_result = try!(self.mut_store().handle_raft_ready(&ready));
        
        //不是leader
        if !self.is_leader() {
            try!(self.send(trans, &ready.messages));
        }
        let exec_results = try!(self.handle_raft_commit_entries(&ready.committed_entries));
        slow_log!(t,
                  "{} handle ready, entries {}, committed entries {}, messages \
                   {}, hard state changed {}",
                  self.tag,
                  ready.entries.len(),
                  ready.committed_entries.len(),
                  ready.messages.len(),
                  ready.hs.is_some());

        if is_applying {
            // remove hard state so raft won't change the apply index.
            ready.hs.take();
        }

        self.raft_group.advance(ready);
        Ok(Some(ReadyResult {
            apply_snap_result: apply_result,
            exec_results: exec_results,
        }))
    }

    fn handle_raft_commit_entries(&mut self,
                                  committed_entries: &[eraftpb::Entry])
                                  -> Result<Vec<ExecResult>> {
        // If we send multiple ConfChange commands, only first one will be proposed correctly,
        // others will be saved as a normal entry with no data, so we must re-propose these
        // commands again.
        let t = SlowTimer::new();
        let mut results = vec![];
        let committed_count = committed_entries.len();
        for entry in committed_entries {
            let res = try!(match entry.get_entry_type() {
                eraftpb::EntryType::EntryNormal => self.handle_raft_entry_normal(entry),
                eraftpb::EntryType::EntryConfChange => self.handle_raft_entry_conf_change(entry),
            });

            if let Some(res) = res {
                results.push(res);
            }
        }
        slow_log!(t,
                  "{} handle {} committed entries",
                  self.tag,
                  committed_count);
        Ok(results)
    }

    fn handle_raft_entry_normal(&mut self, entry: &eraftpb::Entry) -> Result<Option<ExecResult>> {
        let index = entry.get_index();
        let term = entry.get_term();
        let data = entry.get_data();

        if data.is_empty() {
            // when a peer become leader, it will send an empty entry.
            let wb = WriteBatch::new();
            let mut state = self.get_store().apply_state.clone();
            state.set_applied_index(index);
            let engine = self.engine.clone();
            let raft_cf = try!(rocksdb::get_cf_handle(engine.as_ref(), CF_RAFT));
            try!(wb.put_msg_cf(raft_cf, &keys::apply_state_key(self.region_id), &state));
            try!(self.engine.write(wb));
            self.mut_store().apply_state = state;
            self.mut_store().applied_index_term = term;
            return Ok(None);
        }

        let cmd = try!(protobuf::parse_from_bytes::<RaftCmdRequest>(data));
        // no need to return error here.
        self.process_raft_cmd(index, term, cmd).or_else(|e| {
            error!("{} process raft command at index {} err: {:?}",
                   self.tag,
                   index,
                   e);
            Ok(None)
        })
    }

    fn find_cb(&mut self, uuid: Uuid, term: u64, cmd: &RaftCmdRequest) -> Option<Callback> {
        if get_change_peer_cmd(cmd).is_some() {
            if let Some(cmd) = self.pending_cmds.take_conf_change() {
                if cmd.uuid == uuid {
                    return Some(cmd.cb);
                } else {
                    self.notify_not_leader(cmd);
                }
            }
            return None;
        }
        while let Some(head) = self.pending_cmds.pop_normal(term) {
            if head.uuid == uuid {
                return Some(head.cb);
            }
            // because of the lack of original RaftCmdRequest, we skip calling
            // coprocessor here.
            // TODO: call coprocessor with uuid instead.
            self.notify_not_leader(head);
        }
        None
    }

    fn process_raft_cmd(&mut self,
                        index: u64,
                        term: u64,
                        cmd: RaftCmdRequest)
                        -> Result<Option<ExecResult>> {
        if index == 0 {
            return Err(box_err!("processing raft command needs a none zero index"));
        }

        let uuid = util::get_uuid_from_req(&cmd).unwrap();
        let cb = self.find_cb(uuid, term, &cmd);
        let (mut resp, exec_result) = self.apply_raft_cmd(index, term, &cmd).unwrap_or_else(|e| {
            error!("{} apply raft command err {:?}", self.tag, e);
            let mut resp = RaftCmdResponse::new();
            resp.mut_header().set_error(e.into());
            (resp, None)
        });

        debug!("{} applied command with uuid {:?} at log index {}",
               self.tag,
               uuid,
               index);

        if cb.is_none() {
            return Ok(exec_result);
        } 
        let cb = cb.unwrap();
        // TODO: if we have exec_result, maybe we should return this callback too. Outer
        // store will call it after handing exec result.
        // Bind uuid here.
        resp.mut_header().set_uuid(uuid.as_bytes().to_vec());
        resp.mut_header().set_current_term(self.term());
        
        if let Err(e) = cb.call_box((resp,)) {
            error!("{} callback err {:?}", self.tag, e);
        }

        Ok(exec_result)
    }

    fn handle_raft_entry_conf_change(&mut self,
                                     entry: &eraftpb::Entry)
                                     -> Result<Option<ExecResult>> {
        let index = entry.get_index();
        let term = entry.get_term();
        let mut conf_change =
            try!(protobuf::parse_from_bytes::<eraftpb::ConfChange>(entry.get_data()));
        let cmd = try!(protobuf::parse_from_bytes::<RaftCmdRequest>(conf_change.get_context()));
        
        let res = match self.process_raft_cmd(index, term, cmd) {
            a @ Ok(Some(_)) => a,
            e => {
                error!("{} process raft command at index {} err: {:?}",
                       self.tag,
                       index,
                       e);
                // If failed, tell raft that the config change was aborted.
                conf_change = eraftpb::ConfChange::new();
                Ok(None)
            }
        };
        self.raft_group.apply_conf_change(conf_change);
        res
    }

    //  fn check_data_key(&self, key: &[u8]) -> Result<()> {
    //     // region key range has no data prefix, so we must use origin key to check.
    //     try!(Peer::check_key_in_region(key, self.get_store().get_region()));

    //     Ok(())
    // }
     /// Check if key in region range [`start_key`, `end_key`).
    pub fn check_key_in_region(key: &[u8], region: &metapb::Region) -> Result<()> {
        let end_key = region.get_end_key();
        let start_key = region.get_start_key();
        if key >= start_key && (end_key.is_empty() || key < end_key) {
            Ok(())
        } else {
            Err(Error::KeyNotInRegion(key.to_vec(), region.clone()))
        }
    }

    pub fn check_epoch(&self, req: &RaftCmdRequest) -> Result<()> {
        let (mut check_ver, mut check_conf_ver) = (false, false);
        if req.has_admin_request() {
            match req.get_admin_request().get_cmd_type() {
                AdminCmdType::CompactLog |
                AdminCmdType::InvalidAdmin |
                AdminCmdType::ComputeHash |
                AdminCmdType::VerifyHash => {}
                AdminCmdType::Split => check_ver = true,
                AdminCmdType::ChangePeer => check_conf_ver = true,
                AdminCmdType::TransferLeader => {
                    check_ver = true;
                    check_conf_ver = true;
                }
            };
        } else {
            // for get/set/delete, we don't care conf_version.
            check_ver = true;
        }

        if !check_ver && !check_conf_ver {
            return Ok(());
        }

        if !req.get_header().has_region_epoch() {
            return Err(box_err!("missing epoch!"));
        }

        let from_epoch = req.get_header().get_region_epoch();
        let latest_region = self.region();
        let latest_epoch = latest_region.get_region_epoch();

        // should we use not equal here?
        if (check_conf_ver && from_epoch.get_conf_ver() < latest_epoch.get_conf_ver()) ||
           (check_ver && from_epoch.get_version() < latest_epoch.get_version()) {
            debug!("{} received stale epoch {:?}, mime: {:?}",
                   self.tag,
                   from_epoch,
                   latest_epoch);
            return Err(Error::StaleEpoch(format!("latest_epoch of region {} is {:?}, but you \
                                                  sent {:?}",
                                                 self.region_id,
                                                 latest_epoch,
                                                 from_epoch),
                                         vec![self.region().to_owned()]));
        }

        Ok(())
    }

    fn should_read_local(&mut self, req: &RaftCmdRequest) -> bool {
        if (req.has_header() && req.get_header().get_read_quorum()) ||
           !self.raft_group.raft.in_lease() || req.get_requests().len() == 0 {
            return false;
        }

        // If applied index's term is differ from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if self.get_store().applied_index_term != self.raft_group.raft.term {
            return false;
        }

        for cmd_req in req.get_requests() {
            if cmd_req.get_cmd_type() != CmdType::Snap && cmd_req.get_cmd_type() != CmdType::Get {
                return false;
            }
        }

        // If the leader lease has expired, local read should not be performed.
        if self.leader_lease_expired_time.is_none() {
            return false;
        }

        let now = clocktime::raw_now();
        let expired_time = self.leader_lease_expired_time.unwrap();
        if now > expired_time {
            debug!("{} leader lease expired time {:?} is outdated",
                   self.tag,
                   self.leader_lease_expired_time);
            // Reset leader lease expiring time.
            self.leader_lease_expired_time = None;
            // Perform a consistent read to Raft quorum and try to renew the leader lease.
            return false;
        }

        true
    }

    fn is_local_read(&self, req: &RaftCmdRequest) -> bool {
        if (req.has_header() && req.get_header().get_read_quorum()) ||
           !self.raft_group.raft.in_lease() || req.get_requests().len() == 0 {
            return false;
        }

        // If applied index's term is differ from current raft's term, leader transfer
        // must happened, if read locally, we may read old value.
        if self.get_store().applied_index_term != self.raft_group.raft.term {
            return false;
        }

        for cmd_req in req.get_requests() {
            if cmd_req.get_cmd_type() != CmdType::Snap && cmd_req.get_cmd_type() != CmdType::Get {
                return false;
            }
        }

        true
    }

    /// Propose a request.
    ///
    /// Return true means the request has been proposed successfully.
     pub fn propose(&mut self,
                   cmd: PendingCmd,
                   req: RaftCmdRequest,
                   mut err_resp: RaftCmdResponse)
                   -> Result<()> {
        if self.pending_cmds.contains(&cmd.uuid) {
            let e: Error = box_err!("duplicated uuid {:?}", cmd.uuid);
            err_resp.mut_header().set_error(e.into());
            return cmd.cb.call_box((err_resp,));
        }

        debug!("{} propose command with uuid {:?}", self.tag, cmd.uuid);
        
        let local_read = self.is_local_read(&req);
        if local_read {
            // for read-only, if we don't care stale read, we can
            // execute these commands immediately in leader.
            let engine = self.engine.clone();
            let mut ctx = ExecContext {
                snap: Snapshot::new(engine),
                apply_state: self.get_store().apply_state.clone(),
                wb: WriteBatch::new(),
                req: &req,
            };
            let (mut resp, _) = self.exec_raft_cmd(&mut ctx).unwrap_or_else(|e| {
                error!("{} execute raft command err: {:?}", self.tag, e);
                let mut resp = RaftCmdResponse::new();
                resp.mut_header().set_error(e.into());
                
                (resp, None)
            });

            resp.mut_header().set_uuid(cmd.uuid.as_bytes().to_vec());
            if self.term() > 0{
                resp.mut_header().set_current_term(self.term());
            }
            return cmd.cb.call_box((resp,));
        } 
        else if get_transfer_leader_cmd(&req).is_some() {
            let transfer_leader = get_transfer_leader_cmd(&req).unwrap();
            let peer = transfer_leader.get_peer();

            if self.is_tranfer_leader_allowed(peer) {
                self.transfer_leader(peer);
            } else {
                info!("{} transfer leader message {:?} ignored directly",
                      self.tag,
                      req);
            }

            // transfer leader command doesn't need to replicate log and apply, so we
            // return immediately. Note that this command may fail, we can view it just as an advice
            return cmd.cb.call_box((make_transfer_leader_response(),));
        } else if get_change_peer_cmd(&req).is_some() {
            if self.raft_group.raft.pending_conf {
                return Err(box_err!("there is a pending conf change, try later"));
            }
            if let Some(cmd) = self.pending_cmds.take_conf_change() {
                // if it loses leadership before conf change is replicated, there may be
                // a stale pending conf change before next conf change is applied. If it
                // becomes leader again with the stale pending conf change, will enter
                // this block, so we notify leadership may have changed.
                self.notify_not_leader(cmd);
            }

            if let Err(e) = self.propose_conf_change(req) {
                err_resp.mut_header().set_error(e.into());
                return cmd.cb.call_box((err_resp,));
            }

            self.pending_cmds.set_conf_change(cmd);
        } 
        else if let Err(e) = self.propose_normal(req) {
            err_resp.mut_header().set_error(e.into());
            return cmd.cb.call_box((err_resp,));
        } else {
            self.pending_cmds.append_normal(cmd);
        }

        Ok(())
    }
    

    fn propose_normal(&mut self,
                      mut cmd: RaftCmdRequest)
                      -> Result<()> {
  
        // TODO: validate request for unexpected changes.
        //try!(self.coprocessor_host.pre_propose(&self.raft_group.get_store(), &mut cmd));
        let data = try!(cmd.write_to_bytes());

        if data.len() as u64 > self.raft_entry_max_size {
            error!("entry is too large, entry size {}", data.len());
            return Err(Error::RaftEntryTooLarge(self.region_id, data.len() as u64));
        }

        let propose_index = self.next_proposal_index();
        try!(self.raft_group.propose(data));
        if self.next_proposal_index() == propose_index {
            // The message is dropped silently, this usually due to leader absence
            // or transferring leader. Both cases can be considered as NotLeader error.
            return Err(Error::NotLeader(self.region_id, None));
        }
        Ok(())
    }

    fn propose_conf_change(&mut self, cmd: RaftCmdRequest) -> Result<()> {
        let data = try!(cmd.write_to_bytes());
        let change_peer = get_change_peer_cmd(&cmd).unwrap();

        let mut cc = eraftpb::ConfChange::new();
        cc.set_change_type(change_peer.get_change_type());
        cc.set_node_id(change_peer.get_peer().get_id());
        cc.set_context(data);

        info!("{} propose conf change {:?} peer {:?}",
              self.tag,
              cc.get_change_type(),
              cc.get_node_id());

        self.raft_group.propose_conf_change(cc).map_err(From::from)
    }

    pub fn region(&self) -> &metapb::Region {
        self.get_store().get_region()
    }

    pub fn peer_id(&self) -> u64 {
        self.peer.get_id()
    }

    pub fn get_raft_status(&self) -> raft::Status {
        self.raft_group.status()
    }

    pub fn leader_id(&self) -> u64 {
        self.raft_group.raft.leader_id
    }

    pub fn is_leader(&self) -> bool {
        self.raft_group.raft.state == StateRole::Leader
    }

    #[inline]
    pub fn get_store(&self) -> &PeerStorage {
        self.raft_group.get_store()
    }

    #[inline]
    pub fn mut_store(&mut self) -> &mut PeerStorage {
        self.raft_group.mut_store()
    }

    pub fn term(&self) -> u64 {
        self.raft_group.raft.term
    }

    pub fn is_initialized(&self) -> bool {
        self.get_store().is_initialized()
    }

    pub fn destroy(&mut self) -> Result<()> {
        let t = Instant::now();

        // TODO: figure out a way to unit test this.
        let peer_id = self.peer_id();
        for cmd in self.pending_cmds.normals.drain(..) {
            notify_region_removed(self.region_id, peer_id, cmd);
        }
        if let Some(cmd) = self.pending_cmds.conf_change.take() {
            notify_region_removed(self.region_id, peer_id, cmd);
        }

        let region = self.get_store().get_region().clone();
        info!("{} begin to destroy", self.tag);

        // First set Tombstone state explicitly, and clear raft meta.
        // If we meet panic when deleting data and raft log, the dirty data
        // will be cleared by Compaction Filter later or a newer snapshot applying.
        let wb = WriteBatch::new();
        try!(self.get_store().clear_meta(&wb));
        try!(write_peer_state(&wb, &region, PeerState::Tombstone));
        try!(self.engine.write(wb));
        if self.get_store().is_initialized() {
            try!(self.get_store().clear_data());
        }
        info!("{} destroy itself, takes {:?}", self.tag, t.elapsed());

        Ok(())
    }

    fn is_tranfer_leader_allowed(&self, peer: &metapb::Peer) -> bool {
        let peer_id = peer.get_id();
        let status = self.raft_group.status();

        if !status.progress.contains_key(&peer_id) {
            return false;
        }

        for progress in status.progress.values() {
            if progress.state == ProgressState::Snapshot {
                return false;
            }
        }

        let last_index = self.get_store().last_index();
        last_index <= status.progress[&peer_id].matched + TRANSFER_LEADER_ALLOW_LOG_LAG
    }
    fn transfer_leader(&mut self, peer: &metapb::Peer) {
        info!("{} transfer leader to {:?}", self.tag, peer);
        self.raft_group.transfer_leader(peer.get_id());
    }

    fn notify_not_leader(&self, cmd: PendingCmd) {
        let leader = self.get_peer_from_cache(self.leader_id());
        let not_leader = Error::NotLeader(self.region_id, leader);

        let mut resp = RaftCmdResponse::new();
        resp.mut_header().set_error(not_leader.into());
        resp.mut_header().set_current_term(self.term());
        resp.mut_header().set_uuid(cmd.uuid.as_bytes().to_vec());
        warn!("{} command {} is stale, skip", self.tag, cmd.uuid);
        if let Err(e) = cmd.cb.call_box((resp,)) {
            error!("{} failed to clean stale callback of {}: {:?}",
                   self.tag,
                   cmd.uuid,
                   e);
        }
    }

    fn apply_raft_cmd(&mut self,
                      index: u64,
                      term: u64,
                      req: &RaftCmdRequest)
                      -> Result<(RaftCmdResponse, Option<ExecResult>)> {
        if self.pending_remove {
            let region_not_found = Error::RegionNotFound(self.region_id);
            let mut resp = RaftCmdResponse::new();
            resp.mut_header().set_error(region_not_found.into());
            if let Some(uuid) = util::get_uuid_from_req(req) {
                resp.mut_header().set_uuid(uuid.as_bytes().to_vec());
            }
            return Ok((resp, None));
        }

        let last_applied_index = self.get_store().applied_index();
        if last_applied_index >= index {
            return Err(box_err!("applied index moved backwards, {} >= {}",
                                last_applied_index,
                                index));
        }

        let engine = self.engine.clone();

        // struct ExecContext<'a> {
        //     pub snap: Snapshot,
        //     pub apply_state: RaftApplyState,
        //     pub wb: WriteBatch,
        //     pub req: &'a RaftCmdRequest,
        // }
        let mut ctx = ExecContext {
            snap: Snapshot::new(engine),
            apply_state: self.get_store().apply_state.clone(),
            wb: WriteBatch::new(),
            req: req,
        };
        let (mut resp, exec_result) = self.exec_raft_cmd(&mut ctx).unwrap_or_else(|e| {
            error!("{} execute raft command err: {:?}", self.tag, e);
            let mut resp = RaftCmdResponse::new();
            resp.mut_header().set_error(e.into());
            (resp, None)
        });

        ctx.apply_state.set_applied_index(index);
        ctx.save(self.region_id).expect("save state must not fail");

        // Commit write and change storage fields atomically.
        let mut storage = self.mut_store();
        match storage.engine.write(ctx.wb) {
            Ok(_) => {
                //更新Storage的apply信息
                //存储raft apply的相关信息
                storage.apply_state = ctx.apply_state;
                //主要用来判断是否可以local read
                storage.applied_index_term = term;

                if let Some(ref exec_result) = exec_result {
                    match *exec_result {
                        ExecResult::ChangePeer { ref region, .. } => {
                            storage.region = region.clone();
                        }
                        // ExecResult::CompactLog { .. } => {}
                        // ExecResult::SplitRegion { ref left, .. } => {
                        //     storage.region = left.clone();
                        // }
                    }
                };
            }
            Err(e) => {
                error!("{} commit batch failed err {:?}", storage.tag, e);
                resp = cmd_resp::message_error(e);
            }
        };

        Ok((resp, exec_result))
    }
}

/// Call the callback of `cmd` that the region is removed.
fn notify_region_removed(region_id: u64, peer_id: u64, cmd: PendingCmd) {
    let region_not_found = Error::RegionNotFound(region_id);
    
    let mut resp = RaftCmdResponse::new();
    resp.mut_header().set_error(region_not_found.into());
    resp.mut_header().set_uuid(cmd.uuid.as_bytes().to_vec());

    debug!("[region {}] {} is removed, notify {}.",
           region_id,
           peer_id,
           cmd.uuid);
    if let Err(e) = cmd.cb.call_box((resp,)) {
        error!("failed to notify {}: {:?}", cmd.uuid, e);
    }
}


// Here we implement all commands.
impl Peer {
    // Only errors that will also occur on all other stores should be returned.
    fn exec_raft_cmd(&mut self,
                     ctx: &mut ExecContext)
                     -> Result<(RaftCmdResponse, Option<ExecResult>)> {
        try!(self.check_epoch(ctx.req));
        if ctx.req.has_admin_request() {
            self.exec_admin_cmd(ctx)
        } else {
        //     // Now we don't care write command outer, so use None.
             self.exec_write_cmd(ctx).and_then(|v| Ok((v, None)))
        }
    }

    fn exec_admin_cmd(&mut self,
                      ctx: &mut ExecContext)
                      -> Result<(RaftCmdResponse, Option<ExecResult>)> {
        let request = ctx.req.get_admin_request();
        let cmd_type = request.get_cmd_type();
        info!("{} execute admin command {:?} at ",
              self.tag,
              request);

        let (mut response, exec_result) = try!(match cmd_type {
            AdminCmdType::ChangePeer => self.exec_change_peer(ctx, request),
            AdminCmdType::TransferLeader => Err(box_err!("transfer leader won't exec")),
            AdminCmdType::ComputeHash => Err(box_err!("ComputeHash won't exec")), //self.exec_compute_hash(ctx, request),
            AdminCmdType::VerifyHash => Err(box_err!("VerifyHash won't exec")), //self.exec_verify_hash(ctx, request),
            AdminCmdType::InvalidAdmin => Err(box_err!("unsupported admin command type")),
            AdminCmdType::Split => Err(box_err!("split command type")),//self.exec_split(ctx, request),
            AdminCmdType::CompactLog => Err(box_err!("compactlog admin command type")),//self.exec_compact_log(ctx, request),
        });
        response.set_cmd_type(cmd_type);

        let mut resp = RaftCmdResponse::new();
        resp.set_admin_response(response);
        Ok((resp, exec_result))
    }

    fn exec_change_peer(&mut self,
                        ctx: &ExecContext,
                        request: &AdminRequest)
                        -> Result<(AdminResponse, Option<ExecResult>)> {
        //enum ConfChangeType {
        //    AddNode    = 0;
        //    RemoveNode = 1;
        //}

        // message Peer {      
        //     optional uint64 id          = 1 [(gogoproto.nullable) = false]; 
        //     optional uint64 store_id    = 2 [(gogoproto.nullable) = false];
        // }

        // message ChangePeerRequest {
        //     // This can be only called in internal RaftStore now.
        //     optional eraftpb.ConfChangeType change_type = 1;
        //     optional metapb.Peer peer                   = 2;
        // }
        let request = request.get_change_peer();
        let peer = request.get_peer();
        let store_id = peer.get_store_id();
        let change_type = request.get_change_type();
        let mut region = self.region().clone();

        warn!("{} exec ConfChange {:?}, epoch: {:?}",
              self.tag,
              util::conf_change_type_str(&change_type),
              region.get_region_epoch());

        // TODO: we should need more check, like peer validation, duplicated id, etc.
        let exists = util::find_peer(&region, store_id).is_some();
        let conf_ver = region.get_region_epoch().get_conf_ver() + 1;

        region.mut_region_epoch().set_conf_ver(conf_ver);

        match change_type {
            eraftpb::ConfChangeType::AddNode => {
                if exists {
                    error!("{} can't add duplicated peer {:?} to region {:?}",
                           self.tag,
                           peer,
                           region);
                    return Err(box_err!("can't add duplicated peer {:?} to region {:?}",
                                        peer,
                                        region));
                }
                // TODO: Do we allow adding peer in same node?

                // Add this peer to cache.
                self.peer_cache.borrow_mut().insert(peer.get_id(), peer.clone());
                self.peer_heartbeats.insert(peer.get_id(), Instant::now());
                region.mut_peers().push(peer.clone());

               
                warn!("{} add peer {:?} to region {:?}",
                      self.tag,
                      peer,
                      self.region());
            }
            eraftpb::ConfChangeType::RemoveNode => {
                if !exists {
                    error!("{} remove missing peer {:?} from region {:?}",
                           self.tag,
                           peer,
                           region);
                    return Err(box_err!("remove missing peer {:?} from region {:?}", peer, region));
                }

                if self.peer_id() == peer.get_id() {
                    // Remove ourself, we will destroy all region data later.
                    // So we need not to apply following logs.
                    self.pending_remove = true;
                }

                // Remove this peer from cache.
                self.peer_cache.borrow_mut().remove(&peer.get_id());
                self.peer_heartbeats.remove(&peer.get_id());
                util::remove_peer(&mut region, store_id).unwrap();

                warn!("{} remove {} from region:{:?}",
                      self.tag,
                      peer.get_id(),
                      self.region());
            }
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
        let mut state = RegionLocalState::new();
        state.set_region(region.clone());
        try!(ctx.wb.put_msg(&keys::region_state_key(region.get_id()), &state));

        let mut resp = AdminResponse::new();
        resp.mut_change_peer().set_region(region.clone());

        Ok((resp,
            Some(ExecResult::ChangePeer {
            change_type: change_type,
            peer: peer.clone(),
            region: region,
        })))
    }

    
    fn exec_write_cmd(&mut self, ctx: &ExecContext) -> Result<RaftCmdResponse> {
        let requests = ctx.req.get_requests();
        let mut responses = Vec::with_capacity(requests.len());

        for req in requests {
            let cmd_type = req.get_cmd_type();
            let mut resp = try!(match cmd_type {
                CmdType::Get => self.do_get(ctx, req),
                CmdType::Put => self.do_put(ctx, req),
                CmdType::Delete => self.do_get(ctx, req),
                CmdType::Snap => self.do_delete(ctx, req),
                CmdType::Invalid => Err(box_err!("invalid cmd type, message maybe currupted")),
            });

            resp.set_cmd_type(cmd_type);
            responses.push(resp);
        }

        let mut resp = RaftCmdResponse::new();
        resp.set_responses(protobuf::RepeatedField::from_vec(responses));
        Ok(resp)
    }

    fn do_get(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        // TODO: the get_get looks wried, maybe we should figure out a better name later.
        let key = req.get_get().get_key();
        //try!(self.check_data_key(key));
        let mut resp = Response::new();
        // let mut res;
        // if self.needle_cache.contains_key(&key){
        //     if let Some(item) = self.needle_cache.get(&key){
        //         debug!("do_get key:[{}] exists! offset:[{}] size:[{}] volume_file:[{:?}]", 
        //             key, item.offset, item.size, self.volume_read_file);
        //         //let mut file_copy = try!(self.volume_read_file.try_clone());
        //         res = Vec::<u8>::with_capacity(item.size as usize);
        //         unsafe { res.set_len(item.size as usize); }
        //         try!(self.volume_read_file.seek(SeekFrom::Start(item.offset)));
        //         debug!("file:[{:?}] seek success!", self.volume_read_file);
        //         let bytes_read = try!(self.volume_read_file.read(&mut res[..]));
        //         resp.mut_get().set_value(res.to_vec());
        //     }
        // }else{
        //     debug!("do_get key:[{}] not exists!", key);
        // }
        
        // let res = if req.get_get().has_cf() {
        //     let cf = req.get_get().get_cf();
        //     // TODO: check whether cf exists or not.
        //     ctx.snap.get_value_cf(cf, &keys::data_key(key)).unwrap_or_else(|e| {
        //         panic!("{} failed to get {} with cf {}: {:?}",
        //                self.tag,
        //                escape(key),
        //                cf,
        //                e)
        //     })
        // } else {
        //     ctx.snap
        //         .get_value(&keys::data_key(key))
        //         .unwrap_or_else(|e| panic!("{} failed to get {}: {:?}", self.tag, escape(key), e))
        // };
        Ok(resp)
    }

    fn do_put(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        let (key, value) = (req.get_put().get_key(), req.get_put().get_value());
        // self.volume_file.write(value);

        // let needle_size = value.len() as u64;
        // let needle_offset = self.volume_file_offset;
        // let cache_item = CacheItem{
        //     offset: needle_offset,
        //     size: needle_size,
        // };

        // let mut buf = vec![0;24];
        // BigEndian::write_u64(&mut buf[0..8], key);
        // BigEndian::write_u64(&mut buf[8..16], cache_item.offset);
        // BigEndian::write_u64(&mut buf[16..24], cache_item.size);
        // self.volume_idx_file.write(&buf);
        // self.volume_file_offset += needle_size;
        // self.needle_cache.insert(key, cache_item);
        let resp = Response::new();
        // let key = keys::data_key(key);
        // if let Some(diff) = self.size_diff_hint.checked_add(key.len() as u64) {
        //     self.size_diff_hint = diff;
        // }
        // if let Some(diff) = self.size_diff_hint.checked_add(value.len() as u64) {
        //     self.size_diff_hint = diff;
        // }
        // self.size_diff_hint += key.len() as u64;
        // self.size_diff_hint += value.len() as u64;
        
        // if req.get_put().has_cf() {
        //     let cf = req.get_put().get_cf();
        //     // TODO: check whether cf exists or not.
        //     debug!("do_put cf:[{}] key:[{}] value:[{}] ", cf, escape(&key), escape(value));
        //     rocksdb::get_cf_handle(&self.engine, cf)
        //         .and_then(|handle| ctx.wb.put_cf(handle, &key, value))
        //         .unwrap_or_else(|e| {
        //             panic!("{} failed to write ({}, {}) to cf {}: {:?}",
        //                    self.tag,
        //                    escape(&key),
        //                    escape(value),
        //                    cf,
        //                    e)
        //         });
        // } else {
        // debug!("do_put key:[{}] value:[{}] volume_file_offset:[{}] needle_size:[{}]", 
        //     key, escape(value), needle_offset, needle_size);

        //     ctx.wb.put(&key, value).unwrap_or_else(|e| {
        //         panic!("{} failed to write ({}, {}): {:?}",
        //                self.tag,
        //                escape(&key),
        //                escape(value),
        //                e);
        //     });
        // }
        Ok(resp)
    }

    fn do_delete(&mut self, ctx: &ExecContext, req: &Request) -> Result<Response> {
        let key = req.get_delete().get_key();
        //try!(self.check_data_key(key));

        // let key = keys::data_key(key);
        // // since size_diff_hint is not accurate, so we just skip calculate the value size.
        // let klen = key.len() as u64;
        // if self.size_diff_hint > klen {
        //     self.size_diff_hint -= klen;
        // } else {
        //     self.size_diff_hint = 0;
        // }
        let resp = Response::new();
        debug!("do_delete key:[{}]", key);
        // if req.get_delete().has_cf() {
        //     let cf = req.get_delete().get_cf();
        //     // TODO: check whether cf exists or not.
        //     rocksdb::get_cf_handle(&self.engine, cf)
        //         .and_then(|handle| ctx.wb.delete_cf(handle, &key))
        //         .unwrap_or_else(|e| {
        //             panic!("{} failed to delete {}: {:?}", self.tag, escape(&key), e)
        //         });
        //     // lock cf is compact periodically.
            
        // } else {
        //     ctx.wb.delete(&key).unwrap_or_else(|e| {
        //         panic!("{} failed to delete {}: {:?}", self.tag, escape(&key), e)
        //     });
        // }

        Ok(resp)
    }
}


fn get_transfer_leader_cmd(msg: &RaftCmdRequest) -> Option<&TransferLeaderRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_transfer_leader() {
        return None;
    }

    Some(req.get_transfer_leader())
}

fn get_change_peer_cmd(msg: &RaftCmdRequest) -> Option<&ChangePeerRequest> {
    if !msg.has_admin_request() {
        return None;
    }
    let req = msg.get_admin_request();
    if !req.has_change_peer() {
        return None;
    }

    Some(req.get_change_peer())
}
fn make_transfer_leader_response() -> RaftCmdResponse {
    let mut response = AdminResponse::new();
    response.set_cmd_type(AdminCmdType::TransferLeader);
    response.set_transfer_leader(TransferLeaderResponse::new());
    let mut resp = RaftCmdResponse::new();
    resp.set_admin_response(response);
    resp
}

