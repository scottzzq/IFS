use std::sync::Arc;
use std::sync::mpsc::{Receiver as StdReceiver, TryRecvError};
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, BTreeMap};
use std::boxed::Box;
use std::collections::Bound::{Excluded, Unbounded};
use std::time::{Duration, Instant};
use util::get_disk_stat;
use rocksdb::DB;
use mio::{self, EventLoop, EventLoopBuilder, Sender};
use protobuf;
use protobuf::RepeatedField;
use protobuf::Message;

use fs2;

use pd::PdClient;
use uuid::Uuid;
use time::{self, Timespec};
use raftstore::{Result, Error, store};
use kvproto::metapb;
use kvproto::pdpb::StoreStats;
use std::{cmp, u64};
use kvproto::raft_cmdpb::{AdminCmdType, AdminRequest, StatusCmdType, StatusResponse,
                          RaftCmdRequest, RaftCmdResponse};
use kvproto::volume_cmdpb::{Request as VolumeCmdRequest, Response as VolumeCmdResponse, 
                          AddRequest as VolumeAddRequest, AddResponse as VolumeAddResponse};

use kvproto::raft_serverpb::{RaftMessage, RaftSnapshotData, RaftTruncatedState, RegionLocalState,
                             PeerState};
use kvproto::eraftpb::{ConfChangeType, Snapshot, MessageType};
use super::peer_storage::{ApplySnapResult, SnapState};

use util::transport::SendCh;
use util::rocksdb;
use util::SlowTimer;
use raftstore::store::engine::{Iterable,Peekable};

use super::worker::{PdRunner, PdTask, RegionTask, RegionRunner};

use super::keys::{self};
use super::{Msg, Tick};
use super::peer::{Peer, ReadyResult, ExecResult, PendingCmd, StaleState};
use super::config::Config;
use super::transport::Transport;
use super::msg::Callback;
use util::worker::{Worker, Scheduler};
use super::{util, SnapManager};
use storage::{ALL_CFS, CF_LOCK};

const MIO_TICK_RATIO: u64 = 10;

const ROCKSDB_TOTAL_SST_FILE_SIZE_PROPERTY: &'static str = "rocksdb.total-sst-files-size";

pub fn create_event_loop<T, C>(cfg: &Config) -> Result<EventLoop<Store<T, C>>>
    where T: Transport,
          C: PdClient
{
    let mut builder = EventLoopBuilder::new();
    // To make raft base tick more accurate, timer tick should be small enough.
    builder.timer_tick(Duration::from_millis(cfg.raft_base_tick_interval / MIO_TICK_RATIO));
    builder.notify_capacity(cfg.notify_capacity);
    builder.messages_per_tick(cfg.messages_per_tick);
    let event_loop = try!(builder.build());
    Ok(event_loop)
}



fn register_timer<T: Transport, C: PdClient>(event_loop: &mut EventLoop<Store<T, C>>,
                                             tick: Tick,
                                             delay: u64)
                                             -> Result<()> {
    // TODO: now mio TimerError doesn't implement Error trait,
    // so we can't use `try!` directly.
    if delay == 0 {
        // 0 delay means turn off the timer.
        return Ok(());
    }
    box_try!(event_loop.timeout(tick, Duration::from_millis(delay)));
    Ok(())
}


// A helper structure to bundle all channels for messages to `Store`.
pub struct StoreChannel {
    pub sender: Sender<Msg>,
}

pub struct Store<T: Transport, C: PdClient + 'static> {
    cfg: Config,
    store: metapb::Store,
    engine: Arc<DB>,
    sendch: SendCh<Msg>,
    // sent_snapshot_count: u64,
    // snapshot_status_receiver: StdReceiver<SnapshotStatusMsg>,
    // // region_id -> peers
    region_peers: HashMap<u64, Peer>,
    pending_raft_groups: HashSet<u64>,
    // // region end key -> region id
    // region_ranges: BTreeMap<Key, u64>,
    pending_regions: Vec<metapb::Region>,
    // split_check_worker: Worker<SplitCheckTask>,
    region_worker: Worker<RegionTask>,
    // raftlog_gc_worker: Worker<RaftlogGcTask>,
    // compact_worker: Worker<CompactTask>,
    pd_worker: Worker<PdTask>,
    // consistency_check_worker: Worker<ConsistencyCheckTask>,

    trans: T,
    pd_client: Arc<C>,
    peer_cache: Rc<RefCell<HashMap<u64, metapb::Peer>>>,
    tag: String,
    snap_mgr: SnapManager,

    // start_time: Timespec,
    // is_busy: bool,

    // region_written_bytes: LocalHistogram,
    // region_written_keys: LocalHistogram,
}

impl<T: Transport, C: PdClient> Store<T, C> {
     pub fn new(ch: StoreChannel,
               meta: metapb::Store,
               cfg: Config,
               engine: Arc<DB>,
               trans: T,
               pd_client: Arc<C>,
               mgr: SnapManager)
               -> Result<Store<T,C>> {
        // TODO: we can get cluster meta regularly too later.
        try!(cfg.validate());

        let sendch = SendCh::new(ch.sender, "raftstore");
        let peer_cache = HashMap::new();
        let tag = format!("[store {}]", meta.get_id());

        let mut s = Store {
            cfg: cfg,
            store: meta,
            engine: engine,
            sendch: sendch,
            // sent_snapshot_count: 0,
            // snapshot_status_receiver: ch.snapshot_status_receiver,
            region_peers: HashMap::new(),
            pending_raft_groups: HashSet::new(),
            // split_check_worker: Worker::new("split check worker"),
            region_worker: Worker::new("snapshot worker"),
            // raftlog_gc_worker: Worker::new("raft gc worker"),
            // compact_worker: Worker::new("compact worker"),
            pd_worker: Worker::new("pd worker"),
            // consistency_check_worker: Worker::new("consistency check worker"),
            // region_ranges: BTreeMap::new(),
            pending_regions: vec![],
            trans: trans,
            pd_client: pd_client,
            peer_cache: Rc::new(RefCell::new(peer_cache)),
            snap_mgr: mgr,
            // raft_metrics: RaftMetrics::default(),
            tag: tag,
            // start_time: time::get_time(),
            // is_busy: false,
            // region_written_bytes: REGION_WRITTEN_BYTES_HISTOGRAM.local(),
            // region_written_keys: REGION_WRITTEN_KEYS_HISTOGRAM.local(),
        };
        try!(s.init());
        Ok(s)
    }

        /// Initialize this store. It scans the db engine, loads all regions
    /// and their peers from it, and schedules snapshot worker if neccessary.
    /// WARN: This store should not be used before initialized.
    fn init(&mut self) -> Result<()> {
        // Scan region meta to get saved regions.
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let engine = self.engine.clone();
        let mut total_count = 0;
        let mut tomebstone_count = 0;
        let mut applying_count = 0;

        let t = Instant::now();
        try!(engine.scan(start_key,
                         end_key,
                         &mut |key, value| {
            let (region_id, suffix) = try!(keys::decode_region_meta_key(key));
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }

            total_count += 1;

            let local_state = try!(protobuf::parse_from_bytes::<RegionLocalState>(value));
            let region = local_state.get_region();
            if local_state.get_state() == PeerState::Tombstone {
                tomebstone_count += 1;
                debug!("region {:?} is tombstone in store {}",
                       region,
                       self.store_id());
                return Ok(true);
            }
            let mut peer = try!(Peer::create(self, region));
            //maself.region_ranges.insert(enc_end_key(region), region_id);
            // No need to check duplicated here, because we use region id as the key
            // in DB.
            self.region_peers.insert(region_id, peer);
            info!("Create Peer for region_id:[{}]", region_id);
            Ok(true)
        }));

        info!("{} starts with {} regions, including {} tombstones and {} applying \
               regions, takes {:?}",
              self.tag,
              total_count,
              tomebstone_count,
              applying_count,
              t.elapsed());

        try!(self.clean_up());

        Ok(())
    }

    pub fn store_id(&self) -> u64 {
        self.store.get_id()
    }

    pub fn engine(&self) -> Arc<DB> {
        self.engine.clone()
    }
    
    pub fn config(&self) -> &Config {
        &self.cfg
    }

    pub fn peer_cache(&self) -> Rc<RefCell<HashMap<u64, metapb::Peer>>> {
        self.peer_cache.clone()
    }

    /// `clean_up` clean up all possible garbage data.
    fn clean_up(&mut self) -> Result<()> {
        Ok(())
    }

    pub fn snap_scheduler(&self) -> Scheduler<RegionTask> {
        self.region_worker.scheduler()
    }

}

impl<T: Transport, C: PdClient> Store<T, C> {
    //注册Store心跳数据包定时器
    fn register_pd_store_heartbeat_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::PdStoreHeartbeat,
                                       self.cfg.pd_store_heartbeat_tick_interval) {
            error!("register pd store heartbeat tick err: {:?}", e);
        };
    }

    fn on_pd_store_heartbeat_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        self.store_heartbeat_pd();
        self.register_pd_store_heartbeat_tick(event_loop);
    }

    fn store_heartbeat_pd(&self) {
        let mut stats = StoreStats::new();
        let disk_stat = match get_disk_stat(self.engine.path()) {
            Ok(disk_stat) => disk_stat,
            Err(_) => {
                error!("get disk stat for rocksdb {} failed", self.engine.path());
                return;
            }
        };

        let capacity = cmp::min(disk_stat.capacity, self.cfg.capacity);
        stats.set_capacity(capacity);
        // Must get the total SST file size here.
        let mut used_size: u64 = 0;
        for cf in ALL_CFS {
            let handle = rocksdb::get_cf_handle(&self.engine, cf).unwrap();
            let cf_used_size = self.engine
                .get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILE_SIZE_PROPERTY)
                .expect("rocksdb is too old, missing total-sst-files-size property");

            used_size += cf_used_size;
        }
        let mut available = if capacity > used_size {
            capacity - used_size
        } else {
            warn!("no available space for store {}", self.store_id());
            0
        };
        // We only care rocksdb SST file size, so we should
        // check disk available here.
        if available > disk_stat.available {
            available = disk_stat.available
        }
        stats.set_store_id(self.store_id());
        stats.set_available(available);
        stats.set_region_count(self.region_peers.len() as u32);
        // let snap_stats = self.snap_mgr.rl().stats();
        // stats.set_sending_snap_count(snap_stats.sending_count as u32);
        // stats.set_receiving_snap_count(snap_stats.receiving_count as u32);
        if let Err(e) = self.pd_worker.schedule(PdTask::StoreHeartbeat { stats: stats }) {
            error!("failed to notify pd: {}", e);
        }
    }

    fn register_pd_heartbeat_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::PdHeartbeat,
                                       self.cfg.pd_heartbeat_tick_interval) {
            error!("register pd heartbeat tick err: {:?}", e);
        };
    }

    fn on_pd_heartbeat_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        for peer in self.region_peers.values_mut() {
            peer.check_peers();
        }
        let mut leader_count = 0;
        for peer in self.region_peers.values() {
            if peer.is_leader() {
                leader_count += 1;
                self.heartbeat_pd(peer);
            }
        }
        self.register_pd_heartbeat_tick(event_loop);
    }

    fn heartbeat_pd(&self, peer: &Peer) {
        let mut region =  peer.region().clone();
        region.set_term(peer.term());

        let task = PdTask::Heartbeat {
            region: region,
            peer: peer.peer.clone(),
            down_peers: peer.collect_down_peers(self.cfg.max_peer_down_duration),
        };
        if let Err(e) = self.pd_worker.schedule(task) {
            error!("{} failed to notify pd: {}", peer.tag, e);
        }
    }

    fn first_register_pd_heartbeat_tick(&self, event_loop: &mut EventLoop<Self>) {
        if let Err(e) = register_timer(event_loop,
                                       Tick::PdHeartbeat,
                                       5000) {
            error!("register pd heartbeat tick err: {:?}", e);
        };
    }

    pub fn run(&mut self, event_loop: &mut EventLoop<Self>) -> Result<()> {
        self.register_raft_base_tick(event_loop);
        //定时Store心跳定时器
        self.register_pd_store_heartbeat_tick(event_loop);
        //定时Region心跳定时器
        self.first_register_pd_heartbeat_tick(event_loop);
        //定时Store心跳线程

        let pd_runner = PdRunner::new(self.pd_client.clone(), self.sendch.clone());
        box_try!(self.pd_worker.start(pd_runner));

        let runner = RegionRunner::new(self.engine.clone(),
                                       self.get_sendch(),
                                       self.snap_mgr.clone(),
                                       self.cfg.snap_apply_batch_size);
        box_try!(self.region_worker.start(runner));

        try!(event_loop.run(self));
        Ok(())
    }

    fn register_raft_base_tick(&self, event_loop: &mut EventLoop<Self>) {
        // If we register raft base tick failed, the whole raft can't run correctly,
        // TODO: shutdown the store?
        if let Err(e) = register_timer(event_loop, Tick::Raft, self.cfg.raft_base_tick_interval) {
            error!("{} register raft base tick err: {:?}", self.tag, e);
        };
    }

    fn on_raft_base_tick(&mut self, event_loop: &mut EventLoop<Self>) {
        let t = Instant::now();
        for (&region_id, peer) in &mut self.region_peers {
            peer.raft_group.tick();
            // If this peer detects the leader is missing for a long long time,
            // it should consider itself as a stale peer which is removed from
            // the original cluster.
            // This most likely happens in the following scenario:
            // At first, there are three peer A, B, C in the cluster, and A is leader.
            // Peer B gets down. And then A adds D, E, F into the cluster.
            // Peer D becomes leader of the new cluster, and then removes peer A, B, C.
            // After all these peer in and out, now the cluster has peer D, E, F.
            // If peer B goes up at this moment, it still thinks it is one of the cluster
            // and has peers A, C. However, it could not reach A, C since they are removed
            // from the cluster or probably destroyed.
            // Meantime, D, E, F would not reach B, since it's not in the cluster anymore.
            // In this case, peer B would notice that the leader is missing for a long time,
            // and it would check with pd to confirm whether it's still a member of the cluster.
            // If not, it destroys itself as a stale peer which is removed out already.
            match peer.check_stale_state(self.cfg.max_leader_missing_duration) {
                StaleState::Valid => {
                    self.pending_raft_groups.insert(region_id);
                }
                StaleState::ToValidate => {
                    // for peer B in case 1 above
                    info!("{} detects leader missing for a long time. To check with pd \
                           whether it's still valid",
                          peer.tag);

                    //debug
                    // let task = PdTask::ValidatePeer {
                    //     peer: peer.peer.clone(),
                    //     region: peer.region().clone(),
                    // };
                    // if let Err(e) = self.pd_worker.schedule(task) {
                    //     error!("{} failed to notify pd: {}", peer.tag, e)
                    // }

                    self.pending_raft_groups.insert(region_id);
                }
            }
        }
        self.register_raft_base_tick(event_loop);
    }

    fn on_raft_ready(&mut self)  -> Result<()> {
        let t = SlowTimer::new();
        let ids: Vec<u64> = self.pending_raft_groups.drain().collect();
        let pending_count = ids.len();
        for region_id in ids {
            let mut ready_result = None;
            if let Some(peer) = self.region_peers.get_mut(&region_id) {
                //获取Peer执行的结果
                match peer.handle_raft_ready(&self.trans) {
                    Err(e) => {
                        // TODO: should we panic or shutdown the store?
                        error!("{} handle raft ready err: {:?}", peer.tag, e);
                        return Err(e);
                    }
                    Ok(ready) => ready_result = ready,
                }
            }

            if let Some(ready_result) = ready_result {
                if let Err(e) = self.on_ready_result(region_id, ready_result) {
                    error!("[region {}] handle raft ready result err: {:?}",
                           region_id,
                           e);
                    return Err(e);
                }
            }
        }
        slow_log!(t, "on {} regions raft ready", pending_count);
        Ok(())
    }

    //用Peer.handle_raft_ready执行得到的结果
    fn on_ready_result(&mut self, region_id: u64, ready_result: ReadyResult) -> Result<()> { 
        // if let Some(apply_result) = ready_result.apply_snap_result {
        //     self.on_ready_apply_snapshot(apply_result);
        // }
        let t = SlowTimer::new();
        let result_count = ready_result.exec_results.len();
        // handle executing committed log results
        for result in ready_result.exec_results {
            match result {
                ExecResult::ChangePeer { change_type, peer, .. } => {
                    self.on_ready_change_peer(region_id, change_type, peer)
                }
               // ExecResult::CompactLog { state } => {},//self.on_ready_compact_log(region_id, state),
                //ExecResult::SplitRegion { left, right } => {}
                //     self.on_ready_split_region(region_id, left, right)
                // }
                }
        }
        
        slow_log!(t,
                  "[region {}] on ready {} results",
                  region_id,
                  result_count);
        Ok(())
    }

    fn on_ready_change_peer(&mut self,
                            region_id: u64,
                            change_type: ConfChangeType,
                            peer: metapb::Peer) {
        let mut peer_id = 0;
        if let Some(p) = self.region_peers.get(&region_id) {
            if p.is_leader() {
                // Notify pd immediately.
                info!("{} notify pd with change peer region {:?}",
                      p.tag,
                      p.region());
                //self.heartbeat_pd(p);
            }
            peer_id = p.peer_id();
        }

        // We only care remove itself now.
        if change_type == ConfChangeType::RemoveNode && peer.get_store_id() == self.store_id() {
            if peer_id == peer.get_id() {
                self.destroy_peer(region_id, peer)
            } else {
                panic!("trying to remove unknown peer {:?}", peer);
            }
        }
    }

    fn destroy_peer(&mut self, region_id: u64, peer: metapb::Peer) {
        warn!("[region {}] destroy peer {:?}", region_id, peer);
        // TODO: should we check None here?
        // Can we destroy it in another thread later?
        let mut p = self.region_peers.remove(&region_id).unwrap();
        // // We can't destroy a peer which is applying snapshot.
        // //assert!(!p.is_applying());

        let is_initialized = p.is_initialized();
        if let Err(e) = p.destroy() {
            // should panic here?
            error!("[region {}] destroy peer {:?} in store {} err {:?}",
                   region_id,
                   peer,
                   self.store_id(),
                   e);
            return;
        }

        // if is_initialized && self.region_ranges.remove(&enc_end_key(p.region())).is_none() {
        //     panic!("[region {}] remove peer {:?} in store {}",
        //            region_id,
        //            peer,
        //            self.store_id());
        // }
    }

    // return false means the message is invalid, and can be ignored.
    fn is_raft_msg_valid(&self, msg: &RaftMessage) -> bool {
        let region_id = msg.get_region_id();
        let from = msg.get_from_peer();
        let to = msg.get_to_peer();

        debug!("[region {}] handle raft message {:?}, from {} to {}",
               region_id,
               msg.get_message().get_msg_type(),
               from.get_id(),
               to.get_id());

        if to.get_store_id() != self.store_id() {
            warn!("[region {}] store not match, to store id {}, mine {}, ignore it",
                  region_id,
                  to.get_store_id(),
                  self.store_id());
            return false;
        }

        if !msg.has_region_epoch() {
            error!("[region {}] missing epoch in raft message, ignore it",
                   region_id);
            return false;
        }

        true
    }

    fn handle_gc_peer_msg(&mut self, msg: &RaftMessage) {
        let region_id = msg.get_region_id();

        let mut need_remove = false;
        if let Some(peer) = self.region_peers.get(&region_id) {
            // TODO: need checking peer id changed?
            let from_epoch = msg.get_region_epoch();
            if util::is_epoch_stale(peer.get_store().region.get_region_epoch(), from_epoch) {
                // TODO: ask pd to guarantee we are stale now.
                warn!("[region {}] peer {:?} receives gc message, remove",
                      region_id,
                      msg.get_to_peer());
                need_remove = true;
            }
        }

        if need_remove {
            self.destroy_peer(region_id, msg.get_to_peer().clone());
        }
    }

    fn handle_stale_msg(&self, msg: &RaftMessage, cur_epoch: &metapb::RegionEpoch, need_gc: bool) {
        let region_id = msg.get_region_id();
        let from_peer = msg.get_from_peer();
        let to_peer = msg.get_to_peer();

        if !need_gc {
            warn!("[region {}] raft message {:?} is stale, current {:?}, ignore it",
                  region_id,
                  msg,
                  cur_epoch);
            return;
        }

        warn!("[region {}] raft message {:?} is stale, current {:?}, tell to gc",
              region_id,
              msg,
              cur_epoch);

        let mut gc_msg = RaftMessage::new();
        gc_msg.set_region_id(region_id);
        gc_msg.set_from_peer(to_peer.clone());
        gc_msg.set_to_peer(from_peer.clone());
        gc_msg.set_region_epoch(cur_epoch.clone());
        gc_msg.set_is_tombstone(true);
        if let Err(e) = self.trans.send(gc_msg) {
            error!("[region {}] send gc message failed {:?}", region_id, e);
        }
    }

    fn is_msg_stale(&self, msg: &RaftMessage) -> Result<bool> {
        let region_id = msg.get_region_id();
        let from_epoch = msg.get_region_epoch();
        let is_vote_msg = msg.get_message().get_msg_type() == MessageType::MsgRequestVote;
        let from_store_id = msg.get_from_peer().get_store_id();

        // Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
        // a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
        //  We should ignore this stale message and let 2 remove itself after
        //  applying the ConfChange log.
        // 1删除了2，2仍然可能给1发送MsgAppendResponse消息，1收到这样的消息直接忽略

        // b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
        //  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
        // 2网络隔离，1删除了2，2重新加入集群给1和3发送MsgRequestVote消息，需要回复gc

        // c. 2 is isolated but can communicate with 3. 1 removes 3.
        //  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
        // 2网络隔离，但是可以和3通信，1删除了3，3直接忽略这条消息

        // d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
        //  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
        // 2网络隔离，可以和3通信，1删除了2，增加了4，删除了3, 需要回复gc

        // e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
        //  After 2 rejoins the cluster, 2 may send stale MsgRequestVote消 to 1 and 3,
        //  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
        //  rejoin the raft group again.
        // 2网络隔离，1增加了4、5、6，删除了3，目前4是leader
        // 2重新加入集群，会给1、3发送MsgRequestVote消息，1、3忽略，稍后4会给2发送消息，2会被重新加入到raft group中

        // f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
        //  unlike case e, 2 will be stale forever.
        // 2网络隔离，1增加了4、5、6，删除了1、3，目前4是leader，4删除了2
        // 2重新加入集群会给1、3发送消息，但是1、3都会忽略，此时2将会得不到删除消息，只能通过定时给pd验证

        // TODO: for case f, if 2 is stale for a long time, 2 will communicate with pd and pd will
        // tell 2 is stale, so 2 can remove itself.

        if let Some(peer) = self.region_peers.get(&region_id) {
            // a. 收到的不是MsgRequestVote，所以直接忽略
            // b. 收到的是MsgRequestVote，需要告诉它删除自己
            let region = &peer.get_store().region;
            let epoch = region.get_region_epoch();

            //message RegionEpoch {
            //    // Conf change version, auto increment when add or remove peer
            //    新增节点、删除节点，每次配置变更增加
            //    optional uint64 conf_ver    = 1 [(gogoproto.nullable) = false];
            //    // Region version, auto increment when split or merge
            //    每次分裂、合并增加
            //    optional uint64 version     = 2 [(gogoproto.nullable) = false];
            //}
            //
            //message Region {
            //    optional uint64 id                  = 1 [(gogoproto.nullable) = false];
            //    // Region key range [start_key, end_key).
            //    optional bytes  start_key           = 2;
            //    optional bytes  end_key             = 3;
            //    optional RegionEpoch region_epoch   = 4;
            //    repeated Peer   peers               = 5;
            //}
            //
            //message Peer {      
            //    optional uint64 id          = 1 [(gogoproto.nullable) = false]; 
            //    optional uint64 store_id    = 2 [(gogoproto.nullable) = false];
            //}

            //当前region存在当前store中需要
            //1、检查version和conf_ver
            //2、检查Region配置中有消息来源的store_id
            if util::is_epoch_stale(from_epoch, epoch) &&
               util::find_peer(region, from_store_id).is_none() {
                // The message is stale and not in current region.
                self.handle_stale_msg(msg, epoch, is_vote_msg);
                return Ok(true);
            }

            return Ok(false);
        }

        //c. 3已经被删除了，但是2还没有被删除，收到2的MsgRequestVote消息需要忽略
        //d. 2、3已经被删除了，收到2的MsgRequestVote消息需要告诉它删除自己
        //e. 3、1已经被删除了，收到2的MsgRequestVote消息直接忽略
        //f.
        // no exist, check with tombstone key.
        let state_key = keys::region_state_key(region_id);
        if let Some(local_state) = try!(self.engine.get_msg::<RegionLocalState>(&state_key)) {
            if local_state.get_state() == PeerState::Tombstone {
                let region = local_state.get_region();
                let region_epoch = region.get_region_epoch();
                // The region in this peer is already destroyed
                //消息中的epoch 和rocksdb中的epoch比较
                //比较version conf_ver， 如果是消息中的小就是过期了
                if util::is_epoch_stale(from_epoch, region_epoch) {
                    info!("[region {}] tombstone peer [epoch: {:?}] \
                        receive a stale message {:?}", region_id,
                        region_epoch,
                          msg,
                          );

                    let not_exist = util::find_peer(region, from_store_id).is_none();
                    self.handle_stale_msg(msg, region_epoch, is_vote_msg && not_exist);

                    return Ok(true);
                }

                //消息中conf_ver应该比当前rocksdb中的大才正确
                if from_epoch.get_conf_ver() == region_epoch.get_conf_ver() {
                    return Err(box_err!("tombstone peer [epoch: {:?}] receive an invalid \
                                         message {:?}, ignore it",
                                        region_epoch,
                                        msg));
                }
            }
        }

        Ok(false)
    }

    fn insert_peer_cache(&mut self, peer: metapb::Peer) {
        self.peer_cache.borrow_mut().insert(peer.get_id(), peer);
    }

    pub fn get_sendch(&self) -> SendCh<Msg> {
        self.sendch.clone()
    }


    fn check_target_peer_valid(&mut self, region_id: u64, target: &metapb::Peer) -> Result<bool> {
        // we may encounter a message with larger peer id, which means
        // current peer is stale, then we should remove current peer
        let mut has_peer = false;
        let mut stale_peer = None;
        if let Some(p) = self.region_peers.get_mut(&region_id) {
            has_peer = true;
            let target_peer_id = target.get_id();
            if p.peer_id() < target_peer_id {
                if p.is_applying_snapshot() && !p.mut_store().cancel_applying_snap() {
                    warn!("[region {}] Stale peer {} is applying snapshot, will destroy next \
                           time.",
                          region_id,
                          p.peer_id());
                    return Ok(false);
                }
                stale_peer = Some(p.peer.clone());
            } else if p.peer_id() > target_peer_id {
                warn!("target peer id {} is less than {}, msg maybe stale.",
                      target_peer_id,
                      p.peer_id());
                return Ok(false);
            }
        }
        if let Some(p) = stale_peer {
            info!("[region {}] destroying stale peer {:?}", region_id, p);
            self.destroy_peer(region_id, p);
            has_peer = false;
        }

        if !has_peer {
            let peer = try!(Peer::replicate(self, region_id, target.get_id()));
            // We don't have start_key of the region, so there is no need to insert into
            // region_ranges
            self.region_peers.insert(region_id, peer);
        }
        Ok(true)
    }
    
    fn check_snapshot(&mut self, msg: &RaftMessage) -> Result<bool> {
        let region_id = msg.get_region_id();
        // Check if we can accept the snapshot
        if self.region_peers[&region_id].get_store().is_initialized() ||
           !msg.get_message().has_snapshot() {
            return Ok(true);
        }

        // message ConfState {
        //     repeated uint64 nodes = 1;
        // }
        //message SnapshotMetadata {
        //     optional ConfState conf_state = 1; 
        //     optional uint64    index      = 2; 
        //     optional uint64    term       = 3; 
        // }
        // message Snapshot {
        //     optional bytes            data     = 1;
        //     optional SnapshotMetadata metadata = 2;
        // }

        let snap = msg.get_message().get_snapshot();
        // message SnapshotCFFile {
        //     optional string cf       = 1;
        //     optional uint64 size     = 2;
        //     optional uint32 checksum = 3;
        // }
        // message SnapshotMeta {
        //     repeated SnapshotCFFile cf_files = 1;
        // }
        // message KeyValue {
        //     optional bytes key      = 1;
        //     optional bytes value    = 2;
        // }
        // message RaftSnapshotData {
        //     optional metapb.Region region   = 1;
        //     optional uint64 file_size       = 2;
        //     repeated KeyValue data          = 3;
        //     optional uint64 version         = 4;
        //     optional SnapshotMeta meta      = 5;
        // }
        let mut snap_data = RaftSnapshotData::new();
        try!(snap_data.merge_from_bytes(snap.get_data()));
        let snap_region = snap_data.take_region();
        let peer_id = msg.get_to_peer().get_id();

        if snap_region.get_peers().into_iter().all(|p| p.get_id() != peer_id) {
            warn!("region {:?} doesn't contain peer {:?}, skip.",
                  snap_region,
                  msg.get_to_peer());
            return Ok(false);
        }
        self.pending_regions.push(snap_region);

        Ok(true)
    }

    fn on_raft_message(&mut self, mut msg: RaftMessage) -> Result<()> {
        let region_id = msg.get_region_id();
        //to_peer字段中的store_id是否是当前store_id
        if !self.is_raft_msg_valid(&msg) {
            return Ok(());
        }

        //msg是否是gc消息
        if msg.get_is_tombstone() {
            // we receive a message tells us to remove ourself.
            self.handle_gc_peer_msg(&msg);
            return Ok(());
        }
        //消息是否过期
        // a.1删除了2，2仍然可能给1发送MsgAppendResponse消息，1收到这样的消息直接忽略
        // b. 2网络隔离，1删除了2，2重新加入集群给1和3发送MsgRequestVote消息，需要回复gc
        // c. 2网络隔离，但是可以和3通信，1删除了3，3直接忽略这条消息
        // d. 2网络隔离，可以和3通信，1删除了2，增加了4，删除了3, 需要回复gc
        // e. 2网络隔离，1增加了4、5、6，删除了3，目前4是leader,2重新加入集群，会给1、3发送MsgRequestVote消息，1、3忽略，稍后4会给2发送消息，2会被重新加入到raft group中
        // f. 2网络隔离，1增加了4、5、6，删除了1、3，目前4是leader，4删除了2,2重新加入集群会给1、3发送消息，但是1、3都会忽略，此时2将会得不到删除消息，只能通过定时给pd验证
        if try!(self.is_msg_stale(&msg)) {
            return Ok(());
        }

        //目标Peer是否存在，如果不存在，可能是添加分片的请求，就创建出来Peer来
        if !try!(self.check_target_peer_valid(region_id, msg.get_to_peer())) {
            return Ok(());
        }

        //如果是不包含snapshot消息 ，返回true
        //storage已经初始化完成，返回true
        //包含Snapshot，并且合法，返回true
        if !try!(self.check_snapshot(&msg)) {
            return Ok(());
        }

        self.insert_peer_cache(msg.take_from_peer());
        self.insert_peer_cache(msg.take_to_peer());

        let peer = self.region_peers.get_mut(&region_id).unwrap();
        let timer = SlowTimer::new();
        try!(peer.step(msg.take_message()));
        slow_log!(timer, "{} raft step", peer.tag);

        // Add into pending raft groups for later handling ready.
        self.pending_raft_groups.insert(region_id);

        Ok(())
    }

    fn validate_store_id(&self, msg: &RaftCmdRequest) -> Result<()> {
        let store_id = msg.get_header().get_peer().get_store_id();
        if store_id != self.store.get_id() {
            return Err(Error::StoreNotMatch(store_id, self.store.get_id()));
        }
        Ok(())
    }

    fn validate_region(&self, msg: &RaftCmdRequest) -> Result<()> {
        let region_id = msg.get_header().get_region_id();
        let peer_id = msg.get_header().get_peer().get_id();

        let peer = match self.region_peers.get(&region_id) {
            Some(peer) => peer,
            None => return Err(Error::RegionNotFound(region_id)),
        };
        if !peer.is_leader() {
            return Err(Error::NotLeader(region_id, peer.get_peer_from_cache(peer.leader_id())));
        }
        if peer.peer_id() != peer_id {
            return Err(box_err!("mismatch peer id {} != {}", peer.peer_id(), peer_id));
        }

        let header = msg.get_header();
        // If header's term is 2 verions behind current term, leadership may have been changed away.
        if header.get_term() > 0 && peer.term() > header.get_term() + 1 {
            return Err(Error::StaleCommand);
        }

        let res = peer.check_epoch(msg);
        res
    }

    fn alloc_id(&self) -> Result<u64> {
        let id = try!(self.pd_client.alloc_id());
        Ok(id)
    }

    fn alloc_volume_id(&self) -> Result<u64> {
        let id = try!(self.pd_client.alloc_volume_id());
        Ok(id)
    }

    fn bootstrap_region(&mut self) -> Result<metapb::Region> {
        let region_id = try!(self.alloc_volume_id());
        info!("alloc region id {}, store {}", region_id, self.store_id());
        let peer_id = try!(self.alloc_id());
        info!("alloc peer id {} for region {}", peer_id, region_id);
        let region = try!(store::bootstrap_region(&self.engine, self.store_id(), region_id, peer_id));
        
        let mut peer = try!(Peer::create(self, &region));
        self.region_peers.insert(region_id, peer);
        info!("Create Peer for region_id:[{}]", region_id);
        Ok(region)
    }

    fn propose_raft_command(&mut self, msg: RaftCmdRequest, cb: Callback)  -> Result<()> {
        debug!("store propose_raft_command");
        let mut resp = RaftCmdResponse::new();
        let uuid: Uuid = match get_uuid_from_req(&msg) {
            None => {
                error!("uuid is null! RaftCmdRequest: {:?}", msg);
                let e = Error::Other("missing request uuid".into());
                resp.mut_header().set_error(e.into());
                return cb.call_box((resp,));
            }
            Some(uuid) => {
                resp.mut_header().set_uuid(uuid.as_bytes().to_vec());
                debug!("uuid is:[{:?}]", uuid);
                uuid
            }
        };

        if let Err(e) = self.validate_store_id(&msg) {
            error!("validate_store_id, is not validate store id, error:[{:?}]", e);
            resp.mut_header().set_error(e.into());
            return cb.call_box((resp,));
        }
        info!("validate_store_id, is validate store id");
        // if msg.has_status_request() {
        //     // For status commands, we handle it here directly.
        //     match self.execute_status_command(msg) {
        //         Err(e) => {
        //             resp.mut_header().set_error(e.into());
        //         }
        //         Ok(status_resp) => resp = status_resp,
        //     };
        //     return cb.call_box((resp,));
        // }

        if let Err(e) = self.validate_region(&msg) {
            error!("validate_region, is not validate region id, error:[{:?}]", e);
            resp.mut_header().set_error(e.into());
            return cb.call_box((resp,));
        }
        info!("validate_region, is validate region id");
        // Note:
        // The peer that is being checked is a leader. It might step down to be a follower later. It
        // doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
        // command log entry can't be committed.
        let region_id = msg.get_header().get_region_id();
        let mut peer = self.region_peers.get_mut(&region_id).unwrap();
        let term = peer.term();
        if term > 0{
            resp.mut_header().set_current_term(term);
        }
        let pending_cmd = PendingCmd {
            uuid: uuid,
            term: term,
            cb: cb,
        };

        try!(peer.propose(pending_cmd, msg, resp));

        self.pending_raft_groups.insert(region_id);
        // TODO: add timeout, if the command is not applied after timeout,
        // we will call the callback with timeout error.
        return Ok(())
    }

    fn on_unreachable(&mut self, region_id: u64, to_peer_id: u64) {
        if let Some(mut peer) = self.region_peers.get_mut(&region_id) {
            peer.raft_group.report_unreachable(to_peer_id);
        }
    }

    #[inline]
    pub fn get_snap_mgr(&self) -> SnapManager {
        self.snap_mgr.clone()
    }

    fn on_snap_apply_res(&mut self, region_id: u64, is_success: bool, is_aborted: bool) {
        let peer = self.region_peers.get_mut(&region_id).unwrap();
        let mut storage = peer.mut_store();
        assert!(storage.is_applying_snap(),
                "snap state should not change during applying");
        if is_success {
            storage.set_snap_state(SnapState::Relax);
        } else {
            if !is_aborted {
                // TODO: cleanup region and treat it as tombstone.
                panic!("applying snapshot to {} failed", region_id);
            }
            self.pending_raft_groups.insert(region_id);
            storage.set_snap_state(SnapState::ApplyAborted);
        }
    }
    
    fn on_snap_gen_res(&mut self, region_id: u64, snap: Option<Snapshot>) {
        let peer = match self.region_peers.get_mut(&region_id) {
            None => return,
            Some(peer) => peer,
        };
        let mut storage = peer.mut_store();
        if !storage.is_snap_state(SnapState::Generating) {
            // snapshot no need anymore.
            return;
        }
        match snap {
            Some(snap) => {
                storage.set_snap_state(SnapState::Snap(snap));
            }
            None => {
                storage.set_snap_state(SnapState::Failed);
            }
        }
    }
}

impl<T: Transport, C: PdClient> mio::Handler for Store<T, C> {
    type Timeout = Tick;
    type Message = Msg;

    fn notify(&mut self, event_loop: &mut EventLoop<Self>, msg: Msg) {
                let t = SlowTimer::new();
        let msg_str = format!("{:?}", msg);
        match msg {
            Msg::RaftMessage(data) => {
                if let Err(e) = self.on_raft_message(data) {
                    error!("handle raft message err: {:?}", e);
                }
            }
            Msg::RaftCmd { send_time, request, callback } => {
                self.propose_raft_command(request, callback);
            }
            Msg::VolumeCmd { send_time, request, callback } => {
                info!("Receive VolumeCmd:[{:?}]", request);
                let boot_res = self.bootstrap_region();
                match boot_res{
                    Ok(region) => {
                        let mut volume_ids = Vec::with_capacity(1);
                        volume_ids.push(region.get_id());

                        let mut add_resp = VolumeAddResponse::new();
                        add_resp.set_volume_ids(volume_ids);

                        let mut resp = VolumeCmdResponse::new();
                        resp.set_add(add_resp);
                        callback.call_box((resp,));
                    }
                    Err(e) => {
                        error!("bootstrap_region error {:?}", e);
                        let mut volume_ids = Vec::with_capacity(1);
                        let mut add_resp = VolumeAddResponse::new();
                        add_resp.set_volume_ids(volume_ids);

                        let mut resp = VolumeCmdResponse::new();
                        resp.set_add(add_resp);
                        callback.call_box((resp,));
                    }
                }
            }

            Msg::ReportUnreachable { region_id, to_peer_id } => {
                self.on_unreachable(region_id, to_peer_id);
            }
            //接收或者发送snapshot的时候，需要发送一条心跳消息，供pd调度
            Msg::SnapshotStats => {}//self.store_heartbeat_pd(), 

            //apply snapshot的时候
            Msg::SnapApplyRes { region_id, is_success, is_aborted } => {
                    self.on_snap_apply_res(region_id, is_success, is_aborted);
            }

            //生成snapshot
            Msg::SnapGenRes { region_id, snap } => {
                info!("SnapGenRes Success!!!!");
                self.on_snap_gen_res(region_id, snap);
            }
            Msg::Quit => {
                info!("receive quit message");
                event_loop.shutdown();
            }
        }
        slow_log!(t, "handle {:?}", msg_str);
    }

    fn timeout(&mut self, event_loop: &mut EventLoop<Self>, timeout: Tick) {
         match timeout {
            Tick::Raft => {
                self.on_raft_base_tick(event_loop);
            },
            Tick::RaftLogGc => {}, //self.on_raft_gc_log_tick(event_loop),
            Tick::SplitRegionCheck => {},//self.on_split_region_check_tick(event_loop),
            Tick::CompactCheck => {},//self.on_compact_check_tick(event_loop),
            //Region心跳定时器
            Tick::PdHeartbeat => {
                self.on_pd_heartbeat_tick(event_loop);
            },
            //Store心跳定时器
            Tick::PdStoreHeartbeat => {
                self.on_pd_store_heartbeat_tick(event_loop);
            },
            Tick::SnapGc => {},//self.on_snap_mgr_gc(event_loop),
            Tick::CompactLockCf => {},//self.on_compact_lock_cf(event_loop),
            Tick::ConsistencyCheck => {},//self.on_consistency_check_tick(event_loop),
            Tick::ReportRegionFlow => {},//elf.on_report_region_flow(event_loop),
        }
    }

    // This method is invoked very frequently, should avoid time consuming operation.
    fn tick(&mut self, event_loop: &mut EventLoop<Self>) {
        // We handle raft ready in event loop.
        if !self.pending_raft_groups.is_empty() {
            self.on_raft_ready();
        }
        self.pending_regions.clear();
    }
}

/////////util///////////
pub fn get_uuid_from_req(cmd: &RaftCmdRequest) -> Option<Uuid> {
    //debug!("uuid:{:?}", cmd.get_header().get_uuid());
    Uuid::from_bytes(cmd.get_header().get_uuid()).ok()
}
