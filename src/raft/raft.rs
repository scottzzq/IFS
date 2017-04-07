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

// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


use std::cmp;
use std::boxed::Box;
use raft::storage::Storage;
use raft::progress::{Progress, Inflights, ProgressState};
use raft::errors::{Result, Error, StorageError};
use kvproto::eraftpb::{HardState, Entry, EntryType, Message, Snapshot, MessageType};
use raft::raft_log::{self, RaftLog};
use std::collections::HashMap;
use rand::{self, Rng};
use protobuf::repeated::RepeatedField;

// CAMPAIGN_PRE_ELECTION represents the first phase of a normal election when
// Config.pre_vote is true.
const CAMPAIGN_PRE_ELECTION: &'static [u8] = b"CampaignPreElection";
// CAMPAIGN_ELECTION represents a normal (time-based) election (the second phase
// of the election when Config.pre_vote is true).
const CAMPAIGN_ELECTION: &'static [u8] = b"CampaignElection";
// CAMPAIGN_TRANSFER represents the type of leader transfer.
const CAMPAIGN_TRANSFER: &'static [u8] = b"CampaignTransfer";

// A constant represents invalid id of raft.
pub const INVALID_ID: u64 = 0;
// A constant represents invalid index of raft log.
pub const INVALID_INDEX: u64 = 0;

//raft group配置
#[derive(Default)]
pub struct Config{
	//唯一的id
	pub id: u64,
	//其他peer的id
	pub peers: Vec<u64>,
	//超时触发选举的单位tick
	pub election_tick : usize,
	//心跳包单位tick
	pub heartbeat_tick: usize,

	//已经applied的index
	pub applied: u64,
	//消息最大限制
	pub max_size_per_msg: u64,
	//并发消息限制
	pub max_inflight_msgs: usize,
	//leader是否定期检测
	pub check_quorum: bool,
	//预先投票
	pub pre_vote: bool,

	/// read_only_option specifies how the read only request is processed.
    //pub read_only_option: ReadOnlyOption,
    /// tag is only used for logging
    pub tag: String,
}	

impl Config {
    pub fn validate(&self) -> Result<()>{
        if self.id == INVALID_ID {
            return Err(Error::ConfigInvalid("invalid node id".to_owned()));
        }
        if self.heartbeat_tick == 0 {
            return Err(Error::ConfigInvalid("heartbeat tick must greater than 0".to_owned()));
        }
        if self.election_tick <= self.heartbeat_tick {
            return Err(Error::ConfigInvalid("election tick must be greater than heartbeat tick"
                .to_owned()));
        }
        if self.max_inflight_msgs == 0 {
            return Err(Error::ConfigInvalid("max inflight messages must be greater than 0"
                .to_owned()));
        }
        Ok(())
    }
}
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum StateRole {
    Follower,
    Candidate,
    Leader,
    PreCandidate,
}

impl Default for StateRole {
    fn default() -> StateRole {
        StateRole::Follower
    }
}

// SoftState provides state that is useful for logging and debugging.
// The state is volatile and does not need to be persisted to the WAL.
#[derive(Default, PartialEq, Debug)]
pub struct SoftState {
    pub leader_id: u64,
    pub raft_state: StateRole,
}

//变成Leader之后，首先需要生成进度信息
fn new_progress(next_idx: u64, ins_size: usize) -> Progress {
    Progress {
        next_idx: next_idx,
        ins: Inflights::new(ins_size),
        ..Default::default()
    }
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
// }
// message SnapshotMetadata {
//     optional ConfState conf_state = 1; 
//     optional uint64    index      = 2; 
//     optional uint64    term       = 3; 
// }
// message Snapshot {
//     optional bytes            data     = 1;
//     optional SnapshotMetadata metadata = 2;
// }
// enum MessageType {
//     MsgHup                    = 0;
//     MsgBeat                   = 1;
//     MsgPropose                = 2;
//     MsgAppend                 = 3;
//     MsgAppendResponse         = 4;
//     MsgRequestVote            = 5;
//     MsgRequestVoteResponse    = 6;
//     MsgSnapshot               = 7;
//     MsgHeartbeat              = 8;
//     MsgHeartbeatResponse      = 9;
//     MsgUnreachable            = 10;
//     MsgSnapStatus             = 11;
//     MsgCheckQuorum            = 12;
//     MsgTransferLeader         = 13;
//     MsgTimeoutNow             = 14;
//     MsgReadIndex              = 15;
//     MsgReadIndexResp          = 16;
//     MsgRequestPreVote         = 17;
//     MsgRequestPreVoteResponse = 18;
// }
// message Message {
//     optional MessageType msg_type       = 1;  
//     optional uint64      to             = 2;  
//     optional uint64      from           = 3;  
//     optional uint64      term           = 4;  
//     optional uint64      log_term       = 5;  
//     optional uint64      index          = 6;  
//     repeated Entry       entries        = 7;  
//     optional uint64      commit         = 8;  
//     optional Snapshot    snapshot       = 9;  
//     optional bool        reject         = 10; 
//     optional uint64      reject_hint    = 11; 
//     optional bytes       context        = 12; 
// }
//给group中其他server发消息
fn new_message(to: u64, field_type: MessageType, from: Option<u64>) -> Message {
    let mut m = Message::new();
    m.set_to(to);
    if let Some(id) = from {
        m.set_from(id);
    }
    m.set_msg_type(field_type);
    m
}

// Calculate the quorum of a Raft cluster with the specified total nodes.
pub fn quorum(total: usize) -> usize {
    total / 2 + 1
}

#[derive(Default)]
pub struct Raft<T: Storage> {
	pub id: u64,
    //当前leader id
	pub leader_id: u64,
    //当前角色
	pub state: StateRole,

	pub term: u64,
    pub vote: u64,

    //存储投票结果
    pub votes: HashMap<u64, bool>,
        //需要发送的消息
    pub msgs: Vec<Message>,

    // New configuration is ignored if there exists unapplied configuration.
    // 有未应用到状态机的配置变更，新的配置变更会被忽略
    pub pending_conf: bool,
    /// lead_transferee is id of the leader transfer target when its value is not None.
    /// Follow the procedure defined in raft thesis 3.10.
    /// 转移leader指令
    pub lead_transferee: Option<u64>,
    // Will be called when step** is about to be called.
    // return false will skip step**.
    //step函数中预先处理
    pub before_step_state: Option<Box<FnMut(&Message) -> bool>>,

    /// 存储log
    pub raft_log: RaftLog<T>,

    //leader使用，维护每一个Follower的进度信息
    //进度配置
    pub max_inflight: usize,
    pub max_msg_size: u64,
    pub prs: HashMap<u64, Progress>,

    //是否定期check
    pub check_quorum: bool,
    pre_vote: bool,

     /// number of ticks since it reached last electionTimeout when it is leader
    /// or candidate.
    /// number of ticks since it reached last electionTimeout or received a
    /// valid message from current leader when it is a follower.
    //上一次选举超时之后tick时间
    pub election_elapsed: usize,

    /// number of ticks since it reached last heartbeatTimeout.
    /// only leader keeps heartbeatElapsed.
    //leader用来定期给Follower发送心跳包，标识tick时间
    heartbeat_elapsed: usize,

    // [election_timeout, 2 * election_timeout - 1]. It gets reset
    // when raft changes its state to follower or candidate.
    randomized_election_timeout: usize,

    //心跳超时
    heartbeat_timeout: usize,
    //选举超时
    election_timeout: usize,
    /// tag is only used for logging
    tag: String,
}

impl<T: Storage> Raft<T> {
    pub fn new(c: &Config, store: T) -> Raft<T> {
        //验证配置文件
        c.validate().expect("configuration is invalid");
        //调用Storage初始化,得到RaftState，包括SoftState & HardState
        //pub struct RaftState {
        //    pub hard_state : HardState,
        //    pub conf_state : ConfState,
        //}
        let rs = store.initial_state().expect("");
        //创建RaftLog
        let raft_log = RaftLog::new(store, c.tag.clone());
        //得到当前group的peers
        let mut peers: &[u64] = &c.peers;

        // 调用Storage获取的存储的RaftState中的节点不为空的时候，此时配置文件中的节点必须为空
        // 优先用Storage中存储的
        if !rs.conf_state.get_nodes().is_empty() {
            if !peers.is_empty() {
                // TODO: the peers argument is always nil except in
                // tests; the argument should be removed and these tests should be
                // updated to specify their nodes through a snap
                panic!("{} cannot specify both new(peers) and ConfState.Nodes",
                       c.tag)
            }
            peers = rs.conf_state.get_nodes();
        }
        let mut r = Raft{
            id : c.id,

            leader_id: Default::default(),
            state: StateRole::Follower,

            vote: Default::default(),
            term: Default::default(),

            votes: Default::default(),
            msgs: Default::default(),

            pending_conf: Default::default(),
            lead_transferee: None,
            before_step_state: None,

            raft_log: raft_log,

            max_inflight: c.max_inflight_msgs,
            max_msg_size: c.max_size_per_msg,
            prs: HashMap::with_capacity(peers.len()),
            
            check_quorum: c.check_quorum,
            pre_vote: c.pre_vote,

            election_elapsed: Default::default(),
            heartbeat_elapsed: Default::default(),
            randomized_election_timeout: 0,
            heartbeat_timeout: c.heartbeat_tick,
            election_timeout: c.election_tick,
            tag: c.tag.to_owned(),
        };

        //为集群中每一个peer，先设置一个进度
        for p in peers {
            r.prs.insert(*p, new_progress(1, r.max_inflight));
        }

        //从Storage获取到的HardState不为空的话，需要load
        if rs.hard_state != HardState::new() {
            r.load_state(rs.hard_state);
        }
        if c.applied > 0 {
            r.raft_log.applied_to(c.applied);
        }

        //默认的term
        let term = r.term;
        r.become_follower(term, INVALID_ID);
        info!("{} newRaft [peers: {:?}, term: {:?}, commit: {}, applied: {}, last_index: {}, \
               last_term: {}]",
              r.tag,
              r.nodes(),
              r.term,
              r.raft_log.committed,
              r.raft_log.get_applied(),
              r.raft_log.last_index(),
              r.raft_log.last_term());
        r
    }


    #[inline]
    pub fn get_store(&self) -> &T {
        self.raft_log.get_store()
    }

    #[inline]
    pub fn mut_store(&mut self) -> &mut T {
        self.raft_log.mut_store()
    }

    #[inline]
    pub fn get_snap(&self) -> Option<&Snapshot> {
        self.raft_log.get_unstable().snapshot.as_ref()
    }

	pub fn soft_state(&self) -> SoftState {
        SoftState {
            leader_id: self.leader_id,
            raft_state: self.state,
        }
    }

    // TODO: revoke pub when there is a better way to test.
    pub fn load_state(&mut self, hs: HardState) {
        if hs.get_commit() < self.raft_log.committed ||
           hs.get_commit() > self.raft_log.last_index() {
            panic!("{} hs.commit {} is out of range [{}, {}]",
                   self.tag,
                   hs.get_commit(),
                   self.raft_log.committed,
                   self.raft_log.last_index())
        }
        self.raft_log.committed = hs.get_commit();
        self.term = hs.get_term();
        self.vote = hs.get_vote();
    }

    ////////////////////////////外部调用//////////////////
    pub fn add_node(&mut self, id: u64) {
        self.pending_conf = false;
        if self.prs.contains_key(&id) {
            // Ignore any redundant addNode calls (which can happen because the
            // initial bootstrapping entries are applied twice).
            return;
        }
        let last_index = self.raft_log.last_index();
        self.set_progress(id, 0, last_index + 1);
    }

    pub fn remove_node(&mut self, id: u64) {
        self.del_progress(id);
        self.pending_conf = false;

        // do not try to commit or abort transferring if there is no nodes in the cluster.
        if self.prs.is_empty() {
            return;
        }

        // The quorum size is now smaller, so see if any pending entries can
        // be committed.
        if self.maybe_commit() {
            self.bcast_append();
        }
        // If the removed node is the lead_transferee, then abort the leadership transferring.
        if self.state == StateRole::Leader && self.lead_transferee == Some(id) {
            self.abort_leader_transfer()
        }
    }

     pub fn reset_pending_conf(&mut self) {
        self.pending_conf = false;
    }

    pub fn set_progress(&mut self, id: u64, matched: u64, next_idx: u64) {
        let mut p = new_progress(next_idx, self.max_inflight);
        p.matched = matched;
        self.prs.insert(id, p);
    }

    fn del_progress(&mut self, id: u64) {
        self.prs.remove(&id);
    }

    pub fn hard_state(&self) -> HardState {
        let mut hs = HardState::new();
        hs.set_term(self.term);
        hs.set_vote(self.vote);
        hs.set_commit(self.raft_log.committed);
        hs
    }

    pub fn in_lease(&self) -> bool {
        self.state == StateRole::Leader && self.check_quorum
    }

    fn quorum(&self) -> usize {
        quorum(self.prs.len())
    }

    // for testing leader lease
    pub fn set_randomized_election_timeout(&mut self, t: usize) {
        self.randomized_election_timeout = t;
    }

    pub fn get_election_timeout(&self) -> usize {
        self.election_timeout
    }

    pub fn get_heartbeat_timeout(&self) -> usize {
        self.heartbeat_timeout
    }

    pub fn nodes(&self) -> Vec<u64> {
        let mut nodes = Vec::with_capacity(self.prs.len());
        nodes.extend(self.prs.keys());
        nodes.sort();
        nodes
    }

    //发送超时包
    pub fn send_timeout_now(&mut self, to: u64) {
        let msg = new_message(to, MessageType::MsgTimeoutNow, None);
        self.send(msg);
    }

    // send persists state to stable storage and then sends to its mailbox.
    fn send(&mut self, mut m: Message) {
        //设置消息源id
        m.set_from(self.id);
        //如果是投票的消息的话，term一定不为0，需要提前设置message的term，否则panic
        if m.get_msg_type() == MessageType::MsgRequestVote ||
            m.get_msg_type() == MessageType::MsgRequestPreVote {
                if m.get_term() == 0 {
                    // Pre-vote RPCs are sent at a term other than our actual term, so the code
                    // that sends these messages is responsible for setting the term.
                     panic!("{} term should be set when sending {:?}",
                       self.tag,
                       m.get_msg_type());
                }
        //不是投票的其他消息，不允许提前设置term，否则panic
        }else{
            if m.get_term() != 0 {
                panic!("{} term should not be set when sending {:?} (was {})",
                       self.tag,
                       m.get_msg_type(),
                       m.get_term());
            }
            // do not attach term to MsgPropose, MsgReadIndex
            // proposals are a way to forward to the leader and
            // should be treated as local message.
            // MsgReadIndex is also forwarded to leader.
            // MsgPropose & MsgReadIndex是直接转发给leader的，相当于local消息，不用设置term
            if m.get_msg_type() != MessageType::MsgPropose &&
               m.get_msg_type() != MessageType::MsgReadIndex {
                m.set_term(self.term);
            }
        }
        self.msgs.push(m);
    }

    pub fn abort_leader_transfer(&mut self) {
        self.lead_transferee = None;
    }

    //重新设置election_timeout
    pub fn reset_randomized_election_timeout(&mut self) {
        let prev_timeout = self.randomized_election_timeout;
        let timeout = self.election_timeout +
                      rand::thread_rng().gen_range(0, self.election_timeout);
        debug!("{} reset election timeout {} -> {} at {}",
               self.tag,
               prev_timeout,
               timeout,
               self.election_elapsed);
        self.randomized_election_timeout = timeout;
    }

    //TODO:read_only干什么用的？
    //reset的时候为每一个peer进度都默认设置为raftlog.last_index,
    //并且，如果刚好是当前id，需要把Match也设置为raftlog.last_index,
    pub fn reset(&mut self, term: u64) {
        if self.term != term {
            self.term = term;
            self.vote = INVALID_ID;
        }
        self.leader_id = INVALID_ID;
        self.reset_randomized_election_timeout();
        self.election_elapsed = 0;
        self.heartbeat_elapsed = 0;

        self.abort_leader_transfer();

        self.votes = HashMap::new();
        let (last_index, max_inflight) = (self.raft_log.last_index(), self.max_inflight);
        let self_id = self.id;
        for (id, p) in &mut self.prs {
            *p = new_progress(last_index + 1, max_inflight);
            if id == &self_id {
                p.matched = last_index;
            }
        }
        self.pending_conf = false;
        //self.read_only = ReadOnly::new(self.read_only.option);
    }

    //pending的配置变更的个数
    fn num_pending_conf(&self, ents: &[Entry]) -> usize {
        ents.into_iter().filter(|e| e.get_entry_type() == EntryType::EntryConfChange).count()
    }

    // maybe_commit attempts to advance the commit index. Returns true if
    // the commit index changed (in which case the caller should call
    // r.bcast_append).
    pub fn maybe_commit(&mut self) -> bool {
        // TODO: optimize
        let mut mis = Vec::with_capacity(self.prs.len());
        for p in self.prs.values() {
            mis.push(p.matched);
        }
        // reverse sort
        mis.sort_by(|a, b| b.cmp(a));
        let mci = mis[self.quorum() - 1];
        let term = self.term;
        self.raft_log.maybe_commit(mci, term)
    }

    //将每一个entry设置为当前term，index自增
    pub fn append_entry(&mut self, es: &mut [Entry]) {
        let li = self.raft_log.last_index();
        for (i, e) in es.iter_mut().enumerate() {
            e.set_term(self.term);
            e.set_index(li + 1 + i as u64);
        }
        self.raft_log.append(es);
        self.prs.get_mut(&self.id).unwrap().maybe_update(self.raft_log.last_index());
        // Regardless of maybe_commit's return, our caller will call bcastAppend.
        self.maybe_commit();
    }

    fn poll(&mut self, id: u64, t: MessageType, v: bool) -> usize {
        if v {
            info!("{} received {:?} from {} at term {}",
                  self.tag,
                  t,
                  id,
                  self.term)
        } else {
            info!("{} received {:?} rejection from {} at term {}",
                  self.tag,
                  t,
                  id,
                  self.term)
        }
        self.votes.entry(id).or_insert(v);
        self.votes.values().filter(|x| **x).count()
    }

    //发送投票消息
    //1.CAMPAIGN_PRE_ELECTION这种情况，先发送prevote消息，然后再自增，以及投票自己
    //2.CAMPAIGN_ELECTION，发送vote消息，先投票给自己
    fn campaign(&mut self, campaign_type: &[u8]) {
        let (vote_msg, term) = if campaign_type == CAMPAIGN_PRE_ELECTION {
            self.become_pre_candidate();
            // Pre-vote RPCs are sent for next term before we've incremented self.term.
            (MessageType::MsgRequestPreVote, self.term + 1)
        } else {
            self.become_candidate();
            (MessageType::MsgRequestVote, self.term)
        };
        let id = self.id;
        let t = match vote_msg {
            MessageType::MsgRequestVote => MessageType::MsgRequestVoteResponse,
            MessageType::MsgRequestPreVote => MessageType::MsgRequestPreVoteResponse,
            _ => panic!("Not a vote message: {:?}", vote_msg),
        };

        if self.quorum() == self.poll(id, t, true) {
            // We won the election after voting for ourselves (which must mean that
            // this is a single-node cluster). Advance to the next state.
            if campaign_type == CAMPAIGN_PRE_ELECTION {
                self.campaign(CAMPAIGN_ELECTION);
            } else {
                self.become_leader();
            }
            return;
        }
        let ids: Vec<_> = self.prs.keys().cloned().collect();
        for id in ids {
            if id == self.id {
                continue;
            }
            info!("{} [logterm: {}, index: {}] sent {:?} request to {} at term {}",
                  self.tag,
                  self.raft_log.last_term(),
                  self.raft_log.last_index(),
                  vote_msg,
                  id,
                  self.term);
            let mut m = new_message(id, vote_msg, None);
            m.set_term(term);
            m.set_index(self.raft_log.last_index());
            m.set_log_term(self.raft_log.last_term());
            if campaign_type == CAMPAIGN_TRANSFER {
                m.set_context(campaign_type.to_vec());
            }
            self.send(m);
        }
    }

    /////////////////////heartbeat相关消息/////////////////////////
    // send_heartbeat sends an empty MsgAppend
    //为其他peer发送HeatBeat消息，消息体中携带有commit信息
    //取matched和committed最小的一个
    fn send_heartbeat(&mut self, to: u64, ctx: Option<Vec<u8>>) {
        // Attach the commit as min(to.matched, self.raft_log.committed).
        // When the leader sends out heartbeat message,
        // the receiver(follower) might not be matched with the leader
        // or it might not have all the committed entries.
        // The leader MUST NOT forward the follower's commit to
        // an unmatched index.
        let mut m = Message::new();
        m.set_to(to);
        m.set_msg_type(MessageType::MsgHeartbeat);
        let commit = cmp::min(self.prs[&to].matched, self.raft_log.committed);
        m.set_commit(commit);
        if let Some(context) = ctx {
            m.set_context(context);
        }
        self.send(m);
    }
    // bcast_heartbeat sends RPC, without entries to all the peers.
    // TODO: read_only不知道干啥用的
    pub fn bcast_heartbeat(&mut self) {
        //let ctx = self.read_only.last_pending_request_ctx();
        let v: Vec<u8> = vec![];
        let ctx = Some(v); 
        self.bcast_heartbeat_with_ctx(ctx)
    }

    pub fn bcast_heartbeat_with_ctx(&mut self, ctx: Option<Vec<u8>>) {
        // TODO: avoid copy
        let ids: Vec<_> = self.prs.keys().cloned().collect();
        for id in ids {
            if id == self.id {
                continue;
            }
            self.send_heartbeat(id, ctx.clone());
        }
    }

    /////////////////////append相关消息/////////////////////////
    fn prepare_send_entries(&mut self, m: &mut Message, to: u64, term: u64, ents: Vec<Entry>) {
        let pr = self.prs.get_mut(&to).unwrap();
        m.set_msg_type(MessageType::MsgAppend);
        m.set_index(pr.next_idx - 1);
        m.set_log_term(term);
        m.set_entries(RepeatedField::from_vec(ents));
        m.set_commit(self.raft_log.committed);
        if !m.get_entries().is_empty() {
            match pr.state {
                ProgressState::Replicate => {
                    let last = m.get_entries().last().unwrap().get_index();
                    pr.optimistic_update(last);
                    pr.ins.add(last);
                }
                ProgressState::Probe => pr.pause(),
                _ => {
                    panic!("{} is sending append in unhandled state {:?}",
                           self.tag,
                           pr.state)
                }
            }
        }
    }

    fn prepare_send_snapshot(&mut self, m: &mut Message, to: u64) -> bool {
        info!("raft.rs prepare_send_snapshot");
        let pr = self.prs.get_mut(&to).unwrap();
        if !pr.recent_active {
            debug!("{} ignore sending snapshot to {} since it is not recently active",
                   self.tag,
                   to);
            return false;
        }

        m.set_msg_type(MessageType::MsgSnapshot);
        let snapshot_r = self.raft_log.snapshot();
        if let Err(e) = snapshot_r {
            if e == Error::Store(StorageError::SnapshotTemporarilyUnavailable) {
                debug!("{} failed to send snapshot to {} because snapshot is termporarily \
                        unavailable",
                       self.tag,
                       to);
                return false;
            }
            panic!("{} unexpected error: {:?}", self.tag, e);
        }
        let snapshot = snapshot_r.unwrap();
        if snapshot.get_metadata().get_index() == 0 {
            panic!("{} need non-empty snapshot", self.tag);
        }
        let (sindex, sterm) = (snapshot.get_metadata().get_index(),
                               snapshot.get_metadata().get_term());
        m.set_snapshot(snapshot);
        debug!("{} [firstindex: {}, commit: {}] sent snapshot[index: {}, term: {}] to {} \
                [{:?}]",
               self.tag,
               self.raft_log.first_index(),
               self.raft_log.committed,
               sindex,
               sterm,
               to,
               pr);
        pr.become_snapshot(sindex);
        debug!("{} paused sending replication messages to {} [{:?}]",
               self.tag,
               to,
               pr);
        true
    }
    
    // send_append sends RPC, with entries to the given peer.
    pub fn send_append(&mut self, to: u64) {
        let mut idx: u64 = 0;
         let (term, ents) = {
            let pr = &self.prs[&to];
            if pr.is_paused() {
                return;
            }
            idx = pr.next_idx;
            (self.raft_log.term(pr.next_idx - 1),
             self.raft_log.entries(pr.next_idx, self.max_msg_size))
        };
        let mut m = Message::new();
        m.set_to(to);
        if term.is_err() || ents.is_err() {
            //TODO: send snapshot
            // send snapshot if we failed to get term or entries
            if !self.prepare_send_snapshot(&mut m, to) {
                return;
            }
            //panic!("unexpected term or entries, raftlog_index:[{}]", idx);
        } else {
            self.prepare_send_entries(&mut m, to, term.unwrap(), ents.unwrap());
        }
        self.send(m);
    }

    // bcast_append sends RPC, with entries to all peers that are not up-to-date
    // according to the progress recorded in r.prs.
    pub fn bcast_append(&mut self) {
        // TODO: avoid copy
        let ids: Vec<_> = self.prs.keys().cloned().collect();
        for id in ids {
            if id == self.id {
                continue;
            }
            self.send_append(id);
        }
    }

    //////////////////////////////////tick相关驱动消息/////////////////
    //驱动函数，外部通过tick函数来驱动raft状态机转换
    pub fn tick(&mut self){
        match self.state {
            StateRole::Follower | StateRole::PreCandidate | StateRole::Candidate => {
                self.tick_election();
            }
            StateRole::Leader => {
                self.tick_heartbeat();
            }
        }
    }

    /// `pass_election_timeout` returns true iff `election_elapsed` is greater
    /// than or equal to the randomized election timeout in
    /// [`election_timeout`, 2 * `election_timeout` - 1].
    pub fn pass_election_timeout(&self) -> bool {
        self.election_elapsed >= self.randomized_election_timeout
    }

    //Follower、PreCandidate、Candidate每次执行的tick函数
    pub fn tick_election(&mut self){
        //自上次收到心跳包过了多少个tick了
        self.election_elapsed += 1;
        if self.prs.contains_key(&self.id) && self.election_elapsed >= self.randomized_election_timeout {
            self.election_elapsed = 0;
            let m = new_message(INVALID_ID, MessageType::MsgHup, Some(self.id));
            self.step(m).is_ok();
        }
    }

    //TODO:为什么要abort_leader_transfer？
    pub fn tick_heartbeat(&mut self){
        self.heartbeat_elapsed += 1;
        self.election_elapsed += 1;
        if self.election_elapsed >= self.election_timeout {
            self.election_elapsed = 0;
            //定期发送MsgCheckQuorum消息
            if self.check_quorum {
                let m = new_message(INVALID_ID, MessageType::MsgCheckQuorum, Some(self.id));
                self.step(m).is_ok();
            }
            //在leader状态下，需要做transfer leader,但是时间已经超过了election_timeout
            if self.state == StateRole::Leader && self.lead_transferee.is_some() {
                self.abort_leader_transfer()
            }
        }
        if self.state != StateRole::Leader {
            return;
        }

        //心跳时间到，需要发送MessageType::MsgBeat，调用step处理
        if self.heartbeat_elapsed >= self.heartbeat_timeout {
            self.heartbeat_elapsed = 0;
            let m = new_message(INVALID_ID, MessageType::MsgBeat, Some(self.id));
            self.step(m).is_ok();
        }
    }

    ////////////////////////状态转换/////////////////////////////
    pub fn become_follower(&mut self, term: u64, leader_id: u64) {
        self.reset(term);
        self.leader_id = leader_id;
        self.state = StateRole::Follower;
        info!("{} became follower at term {}", self.tag, self.term);
    }

    // TODO: revoke pub when there is a better way to test.
    // 不允许leader直接转换为candidate
    // 成为candidate的时候，需要将term自增1，reset一下，修改状态Candidate，将当前term的票投给自己
    pub fn become_candidate(&mut self) {
         assert!(self.state != StateRole::Leader,
                "invalid transition [leader -> candidate]");
        let term = self.term + 1;
        self.reset(term);
        let id = self.id;
        self.vote = id;
        self.state = StateRole::Candidate;
        info!("{} became candidate at term {}", self.tag, self.term);
    }

    //TODO:pre-candidate状态是干什么用的？什么情况下需要？
    pub fn become_pre_candidate(&mut self) {
        assert!(self.state != StateRole::Leader,
                "invalid transition [leader -> pre-candidate]");
        // Becoming a pre-candidate changes our state.
        // but doesn't change anything else. In particular it does not increase
        // self.term or change self.vote.
        self.state = StateRole::PreCandidate;
        info!("{} became pre-candidate at term {}", self.tag, self.term);
    }

    // TODO: revoke pub when there is a better way to test.
    // 不允许从follower直接转换成leader
    // 从log 的commit位置开始，找到配置变更的次数，超过1次就直接panic
    // 如果有一项配置变更的log，将pending_conf设置为true
    // append一个空的entry
    pub fn become_leader(&mut self) {
        assert!(self.state != StateRole::Follower,
                "invalid transition [follower -> leader]");
        let term = self.term;
        self.reset(term);
        self.leader_id = self.id;
        self.state = StateRole::Leader;
        let begin = self.raft_log.committed + 1;
        let ents = self.raft_log
            .entries(begin, raft_log::NO_LIMIT)
            .expect("unexpected error getting uncommitted entries");
        let nconf = self.num_pending_conf(&ents);
        if nconf > 1 {
            panic!("{} unexpected double uncommitted config entry", self.tag);
        }

        if nconf == 1 {
            self.pending_conf = true;
        }
        self.append_entry(&mut [Entry::new()]);
        info!("{} became leader at term {}", self.tag, self.term);
    }

    fn log_vote_approve(&self, m: &Message) {
        info!("{} [logterm: {}, index: {}, vote: {}] cast {:?} for {} [logterm: {}, index: {}] \
               at term {}",
              self.tag,
              self.raft_log.last_term(),
              self.raft_log.last_index(),
              self.vote,
              m.get_msg_type(),
              m.get_from(),
              m.get_log_term(),
              m.get_index(),
              self.term);
    }

    fn log_vote_reject(&self, m: &Message) {
        info!("{} [logterm: {}, index: {}, vote: {}] rejected {:?} from {} [logterm: {}, index: \
               {}] at term {}",
              self.tag,
              self.raft_log.last_term(),
              self.raft_log.last_index(),
              self.vote,
              m.get_msg_type(),
              m.get_from(),
              m.get_log_term(),
              m.get_index(),
              self.term);
    }

    //////////////////////////对外接口，收到消息//////////////////////
    pub fn step(&mut self, m: Message) -> Result<()> {
        // Handle the message term, which may result in our stepping down to a follower.
        if m.get_term() == 0 {
            // local message
        } else if m.get_term() > self.term {
            let leader_id = if m.get_msg_type() == MessageType::MsgRequestVote ||
                               m.get_msg_type() == MessageType::MsgRequestPreVote {
                let force = m.get_context() == CAMPAIGN_TRANSFER;
                let in_lease = self.check_quorum && self.leader_id != INVALID_ID &&
                               self.election_elapsed < self.election_timeout;
                if !force && in_lease {
                    // if a server receives ReqeustVote request within the minimum election
                    // timeout of hearing from a current leader, it does not update its term
                    // or grant its vote
                    info!("{} [logterm: {}, index: {}, vote: {}] ignored {:?} vote from \
                           {} [logterm: {}, index: {}] at term {}: lease is not expired \
                           (remaining ticks: {})",
                          self.tag,
                          self.raft_log.last_term(),
                          self.raft_log.last_index(),
                          self.vote,
                          m.get_msg_type(),
                          m.get_from(),
                          m.get_log_term(),
                          m.get_index(),
                          self.term,
                          self.election_timeout - self.election_elapsed);

                    return Ok(());
                }
                INVALID_ID
            } else {
                m.get_from()
            };

            if m.get_msg_type() == MessageType::MsgRequestPreVote ||
               (m.get_msg_type() == MessageType::MsgRequestPreVoteResponse && !m.get_reject()) {
                // For a pre-vote request:
                // Never change our term in response to a pre-vote request.
                //
                // For a pre-vote response with pre-vote granted:
                // We send pre-vote requests with a term in our future. If the
                // pre-vote is granted, we will increment our term when we get a
                // quorum. If it is not, the term comes from the node that
                // rejected our vote so we should become a follower at the new
                // term.
            } else {
                info!("{} [term: {}] received a {:?} message with higher term from {} [term: {}]",
                      self.tag,
                      self.term,
                      m.get_msg_type(),
                      m.get_from(),
                      m.get_term());
                self.become_follower(m.get_term(), leader_id);
            }
        } else if m.get_term() < self.term {
            if self.check_quorum &&
               (m.get_msg_type() == MessageType::MsgHeartbeat ||
                m.get_msg_type() == MessageType::MsgAppend) {
                // We have received messages from a leader at a lower term. It is possible
                // that these messages were simply delayed in the network, but this could
                // also mean that this node has advanced its term number during a network
                // partition, and it is now unable to either win an election or to rejoin
                // the majority on the old term. If checkQuorum is false, this will be
                // handled by incrementing term numbers in response to MsgVote with a higher
                // term, but if checkQuorum is true we may not advance the term on MsgVote and
                // must generate other messages to advance the term. The net result of these
                // two features is to minimize the disruption caused by nodes that have been
                // removed from the cluster's configuration: a removed node will send MsgVotes
                // which will be ignored, but it will not receive MsgApp or MsgHeartbeat, so it
                // will not create disruptive term increases
                let to_send = new_message(m.get_from(), MessageType::MsgAppendResponse, None);
                self.send(to_send);
            } else {
                // ignore other cases
                info!("{} [term: {}] ignored a {:?} message with lower term from {} [term: {}]",
                      self.tag,
                      self.term,
                      m.get_msg_type(),
                      m.get_from(),
                      m.get_term());
            }
            return Ok(());
        }

        if let Some(ref mut f) = self.before_step_state {
            if !f(&m) {
                // skip step**
                return Ok(());
            }
        }


        match m.get_msg_type() {
            MessageType::MsgHup => {
                if self.state != StateRole::Leader {
                    let ents = self.raft_log
                        .slice(self.raft_log.applied + 1,
                               self.raft_log.committed + 1,
                               raft_log::NO_LIMIT)
                        .expect("unexpected error getting unapplied entries");
                    let n = self.num_pending_conf(&ents);
                    if n != 0 && self.raft_log.committed > self.raft_log.applied {
                        warn!("{} cannot campaign at term {} since there are still {} pending \
                               configuration changes to apply",
                              self.tag,
                              self.term,
                              n);
                        return Ok(());
                    }
                    info!("{} is starting a new election at term {}",
                          self.tag,
                          self.term);
                    if self.pre_vote {
                        self.campaign(CAMPAIGN_PRE_ELECTION);
                    } else {
                        self.campaign(CAMPAIGN_ELECTION);
                    }
                } else {
                    debug!("{} ignoring MsgHup because already leader", self.tag);
                }
            }
            MessageType::MsgRequestVote |
            MessageType::MsgRequestPreVote => {
                // The m.get_term() > self.term clause is for MsgRequestPreVote. For MsgRequestVote
                // m.get_term() should always equal self.term
                if (self.vote == INVALID_ID || m.get_term() > self.term ||
                    self.vote == m.get_from()) &&
                   self.raft_log.is_up_to_date(m.get_index(), m.get_log_term()) {
                    self.log_vote_approve(&m);
                    let t = match m.get_msg_type() {
                        MessageType::MsgRequestVote => MessageType::MsgRequestVoteResponse,
                        MessageType::MsgRequestPreVote => MessageType::MsgRequestPreVoteResponse,
                        _ => panic!("Not a vote message: {:?}", m.get_msg_type()),
                    };

                    let mut to_send = new_message(m.get_from(), t, None);
                    to_send.set_reject(false);
                    self.send(to_send);
                    if m.get_msg_type() == MessageType::MsgRequestVote {
                        // Only record real votes.
                        self.election_elapsed = 0;
                        self.vote = m.get_from();
                    }
                } else {
                    self.log_vote_reject(&m);
                    let t = match m.get_msg_type() {
                        MessageType::MsgRequestVote => MessageType::MsgRequestVoteResponse,
                        MessageType::MsgRequestPreVote => MessageType::MsgRequestPreVoteResponse,
                        _ => panic!("Not a vote message: {:?}", m.get_msg_type()),
                    };
                    let mut to_send =
                        new_message(m.get_from(), t, None);
                    to_send.set_reject(true);
                    self.send(to_send);
                }
            }
            _ => {
                match self.state {
                    StateRole::PreCandidate | StateRole::Candidate => self.step_candidate(m),
                    StateRole::Follower => self.step_follower(m),
                    StateRole::Leader => self.step_leader(m),
                }
            }
        }

        Ok(())
    }

    // check_quorum_active returns true if the quorum is active from
    // the view of the local raft state machine. Otherwise, it returns
    // false.
    // check_quorum_active also resets all recent_active to false.
    fn check_quorum_active(&mut self) -> bool {
        let mut act = 0;
        let self_id = self.id;
        for (id, p) in &mut self.prs {
            if id == &self_id {
                // self is always active
                act += 1;
                continue;
            }

            if p.recent_active {
                act += 1;
            }

            p.recent_active = false;
        }
        act >= self.quorum()
    }

        fn handle_append_response(&mut self,
                              m: &Message,
                              old_paused: &mut bool,
                              send_append: &mut bool,
                              maybe_commit: &mut bool) {
        self.prs.get_mut(&m.get_from()).unwrap().recent_active = true;
        if m.get_reject() {
            let pr = self.prs.get_mut(&m.get_from()).unwrap();
            debug!("{} received msgAppend rejection(lastindex: {}) from {} for index {}",
                   self.tag,
                   m.get_reject_hint(),
                   m.get_from(),
                   m.get_index());
            if pr.maybe_decr_to(m.get_index(), m.get_reject_hint()) {
                debug!("{} decreased progress of {} to [{:?}]",
                       self.tag,
                       m.get_from(),
                       pr);
                if pr.state == ProgressState::Replicate {
                    pr.become_probe();
                }
                *send_append = true;
            }
            return;
        }

        {
            let pr = self.prs.get_mut(&m.get_from()).unwrap();
            *old_paused = pr.is_paused();
            if !pr.maybe_update(m.get_index()) {
                return;
            }
        }

        // Transfer leadership is in progress.
        if let Some(lead_transferee) = self.lead_transferee {
            if m.get_from() == lead_transferee &&
               self.prs.get_mut(&m.get_from()).unwrap().matched == self.raft_log.last_index() {
                info!("{} sent MsgTimeoutNow to {} after received MsgAppResp",
                      self.tag,
                      m.get_from());
                self.send_timeout_now(m.get_from());
            }
        }

        let pr = self.prs.get_mut(&m.get_from()).unwrap();
        match pr.state {
            ProgressState::Probe => pr.become_replicate(),
            ProgressState::Snapshot => {
                if !pr.maybe_snapshot_abort() {
                    return;
                }
                debug!("{} snapshot aborted, resumed sending replication messages to {} \
                        [{:?}]",
                       self.tag,
                       m.get_from(),
                       pr);
                pr.become_probe();
            }
            ProgressState::Replicate => pr.ins.free_to(m.get_index()),
        }
        *maybe_commit = true;
    }

    fn handle_transfer_leader(&mut self, m: &Message) {
        let lead_transferee = m.get_from();
        let last_lead_transferee = self.lead_transferee;
        if last_lead_transferee.is_some() {
            if last_lead_transferee.unwrap() == lead_transferee {
                info!("{} [term {}] transfer leadership to {} is in progress, ignores request \
                       to same node {}",
                      self.tag,
                      self.term,
                      lead_transferee,
                      lead_transferee);
                return;
            }
            self.abort_leader_transfer();
            info!("{} [term {}] abort previous transferring leadership to {}",
                  self.tag,
                  self.term,
                  last_lead_transferee.unwrap());
        }
        if lead_transferee == self.id {
            debug!("{} is already leader. Ignored transferring leadership to self",
                   self.tag);
            return;
        }
        // Transfer leadership to third party.
        info!("{} [term {}] starts to transfer leadership to {}",
              self.tag,
              self.term,
              lead_transferee);
        // Transfer leadership should be finished in one electionTimeout
        // so reset r.electionElapsed.
        self.election_elapsed = 0;
        self.lead_transferee = Some(lead_transferee);
        if self.prs[&m.get_from()].matched == self.raft_log.last_index() {
            self.send_timeout_now(lead_transferee);
            info!("{} sends MsgTimeoutNow to {} immediately as {} already has up-to-date log",
                  self.tag,
                  lead_transferee,
                  lead_transferee);
        } else {
            self.send_append(lead_transferee);
        }
    }

    /// check message's progress to decide which action should be taken.
    //  1:MsgAppendResponse
    //  2:MsgHeartbeatResponse
    //  3:MsgSnapStatus
    //  4:MsgUnreachable
    //  5:MsgTransferLeader
    fn check_message_with_progress(&mut self,
                                   m: &mut Message,
                                   send_append: &mut bool,
                                   old_paused: &mut bool,
                                   maybe_commit: &mut bool,
                                   more_to_send: &mut Option<Message>) {
        if !self.prs.contains_key(&m.get_from()) {
            debug!("{} no progress available for {}", self.tag, m.get_from());
            return;
        }
        match m.get_msg_type() {
            MessageType::MsgAppendResponse => {
                self.handle_append_response(m, old_paused, send_append, maybe_commit);
            }
            MessageType::MsgHeartbeatResponse => {
                {
                    let pr = self.prs.get_mut(&m.get_from()).unwrap();
                    pr.recent_active = true;
                    pr.resume();
                    // free one slot for the full inflights window to allow progress.
                    if pr.state == ProgressState::Replicate && pr.ins.full() {
                        pr.ins.free_first_one();
                    }
                    if pr.matched < self.raft_log.last_index() {
                        *send_append = true;
                    }
                }
            }
            MessageType::MsgSnapStatus => {
            }
            MessageType::MsgUnreachable => {
                let pr = self.prs.get_mut(&m.get_from()).unwrap();
                // During optimistic replication, if the remote becomes unreachable,
                // there is huge probability that a MsgAppend is lost.
                if pr.state == ProgressState::Replicate {
                    pr.become_probe();
                }
                debug!("{} failed to send message to {} because it is unreachable [{:?}]",
                       self.tag,
                       m.get_from(),
                       pr);
            }
            MessageType::MsgTransferLeader => {
                self.handle_transfer_leader(m);
            }
            _ => {}
        }
    }

    //Leader处理以下消息类型：
    //1. MsgBeat：广播心跳包
    //2. MsgCheckQuorum：和大多数Follower是否保持连接，如果网络隔离，超时的话，就直接变成follower状态
    //3. MsgPropose：
    //              提议的日志必须不为空，
    //              leader transfer的时候不提供写操作，
    //              已经有pending的EntryConfChange，忽略
    //              本地append_ntry,广播出去
    //4. MsgReadIndex:先不实现
    //5.其他消息在函数check_message_with_progress中处理
    //  A:MsgAppendResponse
    //  B:MsgHeartbeatResponse
    //  C:MsgSnapStatus
    //  D:MsgUnreachable
    //  E:MsgTransferLeader
    fn step_leader(&mut self, mut m: Message) {
        match m.get_msg_type() {
            MessageType::MsgBeat => {
                self.bcast_heartbeat();
                return;
            }
            MessageType::MsgCheckQuorum => {
                if !self.check_quorum_active() {
                    warn!("{} stepped down to follower since quorum is not active",
                          self.tag);
                    let term = self.term;
                    self.become_follower(term, INVALID_ID);
                }
                return;
            }
            MessageType::MsgPropose => {
                if m.get_entries().is_empty() {
                    panic!("{} stepped empty MsgProp", self.tag);
                }
                if !self.prs.contains_key(&self.id) {
                    // If we are not currently a member of the range (i.e. this node
                    // was removed from the configuration while serving as leader),
                    // drop any new proposals.
                    return;
                }
                if self.lead_transferee.is_some() {
                    debug!("{} [term {}] transfer leadership to {} is in progress; dropping \
                            proposal",
                           self.tag,
                           self.term,
                           self.lead_transferee.unwrap());
                    return;
                }

                for e in m.mut_entries().iter_mut() {
                    if e.get_entry_type() == EntryType::EntryConfChange {
                        if self.pending_conf {
                            info!("propose conf {:?} ignored since pending unapplied \
                                   configuration",
                                  e);
                            *e = Entry::new();
                            e.set_entry_type(EntryType::EntryNormal);
                        }
                        self.pending_conf = true;
                    }
                }
                self.append_entry(&mut m.mut_entries());
                self.bcast_append();
                return;
            }
            MessageType::MsgReadIndex => {
            }
            _ => {
            }
        }

        let mut send_append = false;
        let mut maybe_commit = false;
        let mut old_paused = false;
        let mut more_to_send = None;
        self.check_message_with_progress(&mut m,
                                         &mut send_append,
                                         &mut old_paused,
                                         &mut maybe_commit,
                                         &mut more_to_send);
        if maybe_commit {
            if self.maybe_commit() {
                self.bcast_append();
            } else if old_paused {
                // update() reset the wait state on this node. If we had delayed sending
                // an update before, send it now.
                send_append = true;
            }
        }

        if send_append {
            self.send_append(m.get_from());
        }
        if let Some(to_send) = more_to_send {
            self.send(to_send)
        }
    }

    // step_candidate is shared by state Candidate and PreCandidate; the difference is
    // whether they respond to MsgRequestVote or MsgRequestPreVote.
    //Candidate处理消息类型：
    //1.MsgPropose:没有leader，直接忽略这条propose消息
    //2.MsgAppend:变成follower，调用handle_append_entries处理
    //3.MsgHeartbeat：变成follower，调用handle_heartbeat处理
    //4.MsgSnapshot：先不实现
    //5.MsgRequestPreVoteResponse：
    //6.MsgRequestVoteResponse
    //7.MsgTimeoutNow
    fn step_candidate(&mut self, m: Message) {
        let term = self.term;
        match m.get_msg_type() {
            MessageType::MsgPropose => {
                info!("{} no leader at term {}; dropping proposal", self.tag, term);
                return;
            }
            MessageType::MsgAppend => {
                self.become_follower(term, m.get_from());
                self.handle_append_entries(m);
            }
            MessageType::MsgHeartbeat => {
                self.become_follower(term, m.get_from());
                self.handle_heartbeat(m);
            }
            MessageType::MsgSnapshot => {
                self.become_follower(term, m.get_from());
                self.handle_snapshot(m);
            }
            MessageType::MsgRequestPreVoteResponse |
            MessageType::MsgRequestVoteResponse => {
                // Only handle vote responses corresponding to our candidacy (while in
                // state Candidate, we may get stale MsgPreVoteResp messages in this term from
                // our pre-candidate state).
                if (self.state == StateRole::PreCandidate &&
                    m.get_msg_type() != MessageType::MsgRequestPreVoteResponse) ||
                   (self.state == StateRole::Candidate &&
                    m.get_msg_type() != MessageType::MsgRequestVoteResponse) {
                    return;
                }

                let gr = self.poll(m.get_from(), m.get_msg_type(), !m.get_reject());
                info!("{} [quorum:{}] has received {} {:?} votes and {} vote rejections",
                      self.tag,
                      self.quorum(),
                      gr,
                      m.get_msg_type(),
                      self.votes.len() - gr);
                if self.quorum() == gr {
                    if self.state == StateRole::PreCandidate {
                        self.campaign(CAMPAIGN_ELECTION);
                    } else {
                        self.become_leader();
                        self.bcast_append();
                    }
                } else if self.quorum() == self.votes.len() - gr {
                    self.become_follower(term, INVALID_ID);
                }
            }
            MessageType::MsgTimeoutNow => {
                debug!("{} [term {} state {:?}] ignored MsgTimeoutNow from {}",
                       self.tag,
                       self.term,
                       self.state,
                       m.get_from())
            }
            _ => {
            }
        }
    }

    //Follower处理消息分类：
    //1.MsgPropose:直接转发给leader去处理
    //2.MsgAppend：调用handle_append_entries，将Message直接append到raftlog上面，也可能有异常：消息中的index小于commit或者消息中index>log的last_index
    //3.MsgHeartbeat：调用handle_heartbeat处理，需要发送回复数据包，设置commited index
    //4.MsgSnapshot:先不实现
    //5.MsgTransferLeader：只有leader才能处理，直接转发给leader
    //6.MsgTimeoutNow：触发新一轮开始选举，CAMPAIGN_TRANSFER
    //7.MsgReadIndex：先不实现
    //8.MsgReadIndexResp：先不实现
    fn step_follower(&mut self, mut m: Message) {
        match m.get_msg_type(){
            MessageType::MsgPropose => {
                if self.leader_id == INVALID_ID {
                    info!("{} no leader at term {}; dropping proposal",
                          self.tag,
                          self.term);
                    return;
                }
                m.set_to(self.leader_id);
                self.send(m);
            }
            MessageType::MsgAppend => {
                self.election_elapsed = 0;
                self.leader_id = m.get_from();
                self.handle_append_entries(m);
            }
            MessageType::MsgHeartbeat => {
                self.election_elapsed = 0;
                self.leader_id = m.get_from();
                self.handle_heartbeat(m);
            }
            MessageType::MsgSnapshot => {
                self.election_elapsed = 0;
                self.leader_id = m.get_from();
                self.handle_snapshot(m);
            }
            MessageType::MsgTransferLeader => {
                if self.leader_id == INVALID_ID {
                    info!("{} no leader at term {}; dropping leader transfer msg",
                          self.tag,
                          self.term);
                    return;
                }
                m.set_to(self.leader_id);
                self.send(m);
            }
            MessageType::MsgTimeoutNow => {
                if self.prs.contains_key(&self.id){
                    info!("{} [term {}] received MsgTimeoutNow from {} and starts an election to \
                       get leadership.",
                          self.tag,
                          self.term,
                          m.get_from());
                    // Leadership trnasfers never use pre-vote even if self.pre_vote is true; we
                    // know we are not recovering from a partition so there is no need for the
                    // extra round trip.
                    self.campaign(CAMPAIGN_TRANSFER);
                } else {
                    info!("{} received MsgTimeoutNow from {} but is not promotable",
                          self.tag,
                          m.get_from());
                }
            }
            MessageType::MsgReadIndex => {

            }
            MessageType::MsgReadIndexResp => {

            }
            _ => {}
        }
    }

    // TODO: revoke pub when there is a better way to test.
    //收到append消息
    //1.Mesaage中的index比当前已经commit的index还小，忽略这条消息，同时给对方发送commited的index
    //2.符合要求调用raft_log.maybe_append，主要是index和raftlog的last_index,如果index太大就拒绝
    pub fn handle_append_entries(&mut self, m: Message) {
        if m.get_index() < self.raft_log.committed {
            let mut to_send = Message::new();
            to_send.set_to(m.get_from());
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.set_index(self.raft_log.committed);
            self.send(to_send);
            return;
        }
        let mut to_send = Message::new();
        to_send.set_to(m.get_from());
        to_send.set_msg_type(MessageType::MsgAppendResponse);
        match self.raft_log.maybe_append(m.get_index(),
                                         m.get_log_term(),
                                         m.get_commit(),
                                         m.get_entries()) {
            Some(mlast_index) => {
                to_send.set_index(mlast_index);
                self.send(to_send);
            }
            None => {
                debug!("{} [logterm: {}, index: {}] rejected msgApp [logterm: {}, index: {}] \
                        from {}",
                       self.tag,
                       self.raft_log.term(m.get_index()).unwrap_or(0),
                       m.get_index(),
                       m.get_log_term(),
                       m.get_index(),
                       m.get_from());
                to_send.set_index(m.get_index());
                to_send.set_reject(true);
                to_send.set_reject_hint(self.raft_log.last_index());
                self.send(to_send);
            }
        }
    }

    // TODO: revoke pub when there is a better way to test.
    // 处理心跳包，主要发送回复包，设置commited位置
    pub fn handle_heartbeat(&mut self, mut m: Message) {
        self.raft_log.commit_to(m.get_commit());
        let mut to_send = Message::new();
        to_send.set_to(m.get_from());
        to_send.set_msg_type(MessageType::MsgHeartbeatResponse);
        to_send.set_context(m.take_context());
        self.send(to_send);
    }


    fn restore_raft(&mut self, snap: &Snapshot) -> Option<bool> {
        let meta = snap.get_metadata();
        if self.raft_log.match_term(meta.get_index(), meta.get_term()) {
            info!("{} [commit: {}, lastindex: {}, lastterm: {}] fast-forwarded commit to \
                   snapshot [index: {}, term: {}]",
                  self.tag,
                  self.raft_log.committed,
                  self.raft_log.last_index(),
                  self.raft_log.last_term(),
                  meta.get_index(),
                  meta.get_term());
            self.raft_log.commit_to(meta.get_index());
            return Some(false);
        }

        info!("{} [commit: {}, lastindex: {}, lastterm: {}] starts to restore snapshot \
               [index: {}, term: {}]",
              self.tag,
              self.raft_log.committed,
              self.raft_log.last_index(),
              self.raft_log.last_term(),
              meta.get_index(),
              meta.get_term());
        self.prs = HashMap::with_capacity(meta.get_conf_state().get_nodes().len());
        for &n in meta.get_conf_state().get_nodes() {
            let next_idx = self.raft_log.last_index() + 1;
            let matched = if n == self.id { next_idx - 1 } else { 0 };
            self.set_progress(n, matched, next_idx);
            info!("{} restored progress of {} [{:?}]",
                  self.tag,
                  n,
                  self.prs[&n]);
        }
        None
    }

    // restore recovers the state machine from a snapshot. It restores the log and the
    // configuration of state machine.
    pub fn restore(&mut self, snap: Snapshot) -> bool {
        if snap.get_metadata().get_index() < self.raft_log.committed {
            return false;
        }
        if let Some(b) = self.restore_raft(&snap) {
            return b;
        }
        self.raft_log.restore(snap);
        true
    }


    fn handle_snapshot(&mut self, mut m: Message) {
        let (sindex, sterm) = (m.get_snapshot().get_metadata().get_index(),
                               m.get_snapshot().get_metadata().get_term());
        if self.restore(m.take_snapshot()) {
            info!("{} [commit: {}] restored snapshot [index: {}, term: {}]",
                  self.tag,
                  self.raft_log.committed,
                  sindex,
                  sterm);
            let mut to_send = Message::new();
            to_send.set_to(m.get_from());
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.set_index(self.raft_log.last_index());
            self.send(to_send);
        } else {
            info!("{} [commit: {}] ignored snapshot [index: {}, term: {}]",
                  self.tag,
                  self.raft_log.committed,
                  sindex,
                  sterm);
            let mut to_send = Message::new();
            to_send.set_to(m.get_from());
            to_send.set_msg_type(MessageType::MsgAppendResponse);
            to_send.set_index(self.raft_log.committed);
            self.send(to_send);
        }
    }
}
