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

use std::sync::Arc;
use std::fmt::{self, Formatter, Display};
use std::time::Instant;

use uuid::Uuid;

use kvproto::metapb;
use kvproto::eraftpb::ConfChangeType;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, AdminRequest, AdminCmdType};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::pdpb;

use util::worker::Runnable;
use util::escape;
use util::transport::SendCh;
use pd::PdClient;
use raftstore::store::Msg;
use raftstore::Result;
use raftstore::store::util::is_epoch_stale;


// Use an asynchronous thread to tell pd something.
pub enum Task {
    Heartbeat {
        region: metapb::Region,
        peer: metapb::Peer,
        down_peers: Vec<pdpb::PeerStats>,
    },
    StoreHeartbeat { stats: pdpb::StoreStats },
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Heartbeat { ref region, ref peer, .. } => {
                write!(f,
                       "heartbeat for region {:?}, leader {}",
                       region,
                       peer.get_id())
            }
            Task::StoreHeartbeat { ref stats } => write!(f, "store heartbeat stats: {:?}", stats),
        }
    }
}

pub struct Runner<T: PdClient> {
    pd_client: Arc<T>,
    ch: SendCh<Msg>,
}

impl<T: PdClient> Runner<T> {
    pub fn new(pd_client: Arc<T>, ch: SendCh<Msg>) -> Runner<T> {
        Runner {
            pd_client: pd_client,
            ch: ch,
        }
    }

    fn send_admin_request(&self,
                          mut region: metapb::Region,
                          peer: metapb::Peer,
                          request: AdminRequest) {
        let region_id = region.get_id();
        let cmd_type = request.get_cmd_type();
        let mut req = RaftCmdRequest::new();
        req.mut_header().set_region_id(region_id);
        req.mut_header().set_region_epoch(region.take_region_epoch());
        req.mut_header().set_peer(peer);
        req.mut_header().set_uuid(Uuid::new_v4().as_bytes().to_vec());
        req.set_admin_request(request);
        let cb = Box::new(move |_: RaftCmdResponse| -> Result<()> { Ok(()) });
        if let Err(e) = self.ch.try_send(Msg::RaftCmd {
            request: req,
            callback: cb,
            send_time: Instant::now(),
        }) {
            error!("[region {}] send {:?} request err {:?}",
                   region_id,
                   cmd_type,
                   e);
        }
    }
    fn handle_heartbeat(&self,
                        region: metapb::Region,
                        peer: metapb::Peer,
                        down_peers: Vec<pdpb::PeerStats>) {
        // Now we use put region protocol for heartbeat.
        match self.pd_client.region_heartbeat(region.clone(), peer.clone(), down_peers) {
            Ok(mut resp) => {
                if resp.has_change_peer() {
                    let mut change_peer = resp.take_change_peer();
                    info!("[region {}] try to change peer {:?} {:?} for region {:?}",
                          region.get_id(),
                          change_peer.get_change_type(),
                          change_peer.get_peer(),
                          region);
                    let req = new_change_peer_request(change_peer.get_change_type(),
                                                      change_peer.take_peer());
                    self.send_admin_request(region, peer, req);
                } else if resp.has_transfer_leader() {
                    let mut transfer_leader = resp.take_transfer_leader();
                    info!("[region {}] try to transfer leader from {:?} to {:?}",
                          region.get_id(),
                          peer,
                          transfer_leader.get_peer());
                    let req = new_transfer_leader_request(transfer_leader.take_peer());
                    self.send_admin_request(region, peer, req)
                }
            }
            Err(e) => {
                debug!("[region {}] failed to send heartbeat: {:?}",
                       region.get_id(),
                       e)
            }
        }
    }

    fn handle_store_heartbeat(&self, stats: pdpb::StoreStats) {
        if let Err(e) = self.pd_client.store_heartbeat(stats) {
            error!("store heartbeat failed {:?}", e);
        }
    }
    // send a raft message to destroy the specified stale peer
    fn send_destroy_peer_message(&self,
                                 local_region: metapb::Region,
                                 peer: metapb::Peer,
                                 pd_region: metapb::Region) {
        let mut message = RaftMessage::new();
        message.set_region_id(local_region.get_id());
        message.set_from_peer(peer.clone());
        message.set_to_peer(peer.clone());
        message.set_region_epoch(pd_region.get_region_epoch().clone());
        message.set_is_tombstone(true);
        if let Err(e) = self.ch.try_send(Msg::RaftMessage(message)) {
            error!("send gc peer request to region {} err {:?}",
                   local_region.get_id(),
                   e)
        }
    }
}

impl<T: PdClient> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        debug!("executing task {}", task);
        match task {
            Task::Heartbeat { region, peer, down_peers } => {
                self.handle_heartbeat(region, peer, down_peers)
            }
            Task::StoreHeartbeat { stats } => self.handle_store_heartbeat(stats),
        };
    }
}

fn new_change_peer_request(change_type: ConfChangeType, peer: metapb::Peer) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::ChangePeer);
    req.mut_change_peer().set_change_type(change_type);
    req.mut_change_peer().set_peer(peer);
    req
}

fn new_split_region_request(split_key: Vec<u8>,
                            new_region_id: u64,
                            peer_ids: Vec<u64>)
                            -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::Split);
    req.mut_split().set_split_key(split_key);
    req.mut_split().set_new_region_id(new_region_id);
    req.mut_split().set_new_peer_ids(peer_ids);
    req
}

fn new_transfer_leader_request(peer: metapb::Peer) -> AdminRequest {
    let mut req = AdminRequest::new();
    req.set_cmd_type(AdminCmdType::TransferLeader);
    req.mut_transfer_leader().set_peer(peer);
    req
}