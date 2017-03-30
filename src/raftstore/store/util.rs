
use std::option::Option;

use uuid::Uuid;

use kvproto::metapb;
use kvproto::eraftpb::{self, ConfChangeType};
use kvproto::raft_cmdpb::RaftCmdRequest;
use raftstore::{Result, Error};


// check whether epoch is staler than check_epoch.
pub fn is_epoch_stale(epoch: &metapb::RegionEpoch, check_epoch: &metapb::RegionEpoch) -> bool {
    epoch.get_version() < check_epoch.get_version() ||
    epoch.get_conf_ver() < check_epoch.get_conf_ver()
}
pub fn find_peer(region: &metapb::Region, store_id: u64) -> Option<&metapb::Peer> {
    for peer in region.get_peers() {
        if peer.get_store_id() == store_id {
            return Some(peer);
        }
    }

    None
}

pub fn remove_peer(region: &mut metapb::Region, store_id: u64) -> Option<metapb::Peer> {
    region.get_peers()
        .iter()
        .position(|x| x.get_store_id() == store_id)
        .map(|i| region.mut_peers().remove(i))
}


pub fn get_uuid_from_req(cmd: &RaftCmdRequest) -> Option<Uuid> {
    Uuid::from_bytes(cmd.get_header().get_uuid()).ok()
}

const STR_CONF_CHANGE_ADD_NODE: &'static str = "AddNode";
const STR_CONF_CHANGE_REMOVE_NODE: &'static str = "RemoveNode";

pub fn conf_change_type_str(conf_type: &eraftpb::ConfChangeType) -> &'static str {
    match *conf_type {
        ConfChangeType::AddNode => STR_CONF_CHANGE_ADD_NODE,
        ConfChangeType::RemoveNode => STR_CONF_CHANGE_REMOVE_NODE,
    }
}


