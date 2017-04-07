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

use std::time::Instant;
use std::boxed::{Box, FnBox};
use std::fmt;

use raftstore::Result;

use kvproto::raft_serverpb::RaftMessage;
use kvproto::raft_cmdpb::{RaftCmdRequest, RaftCmdResponse};
use kvproto::volume_cmdpb::{Request as VolumeCmdRequest, Response as VolumeCmdResponse};
use kvproto::metapb::RegionEpoch;
use kvproto::eraftpb::Snapshot;

use util::escape;

pub type Callback = Box<FnBox(RaftCmdResponse) -> Result<()> + Send>;

pub type VolumeCallback = Box<FnBox(VolumeCmdResponse) -> Result<()> + Send>;

#[derive(Debug)]
pub enum Tick {
    Raft,
    RaftLogGc,
    SplitRegionCheck,
    CompactCheck,
    PdHeartbeat,
    PdStoreHeartbeat,
    SnapGc,
    CompactLockCf,
    ConsistencyCheck,
    ReportRegionFlow,
}

pub enum Msg {
    Quit,

    // For notify.
    RaftMessage(RaftMessage),

    RaftCmd {
        send_time: Instant,
        request: RaftCmdRequest,
        callback: Callback,
    },

    VolumeCmd {
        send_time: Instant,
        request: VolumeCmdRequest,
        callback: VolumeCallback,
    },

    // // For split check
    // SplitCheckResult {
    //     region_id: u64,
    //     epoch: RegionEpoch,
    //     split_key: Vec<u8>,
    // },

    ReportUnreachable { region_id: u64, to_peer_id: u64 },

    // For snapshot stats.
    SnapshotStats,
    SnapApplyRes {
        region_id: u64,
        is_success: bool,
        is_aborted: bool,
    },
    SnapGenRes {
        region_id: u64,
        snap: Option<Snapshot>,
    },
    // // For consistency check
    // ComputeHashResult {
    //     region_id: u64,
    //     index: u64,
    //     hash: Vec<u8>,
    // },
}

impl fmt::Debug for Msg {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Msg::Quit => write!(fmt, "Quit"),
            Msg::RaftMessage(_) => write!(fmt, "Raft Message"),
            Msg::RaftCmd { .. } => write!(fmt, "Raft Command"),
            Msg::VolumeCmd{..}  => write!(fmt, "Volume Command"),
            // Msg::SplitCheckResult { .. } => write!(fmt, "Split Check Result"),
            Msg::ReportUnreachable { ref region_id, ref to_peer_id } => {
                write!(fmt,
                       "peer {} for region {} is unreachable",
                       to_peer_id,
                       region_id)
            }
            Msg::SnapshotStats => write!(fmt, "Snapshot stats"),
            Msg::SnapApplyRes { region_id, is_success, is_aborted } => {
                write!(fmt,
                       "SnapApplyRes [region_id: {}, is_success: {}, is_aborted: {}]",
                       region_id,
                       is_success,
                       is_aborted)
            }
            Msg::SnapGenRes { region_id, ref snap } => {
                write!(fmt,
                       "SnapGenRes [region_id: {}, is_success: {}]",
                       region_id,
                       snap.is_some())
            }
        }
    }
}

impl Msg {
    pub fn new_raft_cmd(request: RaftCmdRequest, callback: Callback) -> Msg {
        Msg::RaftCmd {
            send_time: Instant::now(),
            request: request,
            callback: callback,
        }
    }
     pub fn new_volume_cmd(request: VolumeCmdRequest, callback: VolumeCallback) -> Msg {
        Msg::VolumeCmd {
            send_time: Instant::now(),
            request: request,
            callback: callback,
        }
    }
}
