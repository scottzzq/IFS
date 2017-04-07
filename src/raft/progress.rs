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

use std::cmp;

#[derive(Debug, Default, Clone)]
pub struct Inflights {
    // the starting index in the buffer
    start: usize,
    // number of inflights in the buffer
    count: usize,
    // ring buffer
    buffer: Vec<u64>,
}

impl Inflights {
	//初始新建一个Inflights，分配容量
    pub fn new(cap: usize) -> Inflights {
        Inflights { buffer: Vec::with_capacity(cap), ..Default::default() }
    }
    // full returns true if the inflights is full.
    pub fn full(&self) -> bool {
        self.count == self.cap()
    }
    pub fn cap(&self) -> usize {
        self.buffer.capacity()
    }
    // add adds an inflight into inflights
    pub fn add(&mut self, inflight: u64) {
        if self.full() {
            panic!("cannot add into a full inflights")
        }

        let mut next = self.start + self.count;
        if next >= self.cap() {
            next -= self.cap();
        }
        assert!(next <= self.buffer.len());
        if next == self.buffer.len() {
            self.buffer.push(inflight);
        } else {
            self.buffer[next] = inflight;
        }
        self.count += 1;
    }

    // free_to frees the inflights smaller or equal to the given `to` flight.
    pub fn free_to(&mut self, to: u64) {
        if self.count == 0 || to < self.buffer[self.start] {
            // out of the left side of the window
            return;
        }

        let mut i = 0usize;
        let mut idx = self.start;
        while i < self.count {
            if to < self.buffer[idx] {
                // found the first large inflight
                break;
            }
            // increase index and maybe rotate
            idx += 1;
            if idx >= self.cap() {
                idx -= self.cap();
            }
            i += 1;
        }
        // free i inflights and set new start index
        self.count -= i;
        self.start = idx;
    }

    pub fn free_first_one(&mut self) {
        let start = self.buffer[self.start];
        self.free_to(start);
    }
    // resets frees all inflights.
    pub fn reset(&mut self) {
        self.count = 0;
        self.start = 0;
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum ProgressState{
	//初始状态
	Probe,
	//复制状态
	Replicate,
	//发送Snapshot状态
	Snapshot,
}

impl Default for ProgressState{
	fn default() -> ProgressState {
        ProgressState::Probe
    }
}

#[derive(Debug, Default, Clone)]
pub struct Progress{
	//Follower已知最大的idx
	pub matched: u64,
	//log的下一个idx
	pub next_idx : u64,

	//状态
	pub state: ProgressState,
	//停止状态
	pub paused: bool,

	//挂起的snapshot的idx
	pub pending_snapshot: u64,

	//最近是否活跃
	pub recent_active: bool,

	//处于Replicate状态，可以不用等Follower恢复就可以继续发送
	pub ins: Inflights,
}

impl Progress {
	pub fn reset_state(&mut self, state: ProgressState){
		self.paused = false;
        self.pending_snapshot = 0;
        self.state = state;
        self.ins.reset();
	}

	//状态变为Probe
	//1.当前处于Snapshot，
	//2.当前处于其他状态
	pub fn become_probe(&mut self) {
        // If the original state is ProgressStateSnapshot, progress knows that
        // the pending snapshot has been sent to this peer successfully, then
        // probes from pendingSnapshot + 1.
        if self.state == ProgressState::Snapshot {
            let pending_snapshot = self.pending_snapshot;
            self.reset_state(ProgressState::Probe);
            self.next_idx = cmp::max(self.matched + 1, pending_snapshot + 1);
        } else {
            self.reset_state(ProgressState::Probe);
            self.next_idx = self.matched + 1;
        }
    }

    //改变状态为Replicate
    pub fn become_replicate(&mut self) {
        self.reset_state(ProgressState::Replicate);
        self.next_idx = self.matched + 1;
    }

    //改变状态为Snapshot
    pub fn become_snapshot(&mut self, snapshot_idx: u64) {
        self.reset_state(ProgressState::Snapshot);
        self.pending_snapshot = snapshot_idx;
    }

    pub fn snapshot_failure(&mut self) {
        self.pending_snapshot = 0;
    }

    // maybe_snapshot_abort unsets pendingSnapshot if Match is equal or higher than
    // the pendingSnapshot
    pub fn maybe_snapshot_abort(&self) -> bool {
        self.state == ProgressState::Snapshot && self.matched >= self.pending_snapshot
    }

    pub fn resume(&mut self) {
        self.paused = false;
    }

    //尝试更新进度
    pub fn maybe_update(&mut self, n: u64) -> bool{
    	//当前id的进度是否比n小
    	let need_update = self.matched < n;
    	if need_update {
    		//更新进度到n
            self.matched = n;
            self.resume();
        }

        if self.next_idx < n + 1 {
            self.next_idx = n + 1
        }
        need_update
    }

    // maybe_decr_to returns false if the given to index comes from an out of order message.
    // Otherwise it decreases the progress next index to min(rejected, last) and returns true.
    //对于进度拉下太大的Follower，刚开始next发送过去，是不能匹配到的，所以需要
    //返回false说明乱序的消息
    //1、处于Replicate状态，直接将next_idx设置为matched + 1
    //2、处于其他状态next_idx设置为min(rejected, last + 1)
    pub fn maybe_decr_to(&mut self, rejected: u64, last: u64) -> bool {
        if self.state == ProgressState::Replicate {
            // the rejection must be stale if the progress has matched and "rejected"
            // is smaller than "match".
            if rejected <= self.matched {
                return false;
            }
            self.next_idx = self.matched + 1;
            return true;
        }

        // the rejection must be stale if "rejected" does not match next - 1
        if self.next_idx == 0 || self.next_idx - 1 != rejected {
            return false;
        }

        self.next_idx = cmp::min(rejected, last + 1);
        if self.next_idx < 1 {
            self.next_idx = 1;
        }
        self.resume();
        true
    }

    pub fn optimistic_update(&mut self, n: u64) {
        self.next_idx = n + 1;
    }

    //1、当处于Probe状态，判断paused状态
    //2、当处于Replicate状态，环形缓冲区已经满了
    //3、处于Snapshot状态
    pub fn is_paused(&self) -> bool {
        match self.state {
            ProgressState::Probe => self.paused,
            ProgressState::Replicate => self.ins.full(),
            ProgressState::Snapshot => true,
        }
    }

    pub fn pause(&mut self) {
        self.paused = true;
    }
}


