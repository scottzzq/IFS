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


use raft::storage::Storage;
use raft::log_unstable::Unstable;
use std::{cmp, u64};
use raft::errors::{Result, Error, StorageError};
use kvproto::eraftpb::{Entry, Snapshot};
use util;

pub const NO_LIMIT: u64 = u64::MAX;
#[derive(Default)]
pub struct RaftLog<T: Storage> {
	//存储引擎，可以是内存的，也可以是磁盘
	pub store: T,

	pub unstable: Unstable,

	pub committed: u64,

	pub applied: u64,

	pub tag: String,
}

impl<T> ToString for RaftLog<T> where T : Storage {
	fn to_string(&self) -> String {
        format!("committed={}, applied={}, unstable.offset={}, unstable.entries.len()={}",
                self.committed,
                self.applied,
                self.unstable.offset,
                self.unstable.entries.len())
    }
}

impl<T: Storage> RaftLog<T> {
	//case1:
	//first_index = 1
	//last_index = 0
	//unstable_offset = 1

	//case2:
	//1,2,3,4
	//first_index = 1
	//last_index = 4
	//unstable_offset = 5
    pub fn new(storage: T, tag: String) -> RaftLog<T> {
    	//初始时为1
    	let first_index = storage.first_index().unwrap();
    	//初始是为0
        let last_index = storage.last_index().unwrap();

       	 // Initialize committed and applied pointers to the time of the last compaction.
        RaftLog {
            store: storage,
            committed: first_index - 1,
            applied: first_index - 1,
            unstable: Unstable::new(last_index + 1, tag.clone()),
            tag: tag,
        }
    }

    pub fn unstable_entries(&self) -> Option<&[Entry]> {
        if self.unstable.entries.is_empty() {
            return None;
        }
        Some(&self.unstable.entries)
    }
    
    //优先使用unstable的
    //当unstable没有的时候，选用storage中的
    //初始化为：1
    pub fn first_index(&self) -> u64 {
        match self.unstable.maybe_first_index() {
            Some(idx) => idx,
            None => self.store.first_index().unwrap(),
        }
    }

    //优先使用unstable的
    //当unstable没有的时候，选用storage中的
    //初始化为：0
    pub fn last_index(&self) -> u64 {
        match self.unstable.maybe_last_index() {
            Some(idx) => idx,
            None => self.store.last_index().unwrap(),
        }
    }

    //当前idx对应的term
    pub fn term(&self, idx: u64) -> Result<u64> {
        // the valid term range is [index of dummy entry, last index]
        let dummy_idx = self.first_index() - 1;
        if idx < dummy_idx || idx > self.last_index() {
            return Ok(0u64);
        }

        match self.unstable.maybe_term(idx) {
            Some(term) => Ok(term),
            _ => {
                self.store.term(idx).map_err(|e| {
                    match e {
                        Error::Store(StorageError::Compacted) |
                        Error::Store(StorageError::Unavailable) => {}
                        _ => panic!("{} unexpected error: {:?}", self.tag, e),
                    }
                    e
                })
            }
        }
    }

    pub fn last_term(&self) -> u64 {
        self.term(self.last_index())
            .expect(&format!("{} unexpected error when getting the last term", self.tag))
    }

    #[inline]
    pub fn get_store(&self) -> &T {
        &self.store
    }

    #[inline]
    pub fn mut_store(&mut self) -> &mut T {
        &mut self.store
    }

    //idx对应的term是否匹配
    pub fn match_term(&self, idx: u64, term: u64) -> bool {
        self.term(idx).map(|t| t == term).unwrap_or(false)
    }

    //尝试提交到
    pub fn maybe_commit(&mut self, max_index: u64, term: u64) -> bool {
        if max_index > self.committed && self.term(max_index).unwrap_or(0) == term {
            self.commit_to(max_index);
            true
        } else {
            false
        }
    }

    //提交到
    pub fn commit_to(&mut self, to_commit: u64) {
        // never decrease commit
        if self.committed >= to_commit {
            return;
        }
        if self.last_index() < to_commit {
            panic!("{} to_commit {} is out of range [last_index {}]",
                   self.tag,
                   to_commit,
                   self.last_index())
        }
        self.committed = to_commit;
    }

    //供raft调用，标识当前应用到状态机的index
    pub fn applied_to(&mut self, idx: u64) {
        if idx == 0 {
            return;
        }
        if self.committed < idx || idx < self.applied {
            panic!("{} applied({}) is out of range [prev_applied({}), committed({})",
                   self.tag,
                   idx,
                   self.applied,
                   self.committed)
        }
        self.applied = idx;
    }

    pub fn get_applied(&self) -> u64 {
        self.applied
    }

    pub fn stable_to(&mut self, idx: u64, term: u64) {
        self.unstable.stable_to(idx, term)
    }

    pub fn stable_snap_to(&mut self, idx: u64) {
        self.unstable.stable_snap_to(idx)
    }

    pub fn get_unstable(&self) -> &Unstable {
        &self.unstable
    }

	pub fn find_conflict(&self, ents: &[Entry]) -> u64 {
        for e in ents {
            if !self.match_term(e.get_index(), e.get_term()) {
                if e.get_index() <= self.last_index() {
                    info!("{} found conflict at index {}, [existing term:{}, conflicting term:{}]",
                          self.tag,
                          e.get_index(),
                          self.term(e.get_index()).unwrap_or(0),
                          e.get_term());
                }
                return e.get_index();
            }
        }
        0
    }

    //尝试append
    pub fn maybe_append(&mut self,
                        idx: u64,
                        term: u64,
                        committed: u64,
                        ents: &[Entry])
                        -> Option<u64> {
        let last_new_index = idx + ents.len() as u64;
        if self.match_term(idx, term) {
            let conflict_idx = self.find_conflict(ents);
            if conflict_idx == 0 {
            } else if conflict_idx <= self.committed {
                panic!("{} entry {} conflict with committed entry {}",
                       self.tag,
                       conflict_idx,
                       self.committed)
            } else {
                let offset = idx + 1;
                self.append(&ents[(conflict_idx - offset) as usize..]);
            }
            self.commit_to(cmp::min(committed, last_new_index));
            return Some(last_new_index);
        }
        None
    }

    //直接append
    pub fn append(&mut self, ents: &[Entry]) -> u64 {
        if ents.is_empty() {
            return self.last_index();
        }

        let after = ents[0].get_index() - 1;
        if after < self.committed {
            panic!("{} after {} is out of range [committed {}]",
                   self.tag,
                   after,
                   self.committed)
        }
        self.unstable.truncate_and_append(ents);
        self.last_index()
    }

    pub fn entries(&self, idx: u64, max_size: u64) -> Result<Vec<Entry>> {
        let last = self.last_index();
        if idx > last {
            return Ok(Vec::new());
        }
        self.slice(idx, last + 1, max_size)
    }

    pub fn all_entries(&self) -> Vec<Entry> {
        let first_index = self.first_index();
        match self.entries(first_index, NO_LIMIT) {
            Err(e) => {
                // try again if there was a racing compaction
                if e == Error::Store(StorageError::Compacted) {
                    return self.all_entries();
                }
                panic!("{} unexpected error: {:?}", self.tag, e);
            }
            Ok(ents) => ents,
        }
    }

    fn must_check_outofbounds(&self, low: u64, high: u64) -> Option<Error> {
        if low > high {
            panic!("{} invalid slice {} > {}", self.tag, low, high)
        }
        let first_index = self.first_index();
        if low < first_index {
            return Some(Error::Store(StorageError::Compacted));
        }

        let length = self.last_index() + 1 - first_index;
        if low < first_index || high > first_index + length {
            panic!("{} slice[{},{}] out of bound[{},{}]",
                   self.tag,
                   low,
                   high,
                   first_index,
                   self.last_index())
        }
        None
    }

    pub fn slice(&self, low: u64, high: u64, max_size: u64) -> Result<Vec<Entry>> {
        let err = self.must_check_outofbounds(low, high);
        if err.is_some() {
            return Err(err.unwrap());
        }

        let mut ents = vec![];
        if low == high {
            return Ok(ents);
        }

        if low < self.unstable.offset {
            let stored_entries = self.store
                .entries(low, cmp::min(high, self.unstable.offset), max_size);
            if stored_entries.is_err() {
                let e = stored_entries.unwrap_err();
                match e {
                    Error::Store(StorageError::Compacted) => return Err(e),
                    Error::Store(StorageError::Unavailable) => {
                        panic!("{} entries[{}:{}] is unavailable from storage",
                               self.tag,
                               low,
                               cmp::min(high, self.unstable.offset))
                    }
                    _ => panic!("{} unexpected error: {:?}", self.tag, e),
                }
            }
            ents = stored_entries.unwrap();
            if (ents.len() as u64) < cmp::min(high, self.unstable.offset) - low {
                return Ok(ents);
            }
        }

        if high > self.unstable.offset {
            let offset = self.unstable.offset;
            let unstable = self.unstable.slice(cmp::max(low, offset), high);
            ents.extend_from_slice(unstable);
        }
        util::limit_size(&mut ents, max_size);
        Ok(ents)
    }

    //从snapshot中还原
    pub fn restore(&mut self, snapshot: Snapshot) {
        info!("{} log [{}] starts to restore snapshot [index: {}, term: {}]",
              self.tag,
              self.to_string(),
              snapshot.get_metadata().get_index(),
              snapshot.get_metadata().get_term());
        self.committed = snapshot.get_metadata().get_index();
        self.unstable.restore(snapshot);
    }
    // is_up_to_date determines if the given (lastIndex,term) log is more up-to-date
    // by comparing the index and term of the last entry in the existing logs.
    // If the logs have last entry with different terms, then the log with the
    // later term is more up-to-date. If the logs end with the same term, then
    // whichever log has the larger last_index is more up-to-date. If the logs are
    // the same, the given log is up-to-date.
    pub fn is_up_to_date(&self, last_index: u64, term: u64) -> bool {
        term > self.last_term() || (term == self.last_term() && last_index >= self.last_index())
    }

    // next_entries returns all the available entries for execution.
    // If applied is smaller than the index of snapshot, it returns all committed
    // entries after the index of snapshot.
    pub fn next_entries(&self) -> Option<Vec<Entry>> {
        let offset = cmp::max(self.applied + 1, self.first_index());
        let committed = self.committed;
        if committed + 1 > offset {
            match self.slice(offset, committed + 1, NO_LIMIT) {
                Ok(vec) => return Some(vec),
                Err(e) => panic!("{} {}", self.tag, e),
            }
        }
        None
    }

    //用来判断当前是否有可以写入到Storage中的log
    pub fn has_next_entries(&self) -> bool {
        let offset = cmp::max(self.applied + 1, self.first_index());
        self.committed + 1 > offset
    }

     pub fn snapshot(&self) -> Result<Snapshot> {
        info!("raft_log.rs snapshot");
        self.unstable.snapshot.clone().map_or_else(|| self.store.snapshot(), Ok)
    }

}


