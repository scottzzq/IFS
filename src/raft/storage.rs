use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use raft::errors::{Result, Error, StorageError};
use kvproto::eraftpb::{HardState, ConfState, Entry, Snapshot};
use util::{self,HandyRwLock};

//状态
// message HardState {
//     optional uint64 term   = 1;
//     optional uint64 vote   = 2;
//     optional uint64 commit = 3;
// }
// message ConfState {
//     repeated uint64 nodes = 1;
// }

#[derive(Debug, Clone)]
pub struct RaftState {
	pub hard_state : HardState,
	pub conf_state : ConfState,
}

// enum EntryType {
//     EntryNormal     = 0;
//     EntryConfChange = 1;
// }

//每一条raftlog的数据格式
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

//Snapshot数据格式
// message Snapshot {
//     optional bytes            data     = 1;
//     optional SnapshotMetadata metadata = 2;
// }

pub trait Storage {
	//初始化操作
	fn initial_state(&self) ->Result<RaftState>;
	//得到区间[low, high)之间的log
	fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<Vec<Entry>>;
	//得到idx 对应的term
	fn term(&self, idx: u64) -> Result<u64>;
	//当前log第一个index
	fn first_index(&self) ->Result<u64>;
	//当前raftgroup最后一个index
	fn last_index(&self) ->Result<u64>;
	//得到当前raft group snapshot
	fn snapshot(&self) -> Result<Snapshot>;
}

pub struct MemStorageCore {
	hard_state: HardState,
	snapshot: Snapshot,
	//entries[i]代表i+snapshot.get_metadata().get_index()对应位置的log
	//entries[0]不能用，初始状态防一个空的Entry到entries[0]
	//apply snapshot的时候，防置apply_index apply_term：apply_index之前所有的数据
	entries: Vec<Entry>,
}

impl Default for MemStorageCore {
    fn default() -> MemStorageCore {
        MemStorageCore {
            // When starting from scratch populate the list with a dummy entry at term zero.
            entries: vec![Entry::new()],
            hard_state: HardState::new(),
            snapshot: Snapshot::new(),
        }
    }
}

impl MemStorageCore{
	pub fn set_hardstate(&mut self, hs: HardState){
		self.hard_state = hs
	}

	//0,1,2,3,4: --->4
	//5,6,7,8,9: --->9
	fn inner_last_index(&self) -> u64 {
		self.entries[0].get_index() + self.entries.len() as u64 - 1
	}

	//将snapshot应用到状态机
	pub fn apply_snapshot(&mut self, snapshot: Snapshot) -> Result<()> {
		let index = self.snapshot.get_metadata().get_index();
        let snapshot_index = snapshot.get_metadata().get_index();
        if index >= snapshot_index {
            return Err(Error::Store(StorageError::SnapshotOutOfDate));
        }

        let mut e = Entry::new();
        e.set_term(snapshot.get_metadata().get_term());
        e.set_index(snapshot.get_metadata().get_index());
        self.entries = vec![e];
        self.snapshot = snapshot;
        Ok(())
    }

    //0,1,2,|3|,4: 3
    //idx:3
    //entries[3 - 0] = entries[3]

    //5,6,7,8,9: 8
    //idx:8
    //entries[8 - 5]=entries【3】
    pub fn create_snapshot(&mut self, idx: u64, cs: Option<ConfState>,
    		data: Vec<u8> ) -> Result<&Snapshot> {
		//判断index和当前snapshot的index比较
		if idx <= self.snapshot.get_metadata().get_index() {
            return Err(Error::Store(StorageError::SnapshotOutOfDate));
        }
        if idx > self.inner_last_index(){
        	panic!("snapshot {} is out of bound lastindex({})",
                   idx,
                   self.inner_last_index())
        }
        let offset = self.entries[0].get_index();
        self.snapshot.mut_metadata().set_index(idx);
        self.snapshot.mut_metadata().set_term(self.entries[(idx - offset) as usize].get_term());
        if let Some(cs) = cs {
            self.snapshot.mut_metadata().set_conf_state(cs)
        }
        self.snapshot.set_data(data);
        Ok(&self.snapshot)
    }


    //0,1,2,|3|,4: 3
    //compact_index:3
    //offset = 0
    //entrys:3,4

    //5,6,7,8,9: 8
    //compact_index:8
    //offset = 5
    ///entrys:8,9
    pub fn compact(&mut self, compact_index: u64) -> Result<()> {
    	let offset = self.entries[0].get_index();
        if compact_index <= offset {
            return Err(Error::Store(StorageError::Compacted));
        }
        if compact_index > self.inner_last_index() {
            panic!("compact {} is out of bound lastindex({})",
                   compact_index,
                   self.inner_last_index())
        }
        let i = (compact_index - offset) as usize;
        let entries = self.entries.drain(i..).collect();
        self.entries = entries;
        Ok(())
    }

    //0,1,2,3
    //1. 4,5,6
    //	first = 1
    //  last = 6
    //  first = 1, entry[0] = 4
    //2. 2,3,4
    pub fn append(&mut self, ents: &[Entry]) -> Result<()> {
        if ents.is_empty() {
            return Ok(());
        }
        let first = self.entries[0].get_index() + 1;
        let last = ents[0].get_index() + ents.len() as u64 - 1;

        if last < first {
            return Ok(());
        }
        // truncate compacted entries
        let te: &[Entry] = if first > ents[0].get_index() {
            let start = (first - ents[0].get_index()) as usize;
            &ents[start..ents.len()]
        } else {
            ents
        };


        let offset = te[0].get_index() - self.entries[0].get_index();
        if self.entries.len() as u64 > offset {
            let mut new_entries: Vec<Entry> = vec![];
            new_entries.extend_from_slice(&self.entries[..offset as usize]);
            new_entries.extend_from_slice(te);
            self.entries = new_entries;
        } else if self.entries.len() as u64 == offset {
            self.entries.extend_from_slice(te);
        } else {
            panic!("missing log entry [last: {}, append at: {}]",
                   self.inner_last_index(),
                   te[0].get_index())
        }

        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct MemStorage{
	core: Arc<RwLock<MemStorageCore>>,
}

impl MemStorage {
    pub fn new() -> MemStorage {
        MemStorage { ..Default::default() }
    }

    pub fn rl(&self) -> RwLockReadGuard<MemStorageCore> {
        self.core.rl()
    }

    pub fn wl(&self) -> RwLockWriteGuard<MemStorageCore> {
        self.core.wl()
    }
}

impl Storage for MemStorage {
    /// initial_state implements the Storage trait.
    fn initial_state(&self) -> Result<RaftState> {
        let core = self.rl();
        Ok(RaftState {
            hard_state: core.hard_state.clone(),
            conf_state: core.snapshot.get_metadata().get_conf_state().clone(),
        })
    }

    /// entries implements the Storage trait.
    //得到区间[low, high)之间的log
    fn entries(&self, low: u64, high: u64, max_size: u64) -> Result<Vec<Entry>> {
        let core = self.rl();
        let offset = core.entries[0].get_index();
        if low <= offset {
            return Err(Error::Store(StorageError::Compacted));
        }

        if high > core.inner_last_index() + 1 {
            panic!("index out of bound")
        }
        // only contains dummy entries.
        if core.entries.len() == 1 {
            return Err(Error::Store(StorageError::Unavailable));
        }

        let lo = (low - offset) as usize;
        let hi = (high - offset) as usize;
        let mut ents = core.entries[lo..hi].to_vec();
        util::limit_size(&mut ents, max_size);
        Ok(ents)
    }

    /// term implements the Storage trait.
    //得到idx 对应的term
    fn term(&self, idx: u64) -> Result<u64> {
        let core = self.rl();
        let offset = core.entries[0].get_index();
        if idx < offset {
            return Err(Error::Store(StorageError::Compacted));
        }
        if idx - offset >= core.entries.len() as u64 {
            return Err(Error::Store(StorageError::Unavailable));
        }
        Ok(core.entries[(idx - offset) as usize].get_term())
    }

    /// first_index implements the Storage trait.
    //当前log第一个index
    //case1: 0,1,2
    // first:1

    //case2: 2,3
    // first:3
    fn first_index(&self) -> Result<u64> {
        let core = self.rl();
        Ok(core.entries[0].get_index() + 1)
    }

    /// last_index implements the Storage trait.
    //当前raftgroup最后一个index
     //case1: 0,1,2
    // last:2

    //case2: 2,3
    // last:3
    fn last_index(&self) -> Result<u64> {
        let core = self.rl();
        Ok(core.inner_last_index())
    }

    /// snapshot implements the Storage trait.
    //得到当前raft group snapshot
    fn snapshot(&self) -> Result<Snapshot> {
        let core = self.rl();
        Ok(core.snapshot.clone())
    }
}
