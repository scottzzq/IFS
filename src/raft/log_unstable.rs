//raftlog首先存储到unstable中，最后才持久化到storage中
use kvproto::eraftpb::{Entry, Snapshot};

//unstable.entris[i] has raft log position i+unstable.offset

#[derive(Debug, PartialEq, Default)]
pub struct Unstable {
	pub snapshot: Option<Snapshot>,
	pub entries: Vec<Entry>,
	pub offset: u64,
	pub tag: String,
}

impl Unstable {
	pub fn new(offset: u64, tag: String) -> Unstable{
		Unstable {
			offset : offset,
			snapshot: None,
			entries: vec![],
			tag: tag,
		}
	}

	//优先取Unstable中的snapshot的
	pub fn maybe_first_index(&self) ->Option<u64>{
		self.snapshot.as_ref().map(|snap| snap.get_metadata().get_index() + 1)
	}

	//优先取Unstable的entries中的
	//当entries为空的时候，判断snapshot的
    //offset:5
    //5,6,7
    //7
	pub fn maybe_last_index(&self) ->Option<u64> {
		match self.entries.len(){
			0 => {
				self.snapshot
                    .as_ref()
                    .map(|snap| snap.get_metadata().get_index())
			}
			len => Some(self.offset + len as u64 - 1),
		}
	}

	//得到log编号是idx对应的Entry的term
	//如果刚好是snapshot
	pub fn maybe_term(&self, idx: u64) -> Option<u64> {
		if idx < self.offset {
			if self.snapshot.is_none(){
				return None;
			}
			let meta = self.snapshot.as_ref().unwrap().get_metadata();
            if idx == meta.get_index() {
                return Some(meta.get_term());
            }
            return None;
		}
		self.maybe_last_index().and_then(|last| {
            if idx > last {
                return None;
            }
            Some(self.entries[(idx - self.offset) as usize].get_term())
        })
	}

	//offset:3
	//3,4,5,6: 5
	//start = 5 + 1 - 3 = 3

	//offset:6
	//已经持久化storage中，需要在unstable中删除
	pub fn stable_to(&mut self, idx: u64, term: u64){
		let t = self.maybe_term(idx);
		if t.is_none(){
			return;
		}

		if t.unwrap() == term && idx >= self.offset {
			let start = idx + 1 - self.offset;
            self.entries.drain(..start as usize);
            self.offset = idx + 1;
		}
	}

	//snapshot已经持久化到
	pub fn stable_snap_to(&mut self, idx: u64) {
        if self.snapshot.is_none() {
            return;
        }
        if idx == self.snapshot.as_ref().unwrap().get_metadata().get_index() {
            self.snapshot = None;
        }
    }

    //从snapshot中恢复
     pub fn restore(&mut self, snap: Snapshot) {
        self.entries.clear();
        self.offset = snap.get_metadata().get_index() + 1;
        self.snapshot = Some(snap);
    }


    //cate1: 3
    //3,4,5
    //6,7,8

    //case2: 3
    //3,4,5
    //1,2,3

    //case3: 3
    //3,4,5
    //4,5,6
    pub fn truncate_and_append(&mut self, ents: &[Entry]){
    	let after = ents[0].get_index();
    	
    	if after == self.offset + self.entries.len() as u64{
    		self.entries.extend_from_slice(ents);
    	}else if after <= self.offset {
    		self.offset = after;
            self.entries.clear();
            self.entries.extend_from_slice(ents);
    	}else{
    		let off = self.offset;
            self.must_check_outofbounds(off, after);
            self.entries.truncate((after - off) as usize);
            self.entries.extend_from_slice(ents);
    	}
    }

    //offset:2
    //2,3,4,5,6
    //3,4
    //3 - 2 = 1
    //4 - 2 = 2
    pub fn slice(&self, lo: u64, hi:u64) ->  &[Entry] {
    	self.must_check_outofbounds(lo, hi);
        let l = lo as usize;
        let h = hi as usize;
        let off = self.offset as usize;
        &self.entries[l - off..h - off]
    }

    //检查是否在合法范围之内
    pub fn must_check_outofbounds(&self, lo: u64, hi: u64) {
        if lo > hi {
            panic!("{} invalid unstable.slice {} > {}", self.tag, lo, hi)
        }
        let upper = self.offset + self.entries.len() as u64;
        if lo < self.offset || hi > upper {
            panic!("{} unstable.slice[{}, {}] out of bound[{}, {}]",
                   self.tag,
                   lo,
                   hi,
                   self.offset,
                   upper)
        }
    }
}












