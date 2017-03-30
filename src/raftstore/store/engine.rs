use std::option::Option;
use std::ops::Deref;
use std::sync::Arc;
use std::fmt::{self, Debug, Formatter};

use rocksdb::{DB, Writable, DBIterator, DBVector, WriteBatch, ReadOptions, CFHandle};
use rocksdb::rocksdb_options::UnsafeSnap;
use protobuf;
use byteorder::{ByteOrder, BigEndian};
use util::rocksdb;

use raftstore::Result;
use raftstore::Error;

pub struct Snapshot {
    db: Arc<DB>,
    snap: UnsafeSnap,
}

/// Because snap will be valid whenever db is valid, so it's safe to send
/// it around.
unsafe impl Send for Snapshot {}
unsafe impl Sync for Snapshot {}

#[derive(Debug)]
pub struct SyncSnapshot(Arc<Snapshot>);

impl Deref for SyncSnapshot {
    type Target = Snapshot;

    fn deref(&self) -> &Snapshot {
        &self.0
    }
}

impl SyncSnapshot {
    pub fn new(db: Arc<DB>) -> SyncSnapshot {
        SyncSnapshot(Arc::new(Snapshot::new(db)))
    }

    pub fn clone(&self) -> SyncSnapshot {
        SyncSnapshot(self.0.clone())
    }
}


impl Snapshot {
    pub fn new(db: Arc<DB>) -> Snapshot {
        unsafe {
            Snapshot {
                snap: db.unsafe_snap(),
                db: db,
            }
        }
    }

    pub fn into_sync(self) -> SyncSnapshot {
        SyncSnapshot(Arc::new(self))
    }

    pub fn cf_names(&self) -> Vec<&str> {
        self.db.cf_names()
    }

    pub fn cf_handle(&self, cf: &str) -> Result<&CFHandle> {
        rocksdb::get_cf_handle(&self.db, cf).map_err(Error::from)
    }
}

impl Debug for Snapshot {
    fn fmt(&self, fmt: &mut Formatter) -> fmt::Result {
        write!(fmt, "Engine Snapshot Impl")
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        unsafe {
            self.db.release_snap(&self.snap);
        }
    }
}


//////////////////////////////////key value 查询////////////////////////////////////
// TODO: refactor this trait into rocksdb trait.
pub trait Peekable {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBVector>>;
    fn get_value_cf(&self, cf: &str, key: &[u8]) -> Result<Option<DBVector>>;

    fn get_msg<M>(&self, key: &[u8]) -> Result<Option<M>>
        where M: protobuf::Message + protobuf::MessageStatic
    {
        let value = try!(self.get_value(key));

        if value.is_none() {
            return Ok(None);
        }

        let mut m = M::new();
        try!(m.merge_from_bytes(&value.unwrap()));
        Ok(Some(m))
    }

    fn get_msg_cf<M>(&self, cf: &str, key: &[u8]) -> Result<Option<M>>
        where M: protobuf::Message + protobuf::MessageStatic
    {
        let value = try!(self.get_value_cf(cf, key));

        if value.is_none() {
            return Ok(None);
        }

        let mut m = M::new();
        try!(m.merge_from_bytes(&value.unwrap()));
        Ok(Some(m))
    }

    fn get_u64(&self, key: &[u8]) -> Result<Option<u64>> {
        let value = try!(self.get_value(key));

        if value.is_none() {
            return Ok(None);
        }

        let value = value.unwrap();
        if value.len() != 8 {
            return Err(box_err!("need 8 bytes, but only got {}", value.len()));
        }

        let n = BigEndian::read_u64(&value);
        Ok(Some(n))
    }

    fn get_i64(&self, key: &[u8]) -> Result<Option<i64>> {
        let r = try!(self.get_u64(key));
        match r {
            None => Ok(None),
            Some(n) => Ok(Some(n as i64)),
        }
    }
}

impl Peekable for DB {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBVector>> {
        let v = try!(self.get(key));
        Ok(v)
    }

    fn get_value_cf(&self, cf: &str, key: &[u8]) -> Result<Option<DBVector>> {
        let handle = try!(rocksdb::get_cf_handle(self, cf));
        let v = try!(self.get_cf(handle, key));
        Ok(v)
    }
}

////////////////////////////////////迭代访问///////////////////////////////

#[derive(Clone)]
pub enum SeekMode {
    TotalOrderSeek,
    PrefixSeek,
}
#[derive(Clone)]
pub struct IterOption {
    pub upper_bound: Option<Vec<u8>>,
    pub fill_cache: bool,
    pub seek_mode: SeekMode,
}

impl IterOption {
    pub fn new(upper_bound: Option<Vec<u8>>, fill_cache: bool, seek_mode: SeekMode) -> IterOption {
        IterOption {
            upper_bound: upper_bound,
            fill_cache: fill_cache,
            seek_mode: seek_mode,
        }
    }

    pub fn total_order_seek_used(&self) -> bool {
        match self.seek_mode {
            SeekMode::TotalOrderSeek => true,
            _ => false,
        }
    }
}

impl Default for IterOption {
    fn default() -> IterOption {
        IterOption {
            upper_bound: None,
            fill_cache: true,
            seek_mode: SeekMode::TotalOrderSeek,
        }
    }
}

// TODO: refactor this trait into rocksdb trait.
pub trait Iterable {
    fn new_iterator(&self, Option<&[u8]>) -> DBIterator;
    fn new_iterator_cf(&self, &str, Option<&[u8]>) -> Result<DBIterator>;
    
    // scan scans database using an iterator in range [start_key, end_key), calls function f for
    // each iteration, if f returns false, terminates this scan.
    fn scan<F>(&self, start_key: &[u8], end_key: &[u8], f: &mut F) -> Result<()>
        where F: FnMut(&[u8], &[u8]) -> Result<bool>
    {
        scan_impl(self.new_iterator(Some(end_key)), start_key, f)
    }

    // like `scan`, only on a specific column family.
    fn scan_cf<F>(&self, cf: &str, start_key: &[u8], end_key: &[u8], f: &mut F) -> Result<()>
        where F: FnMut(&[u8], &[u8]) -> Result<bool>
    {
        scan_impl(try!(self.new_iterator_cf(cf, Some(end_key))), start_key, f)
    }

    // Seek the first key >= given key, if no found, return None.
    fn seek(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = self.new_iterator(None);
        iter.seek(key.into());
        Ok(iter.kv())
    }

    // Seek the first key >= given key, if no found, return None.
    fn seek_cf(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        let mut iter = try!(self.new_iterator_cf(cf, None));
        iter.seek(key.into());
        Ok(iter.kv())
    }
}

impl Iterable for DB {
        fn new_iterator(&self, upper_bound: Option<&[u8]>) -> DBIterator {
        match upper_bound {
            Some(key) => {
                let mut readopts = ReadOptions::new();
                readopts.set_iterate_upper_bound(key);
                self.iter_opt(readopts)
            }
            None => self.iter(),
        }
    }

    fn new_iterator_cf(&self, cf: &str, upper_bound: Option<&[u8]>) -> Result<DBIterator> {
        let handle = try!(rocksdb::get_cf_handle(self, cf));
        match upper_bound {
            Some(key) => {
                let mut readopts = ReadOptions::new();
                readopts.set_iterate_upper_bound(key);
                Ok(DBIterator::new_cf(self, handle, readopts))
            }
            None => Ok(self.iter_cf(handle)),
        }
    }
}

impl Iterable for Snapshot {
        fn new_iterator(&self, upper_bound: Option<&[u8]>) -> DBIterator {
        let mut opt = ReadOptions::new();
        if let Some(key) = upper_bound {
            opt.set_iterate_upper_bound(key);
        }
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        DBIterator::new(&self.db, opt)
    }

    fn new_iterator_cf(&self, cf: &str, upper_bound: Option<&[u8]>) -> Result<DBIterator> {
        let handle = try!(rocksdb::get_cf_handle(&self.db, cf));
        let mut opt = ReadOptions::new();
        if let Some(key) = upper_bound {
            opt.set_iterate_upper_bound(key);
        }
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        Ok(DBIterator::new_cf(&self.db, handle, opt))
    }
}


fn scan_impl<F>(mut it: DBIterator, start_key: &[u8], f: &mut F) -> Result<()>
    where F: FnMut(&[u8], &[u8]) -> Result<bool>
{
    it.seek(start_key.into());
    while it.valid() {
        let r = try!(f(it.key(), it.value()));

        if !r || !it.next() {
            break;
        }
    }

    Ok(())
}

/////////////////////////////修改///////////////////////////////////////////
pub trait Mutable: Writable {
    fn put_msg<M: protobuf::Message>(&self, key: &[u8], m: &M) -> Result<()> {
        let value = try!(m.write_to_bytes());
        try!(self.put(key, &value));
        Ok(())
    }

    fn put_msg_cf<M: protobuf::Message>(&self, cf: &CFHandle, key: &[u8], m: &M) -> Result<()> {
        let value = try!(m.write_to_bytes());
        try!(self.put_cf(cf, key, &value));
        Ok(())
    }

    fn put_u64(&self, key: &[u8], n: u64) -> Result<()> {
        let mut value = vec![0;8];
        BigEndian::write_u64(&mut value, n);
        try!(self.put(key, &value));
        Ok(())
    }

    fn put_i64(&self, key: &[u8], n: i64) -> Result<()> {
        self.put_u64(key, n as u64)
    }

    fn del(&self, key: &[u8]) -> Result<()> {
        try!(self.delete(key));
        Ok(())
    }
}

impl Mutable for DB {}
impl Mutable for WriteBatch {}

impl Peekable for Snapshot {
    fn get_value(&self, key: &[u8]) -> Result<Option<DBVector>> {
        let mut opt = ReadOptions::new();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let v = try!(self.db.get_opt(key, &opt));
        Ok(v)
    }

    fn get_value_cf(&self, cf: &str, key: &[u8]) -> Result<Option<DBVector>> {
        let handle = try!(rocksdb::get_cf_handle(&self.db, cf));
        let mut opt = ReadOptions::new();
        unsafe {
            opt.set_snapshot(&self.snap);
        }
        let v = try!(self.db.get_cf_opt(handle, key, &opt));
        Ok(v)
    }
}
