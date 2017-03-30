use std::ffi::{CString, CStr};
use std::mem;
use libc;

pub struct DiskStat {
    pub capacity: u64,
    pub available: u64,
}

// Get the disk stats for path belongs.
// TODO: define own Error type instead of string.
pub fn get_disk_stat(path: &str) -> Result<DiskStat, String> {
    let cpath = CString::new(path).unwrap();
    unsafe {
        let mut stat: libc::statfs = mem::zeroed();
        let ret = libc::statfs(cpath.as_ptr(), &mut stat);
        if ret != 0 {
            return Err(format!("get stats for {} failed {}",
                               path,
                               CStr::from_ptr(libc::strerror(ret)).to_str().unwrap()));
        }

        Ok(DiskStat {
            capacity: (stat.f_bsize as u64 * stat.f_blocks) as u64,
            available: (stat.f_bsize as u64 * stat.f_bfree) as u64,
        })
    }
}