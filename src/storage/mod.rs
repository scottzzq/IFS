pub type CfName = &'static str;
pub const CF_DEFAULT: CfName = "default";
pub const CF_LOCK: CfName = "lock";
pub const CF_WRITE: CfName = "write";
pub const CF_RAFT: CfName = "raft";
// Cfs that should be very large generally.
pub const LARGE_CFS: &'static [CfName] = &[CF_DEFAULT, CF_WRITE];
pub const ALL_CFS: &'static [CfName] = &[CF_DEFAULT, CF_RAFT];

// only used for rocksdb without persistent.
pub const TEMP_DIR: &'static str = "";

pub mod config;
pub use self::config::Config;