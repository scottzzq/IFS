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

pub mod logger;
pub mod file_log;
#[macro_use]
pub mod macros;
pub mod codec;
pub mod transport;
pub mod config;
pub mod rocksdb;
pub mod worker;
pub mod buf;
pub mod clocktime;
pub mod fs;

pub mod panic_hook;

use std::io;

use std::net::{ToSocketAddrs, TcpStream, SocketAddr};

use std::time::{Duration, Instant};

use std::{slice, thread};
use protobuf::Message;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
pub use self::fs::{DiskStat, get_disk_stat};

pub fn limit_size<T: Message + Clone>(entries: &mut Vec<T>, max: u64) {
    if entries.is_empty() {
        return;
    }

    let mut size = Message::compute_size(&entries[0]) as u64;
    let mut limit = 1usize;
    while limit < entries.len() {
        size += Message::compute_size(&entries[limit]) as u64;
        if size > max {
            break;
        }
        limit += 1;
    }
    entries.truncate(limit);
}

/// A handy shortcut to replace `RwLock` write/read().unwrap() pattern to
/// shortcut wl and rl.
pub trait HandyRwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<T>;
    fn rl(&self) -> RwLockReadGuard<T>;
}

impl<T> HandyRwLock<T> for RwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<T> {
        self.write().unwrap()
    }

    fn rl(&self) -> RwLockReadGuard<T> {
        self.read().unwrap()
    }
}

/// A function to escape a byte array to a readable ascii string.
/// escape rules follow golang/protobuf.
/// https://github.com/golang/protobuf/blob/master/proto/text.go#L578
///
/// # Examples
///
/// ```
/// use tikv::util::escape;
///
/// assert_eq!(r"ab", escape(b"ab"));
/// assert_eq!(r"a\\023", escape(b"a\\023"));
/// assert_eq!(r"a\000", escape(b"a\0"));
/// assert_eq!("a\\r\\n\\t '\\\"\\\\", escape(b"a\r\n\t '\"\\"));
/// assert_eq!(r"\342\235\244\360\237\220\267", escape("❤🐷".as_bytes()));
/// ```
pub fn escape(data: &[u8]) -> String {
    let mut escaped = Vec::with_capacity(data.len() * 4);
    for &c in data {
        match c {
            b'\n' => escaped.extend_from_slice(br"\n"),
            b'\r' => escaped.extend_from_slice(br"\r"),
            b'\t' => escaped.extend_from_slice(br"\t"),
            b'"' => escaped.extend_from_slice(b"\\\""),
            b'\\' => escaped.extend_from_slice(br"\\"),
            _ => {
                if c >= 0x20 && c < 0x7f {
                    // c is printable
                    escaped.push(c);
                } else {
                    escaped.push(b'\\');
                    escaped.push(b'0' + (c >> 6));
                    escaped.push(b'0' + ((c >> 3) & 7));
                    escaped.push(b'0' + (c & 7));
                }
            }
        }
    }
    escaped.shrink_to_fit();
    unsafe { String::from_utf8_unchecked(escaped) }
}

pub fn get_tag_from_thread_name() -> Option<String> {
    thread::current().name().and_then(|name| name.split("::").skip(1).last()).map(From::from)
}


// A helper function to parse SocketAddr for mio.
// In mio example, it uses "127.0.0.1:80".parse() to get the SocketAddr,
// but it is just ok for "ip:port", not "host:port".
pub fn to_socket_addr<A: ToSocketAddrs>(addr: A) -> io::Result<SocketAddr> {
    let addrs = try!(addr.to_socket_addrs());
    Ok(addrs.collect::<Vec<SocketAddr>>()[0])
}

pub struct SlowTimer {
    slow_time: Duration,
    t: Instant,
}

impl SlowTimer {
    pub fn new() -> SlowTimer {
        SlowTimer::default()
    }

    pub fn from(slow_time: Duration) -> SlowTimer {
        SlowTimer {
            slow_time: slow_time,
            t: Instant::now(),
        }
    }

    pub fn from_secs(secs: u64) -> SlowTimer {
        SlowTimer::from(Duration::from_secs(secs))
    }

    pub fn from_millis(millis: u64) -> SlowTimer {
        SlowTimer::from(Duration::from_millis(millis))
    }

    pub fn elapsed(&self) -> Duration {
        self.t.elapsed()
    }

    pub fn is_slow(&self) -> bool {
        self.elapsed() >= self.slow_time
    }
}

const DEFAULT_SLOW_SECS: u64 = 1;

impl Default for SlowTimer {
    fn default() -> SlowTimer {
        SlowTimer::from_secs(DEFAULT_SLOW_SECS)
    }
}

pub fn make_std_tcp_conn<A: ToSocketAddrs>(addr: A) -> io::Result<TcpStream> {
    let stream = try!(TcpStream::connect(addr));
    try!(stream.set_nodelay(true));
    Ok(stream)
}

/// `DeferContext` will invoke the wrapped closure when dropped.
pub struct DeferContext<T: FnOnce()> {
    t: Option<T>,
}

impl<T: FnOnce()> DeferContext<T> {
    pub fn new(t: T) -> DeferContext<T> {
        DeferContext { t: Some(t) }
    }
}

impl<T: FnOnce()> Drop for DeferContext<T> {
    fn drop(&mut self) {
        self.t.take().unwrap()()
    }
}
