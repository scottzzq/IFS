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

use std::sync::Arc;
use std::boxed::{Box, FnBox};
use std::net::SocketAddr;
use std::fmt::{self, Formatter, Display};
use std::collections::HashMap;
use std::time::Instant;

use super::Result;
use util;
use util::worker::{Runnable, Worker};
use kvproto::metapb;
use kvproto::pdpb;
use pd::{PdClient, Result as PdResult};
use std::str::FromStr;


const STORE_ADDRESS_REFRESH_SECONDS: u64 = 60;
pub type Callback = Box<FnBox(Result<SocketAddr>) + Send>;

// StoreAddrResolver resolves the store address.
pub trait StoreAddrResolver {
    // Resolve resolves the store address asynchronously.
    fn resolve(&self, store_id: u64, cb: Callback) -> Result<()>;
}


/// Resolve Host Task
struct Task {
    store_id: u64,
    cb: Callback,
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "resolve store {} address", self.store_id)
    }
}
struct StoreAddr {
    sock: SocketAddr,
    last_update: Instant,
}

pub struct Runner<T: PdClient> {
    pd_client: Arc<T>,
    store_addrs: HashMap<u64, StoreAddr>,
}

impl<T: PdClient> Runner<T> {
    fn resolve(&mut self, store_id: u64) -> Result<SocketAddr> {
        if let Some(s) = self.store_addrs.get(&store_id) {
            let now = Instant::now();
            let elasped = now.duration_since(s.last_update);
            if elasped.as_secs() < STORE_ADDRESS_REFRESH_SECONDS {
                return Ok(s.sock);
            }
        }

        let addr = try!(self.get_address(store_id));
        let sock = try!(util::to_socket_addr(addr.as_str()));

        let cache = StoreAddr {
            sock: sock,
            last_update: Instant::now(),
        };
        self.store_addrs.insert(store_id, cache);

        Ok(sock)
    }

    fn get_address(&mut self, store_id: u64) -> Result<String> {
        let pd_client = self.pd_client.clone();
        let s = box_try!(pd_client.get_store(store_id));
        if s.get_state() == metapb::StoreState::Tombstone {
            //RESOLVE_STORE_COUNTER.with_label_values(&["tombstone"]).inc();
            return Err(box_err!("store {} has been removed", store_id));
        }
        let addr = s.get_address().to_owned();
        // In some tests, we use empty address for store first,
        // so we should ignore here.
        // TODO: we may remove this check after we refactor the test.
        if addr.is_empty() {
            return Err(box_err!("invalid empty address for store {}", store_id));
        }
        Ok(addr)
    }
}

impl<T: PdClient> Runnable<Task> for Runner<T> {
    fn run(&mut self, task: Task) {
        let store_id = task.store_id;
        let resp = self.resolve(store_id);
        task.cb.call_box((resp,))
    }
}

pub struct PdStoreAddrResolver {
    worker: Worker<Task>,
}

impl PdStoreAddrResolver {
    pub fn new<T>(pd_client: Arc<T>) -> Result<PdStoreAddrResolver>
        where T: PdClient + 'static
    {
        let mut r = PdStoreAddrResolver { worker: Worker::new("store address resolve worker") };
        let runner = Runner {
            pd_client: pd_client,
            store_addrs: HashMap::new(),
        };
        box_try!(r.worker.start(runner));
        Ok(r)
    }
}


impl StoreAddrResolver for PdStoreAddrResolver {
    fn resolve(&self, store_id: u64, cb: Callback) -> Result<()> {
        let task = Task {
            store_id: store_id,
            cb: cb,
        };
        box_try!(self.worker.schedule(task));
        Ok(())
    }
}

impl Drop for PdStoreAddrResolver {
    fn drop(&mut self) {
        if let Some(Err(e)) = self.worker.stop().map(|h| h.join()) {
            error!("failed to stop store address resolve thread: {:?}!!!", e);
        }
    }
}

// message Store {
//     optional uint64 id          = 1 [(gogoproto.nullable) = false];
//     optional string address     = 2 [(gogoproto.nullable) = false];
//     optional StoreState state   = 3 [(gogoproto.nullable) = false];
//     repeated StoreLabel labels  = 4;
//     // more attributes......
// }
// pub struct MockPdClient {
//         pub start: Instant,
//         pub store: metapb::Store,
// }

// impl PdClient for MockPdClient {
//         fn bootstrap_cluster(&self, _: metapb::Store, _: metapb::Region) -> PdResult<()> {
//             unimplemented!();
//         }
//         fn is_cluster_bootstrapped(&self) -> PdResult<bool> {
//             unimplemented!();
//         }
//         fn alloc_id(&self) -> PdResult<u64> {
//             unimplemented!();
//         }
//         fn put_store(&self, _: metapb::Store) -> PdResult<()> {
//             unimplemented!();
//         }
//         fn get_store(&self, store_id: u64) -> PdResult<metapb::Store> {
//             // // The store address will be changed every millisecond.
//             // let mut store = self.store.clone();
//             // let mut sock = SocketAddr::from_str(store.get_address()).unwrap();
//             // // sock.set_port(util::duration_to_ms(self.start.elapsed()) as u16);
//             // sock.set_port(12 as u16);
//             // store.set_address(format!("{}:{}", sock.ip(), sock.port()));
//             //Ok(store)

//             let mut store = metapb::Store::new();
//             match store_id {
//                 1 => {
//                     store.set_id(1);
//                     store.set_state(metapb::StoreState::Up);
//                     store.set_address("127.0.0.1:20161".into());
//                 },
//                 2 => {
//                     store.set_id(2);
//                     store.set_state(metapb::StoreState::Up);
//                     store.set_address("127.0.0.1:20162".into());
//                 },
//                 3 =>{
//                     store.set_id(3);
//                     store.set_state(metapb::StoreState::Up);
//                     store.set_address("127.0.0.1:20163".into());
//                 }
//                 _ => { }
//             }
//             Ok(store)
//         }
//         fn get_cluster_config(&self) -> PdResult<metapb::Cluster> {
//             unimplemented!();
//         }
//         fn get_region(&self, _: &[u8]) -> PdResult<metapb::Region> {
//             unimplemented!();
//         }
//         fn get_region_by_id(&self, _: u64) -> PdResult<Option<metapb::Region>> {
//             unimplemented!();
//         }
//         fn region_heartbeat(&self,
//                         region: metapb::Region,
//                         leader: metapb::Peer,
//                         down_peers: Vec<pdpb::PeerStats>)
//                         -> Result<pdpb::RegionHeartbeatResponse>{

//            unimplemented!();
//            Ok(pdpb::RegionHeartbeatResponse::new())
//         }
//         fn ask_split(&self, _: metapb::Region) -> PdResult<pdpb::AskSplitResponse> {
//             unimplemented!();
//         }
//         fn store_heartbeat(&self, _: pdpb::StoreStats) -> PdResult<()> {
//             unimplemented!();
//         }
//         fn report_split(&self, _: metapb::Region, _: metapb::Region) -> PdResult<()> {
//             unimplemented!();
//         }
// }
