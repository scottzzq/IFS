use raftstore::store::{Msg as StoreMsg, Transport, Callback};
use raftstore::{Result as RaftStoreResult, Error as RaftStoreError};
use kvproto::raft_serverpb::RaftMessage;
use kvproto::raft_cmdpb::RaftCmdRequest;
use util::transport::SendCh;
use super::{Msg, ConnData};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use kvproto::msgpb::{Message, MessageType};

pub trait RaftStoreRouter: Send + Clone {
    /// Send StoreMsg, retry if failed. Try times may vary from implementation.
    fn send(&self, msg: StoreMsg) -> RaftStoreResult<()>;

    /// Send StoreMsg.
    fn try_send(&self, msg: StoreMsg) -> RaftStoreResult<()>;

    // Send RaftMessage to local store.
    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::RaftMessage(msg))
    }

    // Send RaftCmdRequest to local store.
    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::new_raft_cmd(req, cb))
    }

    fn report_unreachable(&self, region_id: u64, to_peer_id: u64, _: u64) -> RaftStoreResult<()> {
        self.try_send(StoreMsg::ReportUnreachable {
            region_id: region_id,
            to_peer_id: to_peer_id,
        })
    }
}

#[derive(Clone)]
pub struct ServerRaftStoreRouter {
    pub ch: SendCh<StoreMsg>,
    store_id: u64,
}

impl ServerRaftStoreRouter {
    pub fn new(ch: SendCh<StoreMsg>, store_id: u64) -> ServerRaftStoreRouter {
        ServerRaftStoreRouter {
            ch: ch,
            store_id: store_id,
        }
    }

    fn validate_store_id(&self, store_id: u64) -> RaftStoreResult<()> {
        if store_id != self.store_id {
            let store = store_id.to_string();
            //REPORT_FAILURE_MSG_COUNTER.with_label_values(&["store_not_match", &*store]).inc();
            Err(RaftStoreError::StoreNotMatch(store_id, self.store_id))
        } else {
            Ok(())
        }
    }
}

impl RaftStoreRouter for ServerRaftStoreRouter {
    fn try_send(&self, msg: StoreMsg) -> RaftStoreResult<()> {
        try!(self.ch.try_send(msg));
        Ok(())
    }

    fn send(&self, msg: StoreMsg) -> RaftStoreResult<()> {
        try!(self.ch.send(msg));
        Ok(())
    }

    fn send_raft_msg(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let store_id = msg.get_to_peer().get_store_id();
        try!(self.validate_store_id(store_id));
        self.try_send(StoreMsg::RaftMessage(msg))
    }

    fn send_command(&self, req: RaftCmdRequest, cb: Callback) -> RaftStoreResult<()> {
        let store_id = req.get_header().get_peer().get_store_id();
        try!(self.validate_store_id(store_id));
        self.try_send(StoreMsg::new_raft_cmd(req, cb))
    }

    fn report_unreachable(&self,
                          region_id: u64,
                          to_peer_id: u64,
                          to_store_id: u64)
                          -> RaftStoreResult<()> {
        let store = to_store_id.to_string();
        // REPORT_FAILURE_MSG_COUNTER.with_label_values(&["unreachable", &*store]).inc();
        self.try_send(StoreMsg::ReportUnreachable {
            region_id: region_id,
            to_peer_id: to_peer_id,
        })
    }
}


#[derive(Clone)]
pub struct ServerTransport {
    ch: SendCh<Msg>,
    msg_id: Arc<AtomicUsize>,
}

impl ServerTransport {
    pub fn new(ch: SendCh<Msg>) -> ServerTransport {
        ServerTransport {
            ch: ch,
            msg_id: Arc::new(AtomicUsize::new(1)),
        }
    }

    fn alloc_msg_id(&self) -> u64 {
        self.msg_id.fetch_add(1, Ordering::Relaxed) as u64
    }
}

impl Transport for ServerTransport {
    fn send(&self, msg: RaftMessage) -> RaftStoreResult<()> {
        let to_store_id = msg.get_to_peer().get_store_id();

        let mut req = Message::new();
        req.set_msg_type(MessageType::Raft);
        req.set_raft(msg);

        try!(self.ch.try_send(Msg::SendStore {
            store_id: to_store_id,
            data: ConnData::new(self.alloc_msg_id(), req),
        }));
        Ok(())
    }
}