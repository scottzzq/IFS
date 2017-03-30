use std::boxed::Box;
use std::error;

use uuid::Uuid;

use kvproto::raft_cmdpb::RaftCmdResponse;
use raftstore::Error;


pub fn bind_uuid(resp: &mut RaftCmdResponse, uuid: Uuid) {
    resp.mut_header().set_uuid(uuid.as_bytes().to_vec());
}

pub fn bind_term(resp: &mut RaftCmdResponse, term: u64) {
    if term == 0 {
        return;
    }

    resp.mut_header().set_current_term(term);
}

pub fn bind_error(resp: &mut RaftCmdResponse, err: Error) {
    resp.mut_header().set_error(err.into());
}

pub fn new_error(err: Error) -> RaftCmdResponse {
    let mut resp = RaftCmdResponse::new();
    bind_error(&mut resp, err);
    resp
}

pub fn err_resp(e: Error, uuid: Uuid, term: u64) -> RaftCmdResponse {
    let mut resp = new_error(e);
    bind_term(&mut resp, term);
    bind_uuid(&mut resp, uuid);
    resp
}

pub fn message_error<E>(err: E) -> RaftCmdResponse
    where E: Into<Box<error::Error + Send + Sync>>
{
    new_error(Error::Other(err.into()))
}
