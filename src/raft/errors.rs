
use std::{cmp, result, io};
use std::error;

quick_error! {
	#[derive(Debug)]
	pub enum StorageError{
		Compacted{
			description("log compacted")
		}
		Unavailable{
			description("log unavaiable")
		}
		SnapshotOutOfDate{
			description("snapshot out of date")
		}
		SnapshotTemporarilyUnavailable{
			description("snapshot is temporarily unavaiable")
		}
		Other(err: Box<error::Error + Sync + Send>){
			from()
			cause(err.as_ref())
			description(err.description())
			display("unknow error {:?}", err)
		}
	}
}

impl cmp::PartialEq for StorageError {
    #[allow(match_same_arms)]
    fn eq(&self, other: &StorageError) -> bool {
        match (self, other) {
            (&StorageError::Compacted, &StorageError::Compacted) => true,
            (&StorageError::Unavailable, &StorageError::Unavailable) => true,
            (&StorageError::SnapshotOutOfDate, &StorageError::SnapshotOutOfDate) => true,
            (&StorageError::SnapshotTemporarilyUnavailable,
             &StorageError::SnapshotTemporarilyUnavailable) => true,
            _ => false,
        }
    }
}

 quick_error!{
 	#[derive(Debug)]
 	pub enum Error{
 		Io(err : io::Error){
			from()
			cause(err)
			description(err.description())
		}
		Store(err: StorageError){
			from()
            cause(err)
            description(err.description())
		}
		StepLocalMsg {
			description("raft: cannot step raft local message")
		}
		StepPeerNotFound{
			 description("raft: cannot step as peer not found")
		}
		ConfigInvalid(desc :String){
			description(desc)
		}
		Other(err: Box<error::Error + Sync + Send>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
	}
}

impl cmp::PartialEq for Error{
	fn eq(&self, other: &Error) -> bool {
		match (self, other) {
			(&Error::StepPeerNotFound, &Error::StepPeerNotFound) => true,
			(&Error::Store(ref e1), &Error::Store(ref e2)) => e1 == e2,
			(&Error::Io(ref e1), &Error::Io(ref e2)) => e1.kind() == e2.kind(),
            (&Error::StepLocalMsg, &Error::StepLocalMsg) => true,
            (&Error::ConfigInvalid(ref e1), &Error::ConfigInvalid(ref e2)) => e1 == e2,
            _ => false,
		}
	}
}

pub type Result<T> = result::Result<T, Error>;