use std::os::fd::{FromRawFd, OwnedFd};

use io_uring::squeue::Entry;

pub mod open;

pub(crate) type IoDriverToken = u64;

trait IoOP: Drop {
    fn register_to_driver(&mut self) -> IoDriverToken;

    fn to_entry(&self) -> Entry;
}

///this is the file handle, this file will be synchronously closed at drop.
#[derive(Debug)]
pub struct OpenedFile {
    fd: i32,
}

impl Drop for OpenedFile {
    ///synchronous close, async close can lead to fd conflicts
    fn drop(&mut self) {
        let fd = unsafe { OwnedFd::from_raw_fd(self.fd) };
        drop(fd);
    }
}
