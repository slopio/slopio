use std::os::fd::{FromRawFd, OwnedFd};
pub mod open;

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
