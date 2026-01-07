use std::os::fd::{FromRawFd, OwnedFd};
pub mod open;
pub mod read;
pub mod write;

// Re-export commonly used items
pub use open::{OpenFileError, open_file};
pub use read::ReadFile;
pub use write::WriteFile;

///this is the file handle, this file will be synchronously closed at drop.
#[derive(Debug)]
pub struct OpenedFile {
    fd: i32,
}

pub trait AsyncFileRead {
    type Reader<'a>;
    fn read<'a>(&self, buffer: &'a mut [u8], len: u32) -> Self::Reader<'a>;
}

pub trait AsyncFileWrite {
    type Writer<'a>;
    fn write<'a>(&self, data: &'a [u8]) -> Self::Writer<'a>;
}

impl Drop for OpenedFile {
    ///synchronous close, async close can lead to fd conflicts
    fn drop(&mut self) {
        let fd = unsafe { OwnedFd::from_raw_fd(self.fd) };
        drop(fd);
    }
}
