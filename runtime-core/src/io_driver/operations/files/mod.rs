use std::{
    fs::Metadata,
    os::fd::{FromRawFd, IntoRawFd, OwnedFd},
};
pub mod open;
pub mod read;
pub mod write;

pub use open::{OpenFileError, open_file};
pub use read::ReadFile;
pub use write::{WriteFile, write_all};

#[derive(Debug)]
pub struct OpenedFile {
    pub(crate) fd: i32,
}

impl Drop for OpenedFile {
    fn drop(&mut self) {
        let fd = unsafe { OwnedFd::from_raw_fd(self.fd) };
        drop(fd);
    }
}

impl OpenedFile {
    pub fn fstat(&self) -> std::io::Result<Metadata> {
        let f = unsafe { std::fs::File::from_raw_fd(self.fd) };
        let metadata = f.metadata()?;
        let _ = f.into_raw_fd(); //preventing closing the fd
        Ok(metadata)
    }
}
