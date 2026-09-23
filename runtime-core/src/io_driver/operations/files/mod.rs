use std::{
    fs::Metadata,
    os::fd::{FromRawFd, IntoRawFd},
    rc::Rc,
};

use crate::io_driver::operations::fd::{FdInner, OpFd};
pub mod open;
pub mod read;
pub mod write;

pub use open::{OpenFileError, open_file};
pub use read::ReadFile;
pub use write::{WriteFile, write_all};

/// The fd closes once this handle and every operation prepared on it are gone.
#[derive(Debug)]
pub struct OpenedFile {
    inner: Rc<FdInner>,
}

impl OpenedFile {
    pub(crate) fn from_fd(fd: i32) -> Self {
        Self {
            inner: FdInner::new(fd),
        }
    }

    pub fn fd(&self) -> i32 {
        self.inner.fd()
    }

    /// A strong ref for an operation to hold until completion.
    pub(crate) fn share(&self) -> OpFd {
        OpFd::new(&self.inner)
    }

    pub fn fstat(&self) -> std::io::Result<Metadata> {
        let f = unsafe { std::fs::File::from_raw_fd(self.fd()) };
        let metadata = f.metadata()?;
        let _ = f.into_raw_fd(); //preventing closing the fd
        Ok(metadata)
    }
}
