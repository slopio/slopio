//! Files, opened for a known direction.
//!
//! A file handle carries how it was opened in its type, and offers only what
//! that allows: a handle opened for reading has no `write`, one opened for
//! writing has no `read`. The kernel fixes the access mode of a descriptor when
//! it opens it and never changes it, so the type stays true for the handle's
//! life.
//!
//! ```ignore
//! let log = OpenedFile::create("/var/log/app.log").await?;   // OpenedFile<Write>
//! log.write(b"started\n").await.0?;
//!
//! let conf = OpenedFile::open("/etc/app.conf").await?;       // OpenedFile<Read>
//! let (n, buf) = conf.read(Vec::with_capacity(4096)).await;
//! ```
//!
//! Each mode offers what it can honour:
//!
//! ```
//! use slopio::io_driver::operations::files::access::{Read, ReadWrite, Write};
//! use slopio::io_driver::operations::files::{OpenOptions, OpenedFile};
//!
//! fn reads(file: &OpenedFile<Read>) {
//!     let _ = file.read(Vec::<u8>::with_capacity(16));
//! }
//! fn writes(file: &OpenedFile<Write>) {
//!     let _ = file.write(b"yes");
//! }
//! fn both(file: &OpenedFile<ReadWrite>) {
//!     let _ = file.read(Vec::<u8>::with_capacity(16));
//!     let _ = file.write(b"yes");
//! }
//! let _ = OpenOptions::write().truncate();
//! let _ = OpenOptions::read().direct();
//! ```
//!
//! Reading from a handle opened for writing does not compile:
//!
//! ```compile_fail
//! use slopio::io_driver::operations::files::{OpenedFile, access::Write};
//! fn read_it(file: &OpenedFile<Write>) {
//!     let _ = file.read(Vec::<u8>::with_capacity(16));
//! }
//! ```
//!
//! Nor does writing to one opened for reading:
//!
//! ```compile_fail
//! use slopio::io_driver::operations::files::{OpenedFile, access::Read};
//! fn write_it(file: &OpenedFile<Read>) {
//!     let _ = file.write(b"no");
//! }
//! ```
//!
//! Nor does asking a read-only open to truncate the file, which Linux would
//! do:
//!
//! ```compile_fail
//! use slopio::io_driver::operations::files::OpenOptions;
//! let _ = OpenOptions::read().truncate();
//! ```

use std::{
    fs::Metadata,
    marker::PhantomData,
    os::fd::{FromRawFd, IntoRawFd},
    rc::Rc,
};

use crate::io_driver::operations::fd::{FdInner, OpFd};
pub mod open;
pub mod read;
pub mod write;

pub use open::{OpenFileError, OpenOptions};
pub use read::ReadFile;
pub use write::{WriteFile, write_all};

/// How a file is opened: the marker types a handle is parameterised by.
pub mod access {
    mod sealed {
        pub trait Sealed {}
    }

    /// An access mode. Sealed: the three modes of `open(2)` are the whole set.
    pub trait Access: sealed::Sealed {
        #[doc(hidden)]
        const FLAGS: i32;
    }

    /// A mode whose handles can be read from.
    pub trait Readable: Access {}

    /// A mode whose handles can be written to.
    pub trait Writable: Access {}

    /// Opened for reading only.
    #[derive(Debug)]
    pub struct Read;

    /// Opened for writing only.
    #[derive(Debug)]
    pub struct Write;

    /// Opened for reading and writing.
    #[derive(Debug)]
    pub struct ReadWrite;

    impl sealed::Sealed for Read {}
    impl sealed::Sealed for Write {}
    impl sealed::Sealed for ReadWrite {}

    impl Access for Read {
        const FLAGS: i32 = libc::O_RDONLY;
    }
    impl Access for Write {
        const FLAGS: i32 = libc::O_WRONLY;
    }
    impl Access for ReadWrite {
        const FLAGS: i32 = libc::O_RDWR;
    }

    impl Readable for Read {}
    impl Readable for ReadWrite {}
    impl Writable for Write {}
    impl Writable for ReadWrite {}
}

use access::{Read, ReadWrite, Write};

/// An open file, typed by its access mode `M`: [`Read`], [`Write`] or
/// [`ReadWrite`]. `read` exists on the readable modes and `write` on the
/// writable ones.
///
/// The fd closes once this handle and every operation prepared on it are gone.
#[derive(Debug)]
pub struct OpenedFile<M> {
    inner: Rc<FdInner>,
    _mode: PhantomData<M>,
}

impl OpenedFile<Read> {
    /// Open an existing file for reading.
    pub async fn open(path: &str) -> Result<Self, OpenFileError> {
        OpenOptions::read().open(path).await
    }
}

impl OpenedFile<Write> {
    /// Open a file for writing, created with [`open::mode::DEFAULT`] if it does
    /// not exist and emptied if it does.
    pub async fn create(path: &str) -> Result<Self, OpenFileError> {
        OpenOptions::write()
            .create(open::mode::DEFAULT)
            .truncate()
            .open(path)
            .await
    }
}

impl OpenedFile<ReadWrite> {
    /// Open an existing file for reading and writing.
    pub async fn open_rw(path: &str) -> Result<Self, OpenFileError> {
        OpenOptions::read_write().open(path).await
    }
}

impl<M> OpenedFile<M> {
    /// `fd` must have been opened with the access mode `M` names: the type is
    /// what stands between a caller and an `EBADF` the driver panics on.
    pub(crate) fn from_fd(fd: i32) -> Self {
        Self {
            inner: FdInner::new(fd),
            _mode: PhantomData,
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

#[cfg(test)]
mod tests {
    use super::{OpenedFile, access::Read};
    use crate::runtime::main_thread;

    /// With the wrong access mode ruled out by the type, `EBADF` on a file can
    /// only mean a descriptor that is not open: the driver panics, as it does
    /// for a socket.
    #[test]
    fn a_file_op_on_a_descriptor_that_is_not_open_panics() {
        let result = std::thread::spawn(|| {
            main_thread(async {
                let file = OpenedFile::<Read>::from_fd(1_000_000);
                let (res, _buf) = file.read(Vec::<u8>::with_capacity(8)).await;
                drop(res);
            })
        })
        .join();
        assert!(result.is_err(), "EBADF on a file must panic the runtime");
    }
}
