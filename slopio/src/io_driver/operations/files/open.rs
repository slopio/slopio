use std::any::Any;
use std::ffi::CString;
use std::ffi::NulError;
use std::future::Future;

use io_uring::{opcode, squeue::Entry};

use crate::io_driver::IO_DRIVER;
use crate::io_driver::operations::IoOP;
use crate::io_driver::operations::OneshotToken;
use crate::io_driver::operations::files::OpenedFile;
use crate::io_driver::operations::files::access::{Access, Read, ReadWrite, Writable, Write};
use std::marker::PhantomData;

struct OpenFile {
    filename: CString,
    flags: i32,
    mode: u32,
    id: Option<OneshotToken>,
}

#[derive(Debug)]
pub enum OpenFileError {
    NameIssue(NulError),
    Io(std::io::Error),
}

/// How to open a file, for the access mode `M`.
///
/// The access mode comes from the constructor and ends up in the type of the
/// handle; the options a mode cannot honour are not offered for it.
///
/// ```ignore
/// let log = OpenOptions::write().create(mode::DEFAULT).append().open(path).await?;
/// ```
#[derive(Debug)]
pub struct OpenOptions<M> {
    /// Everything but the access mode, which `M` supplies.
    flags: i32,
    mode: u32,
    _mode: PhantomData<M>,
}

impl OpenOptions<Read> {
    /// Open for reading only.
    pub fn read() -> Self {
        Self::new()
    }
}

impl OpenOptions<Write> {
    /// Open for writing only.
    pub fn write() -> Self {
        Self::new()
    }
}

impl OpenOptions<ReadWrite> {
    /// Open for reading and writing.
    pub fn read_write() -> Self {
        Self::new()
    }
}

impl<M: Access> OpenOptions<M> {
    fn new() -> Self {
        Self {
            flags: 0,
            mode: 0,
            _mode: PhantomData,
        }
    }

    fn with(mut self, flag: i32) -> Self {
        self.flags |= flag;
        self
    }

    /// Bypass the page cache (`O_DIRECT`). Buffers, lengths and offsets must
    /// then meet the device's alignment.
    pub fn direct(self) -> Self {
        self.with(libc::O_DIRECT)
    }

    /// Open `path`. The handle's type carries the access mode asked for.
    pub async fn open(self, path: &str) -> Result<OpenedFile<M>, OpenFileError> {
        let filename = CString::new(path).map_err(OpenFileError::NameIssue)?;
        let mut s = OpenFile {
            filename,
            flags: M::FLAGS | self.flags,
            mode: self.mode,
            id: None,
        };
        s.register_to_driver();
        let fd = s.await.map_err(OpenFileError::Io)?;
        Ok(OpenedFile::from_fd(fd))
    }
}

/// What only makes sense on a file opened for writing. On a read-only open the
/// kernel would still create the file, and still truncate it.
impl<M: Writable> OpenOptions<M> {
    /// Create the file with permission bits `mode` if it does not exist. See
    /// [`mode`] for the usual values.
    pub fn create(mut self, mode: u32) -> Self {
        self.mode = mode;
        self.with(libc::O_CREAT)
    }

    /// Create the file with permission bits `mode`, and fail with `EEXIST` if
    /// it already exists.
    pub fn create_new(mut self, mode: u32) -> Self {
        self.mode = mode;
        self.with(libc::O_CREAT | libc::O_EXCL)
    }

    /// Empty the file on open.
    pub fn truncate(self) -> Self {
        self.with(libc::O_TRUNC)
    }

    /// Every write lands at the end of the file.
    pub fn append(self) -> Self {
        self.with(libc::O_APPEND)
    }

    /// Every write returns once the data is on stable storage (`O_SYNC`).
    pub fn sync(self) -> Self {
        self.with(libc::O_SYNC)
    }
}

impl Future for OpenFile {
    /// The raw fd: its opener gives it a handle of the right type at once.
    type Output = std::io::Result<i32>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let waker = cx.waker();
        match IO_DRIVER.with(|d| d.poll_op(&mut self.id, waker)) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(io_result) => std::task::Poll::Ready(match io_result.result {
                fd @ 0.. => Ok(fd),
                err => Err(std::io::Error::from_raw_os_error(-err)),
            }),
        }
    }
}

impl Drop for OpenFile {
    fn drop(&mut self) {
        if let Some(token) = self.id.take() {
            // The kernel reads the filename from the `CString` during
            // `openat`. Move it into the driver's keepalive so it outlives
            // any in-flight kernel access after cancel.
            let filename = std::mem::take(&mut self.filename);
            let keep: Box<dyn Any> = Box::new(filename);
            IO_DRIVER.with(|d| d.cancel_fd_producing_op(token, Some(keep)));
        }
    }
}

impl IoOP for OpenFile {
    fn register_to_driver(&mut self) {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }

    fn to_entry(&self) -> Entry {
        opcode::OpenAt::new(io_uring::types::Fd(libc::AT_FDCWD), self.filename.as_ptr())
            .flags(self.flags)
            .mode(self.mode)
            .build()
    }

    fn driver_cleanup(&mut self) {
        unimplemented!()
    }
}

/// Permission bits for a file an open creates.
pub mod mode {
    #![allow(unused)]
    pub const OWNER_READ: u32 = 0o400;
    pub const OWNER_WRITE: u32 = 0o200;
    pub const OWNER_EXEC: u32 = 0o100;
    pub const GROUP_READ: u32 = 0o040;
    pub const GROUP_WRITE: u32 = 0o020;
    pub const GROUP_EXEC: u32 = 0o010;
    pub const OTHER_READ: u32 = 0o004;
    pub const OTHER_WRITE: u32 = 0o002;
    pub const OTHER_EXEC: u32 = 0o001;

    pub const RW_USER: u32 = 0o600;
    pub const RW_ALL: u32 = 0o666;
    pub const RWX_USER: u32 = 0o700;
    pub const DEFAULT: u32 = 0o644;
}
