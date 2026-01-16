use std::ffi::CString;
use std::ffi::NulError;
use std::future::Future;

use io_uring::{opcode, squeue::Entry};

use crate::io_driver::IO_DRIVER;
use crate::io_driver::operations::IoDriverToken;
use crate::io_driver::operations::IoOP;
use crate::io_driver::operations::files::OpenedFile;

struct OpenFile {
    filename: CString,
    flags: i32,
    mode: u32,
    id: Option<usize>,
}

#[derive(Debug)]
pub enum OpenFileError {
    NameIssue(NulError),
    Io(std::io::Error),
}

pub async fn open_file(filename: &str, flags: i32, mode: u32) -> Result<OpenedFile, OpenFileError> {
    let filename = CString::new(filename).map_err(OpenFileError::NameIssue)?;
    let mut s = OpenFile {
        filename,
        flags,
        mode,
        id: None,
    };
    s.register_to_driver();
    let res = s.await.map_err(OpenFileError::Io)?;
    Ok(res)
}

impl Future for OpenFile {
    type Output = std::io::Result<OpenedFile>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let waker = cx.waker();
        let id = self.id.unwrap();
        match IO_DRIVER.with(|d| d.poll_op(id, waker)) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(io_result) => std::task::Poll::Ready(match io_result.result {
                fd @ 0.. => Ok(OpenedFile { fd }),
                err => Err(std::io::Error::from_raw_os_error(-err)),
            }),
        }
    }
}

impl Drop for OpenFile {
    fn drop(&mut self) {}
}

impl IoOP for OpenFile {
    fn register_to_driver(&mut self) -> IoDriverToken {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token as usize);
        token
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

pub mod flags {
    #![allow(unused)]
    pub const READ_ONLY: i32 = libc::O_RDONLY;
    pub const WRITE_ONLY: i32 = libc::O_WRONLY;
    pub const READ_WRITE: i32 = libc::O_RDWR;
    pub const CREATE: i32 = libc::O_CREAT;
    pub const TRUNCATE: i32 = libc::O_TRUNC;
    pub const APPEND: i32 = libc::O_APPEND;
    pub const EXCLUSIVE: i32 = libc::O_EXCL;
    pub const DIRECT: i32 = libc::O_DIRECT;
    pub const SYNC: i32 = libc::O_SYNC;
    pub const NONBLOCK: i32 = libc::O_NONBLOCK;
}

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
