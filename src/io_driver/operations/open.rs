use std::ffi::NulError;
use std::future::Future;
use std::os::fd::OwnedFd;
use std::{ffi::CString, os::fd::FromRawFd};

use io_uring::{opcode, squeue::Entry};

use crate::io_driver::{
    IO_DRIVER,
    operations::{IoDriverToken, IoOP},
};

pub struct OpenFile {
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
    let filename = CString::new(filename).map_err(|e| OpenFileError::NameIssue(e))?;
    let mut s = OpenFile {
        filename,
        flags,
        mode,
        id: None,
    };
    s.register_to_driver();
    let res = s.await.map_err(|e| OpenFileError::Io(e))?;
    Ok(res)
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
}

// File open flags
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

// File mode constants
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
