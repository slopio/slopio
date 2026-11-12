//! `statx(2)` on an open file, through the ring like every other file op.

use std::{
    any::Any,
    fmt,
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use io_uring::{opcode, types};

use crate::io_driver::{
    IO_DRIVER,
    operations::{OneshotToken, fd::OpFd, files::OpenedFile},
};

/// What the kernel reports about a file: its size, kind, owner, permission
/// bits and times. Resolved by [`OpenedFile::fstat`].
#[derive(Clone, Copy)]
pub struct Metadata(libc::statx);

impl Metadata {
    /// Size in bytes.
    pub fn len(&self) -> u64 {
        self.0.stx_size
    }

    /// Whether the file holds no bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn kind(&self) -> u32 {
        u32::from(self.0.stx_mode) & libc::S_IFMT
    }

    /// A regular file.
    pub fn is_file(&self) -> bool {
        self.kind() == libc::S_IFREG
    }

    /// A directory.
    pub fn is_dir(&self) -> bool {
        self.kind() == libc::S_IFDIR
    }

    /// The permission bits, without the file kind: `0o644` and the like.
    pub fn permissions(&self) -> u32 {
        u32::from(self.0.stx_mode) & 0o7777
    }

    /// User id of the owner.
    pub fn uid(&self) -> u32 {
        self.0.stx_uid
    }

    /// Group id of the owner.
    pub fn gid(&self) -> u32 {
        self.0.stx_gid
    }

    /// Inode number.
    pub fn ino(&self) -> u64 {
        self.0.stx_ino
    }

    /// Number of hard links.
    pub fn nlink(&self) -> u32 {
        self.0.stx_nlink
    }

    /// When the content last changed.
    pub fn modified(&self) -> SystemTime {
        system_time(&self.0.stx_mtime)
    }

    /// When the content was last read.
    pub fn accessed(&self) -> SystemTime {
        system_time(&self.0.stx_atime)
    }

    /// When the file was created, on a filesystem that records it.
    pub fn created(&self) -> Option<SystemTime> {
        (self.0.stx_mask & libc::STATX_BTIME != 0).then(|| system_time(&self.0.stx_btime))
    }
}

fn system_time(at: &libc::statx_timestamp) -> SystemTime {
    let nanos = Duration::from_nanos(u64::from(at.tv_nsec));
    match u64::try_from(at.tv_sec) {
        Ok(secs) => UNIX_EPOCH + Duration::from_secs(secs) + nanos,
        // Before 1970: the seconds count back, the nanoseconds forward.
        Err(_) => UNIX_EPOCH - Duration::from_secs(at.tv_sec.unsigned_abs()) + nanos,
    }
}

impl fmt::Debug for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Metadata")
            .field("len", &self.len())
            .field("is_file", &self.is_file())
            .field("is_dir", &self.is_dir())
            .field("permissions", &format_args!("{:#o}", self.permissions()))
            .field("modified", &self.modified())
            .finish_non_exhaustive()
    }
}

impl<M> OpenedFile<M> {
    /// Ask the kernel about the file: size, kind, owner, permissions, times.
    ///
    /// Asynchronous like a read: on a slow filesystem it is the task that
    /// waits, not the worker.
    pub fn fstat(&self) -> StatFile {
        StatFile {
            fd: self.share(),
            id: None,
            buf: Some(Box::new(unsafe { std::mem::zeroed() })),
        }
    }
}

/// Future of [`OpenedFile::fstat`].
pub struct StatFile {
    fd: OpFd,
    id: Option<OneshotToken>,
    /// `Some` until a cancel hands it to the driver as keepalive.
    buf: Option<Box<libc::statx>>,
}

impl StatFile {
    fn register_to_driver(&mut self) {
        let buf: *mut libc::statx = &mut **self.buf.as_mut().expect("buf present");
        // An empty path with `AT_EMPTY_PATH` names the descriptor itself.
        let entry = opcode::Statx::new(types::Fd(self.fd.fd()), c"".as_ptr(), buf.cast())
            .flags(libc::AT_EMPTY_PATH)
            .mask(libc::STATX_BASIC_STATS | libc::STATX_BTIME)
            .build();
        self.id = Some(IO_DRIVER.with(|d| d.push_op(entry)));
    }
}

impl Future for StatFile {
    type Output = std::io::Result<Metadata>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.id.is_none() {
            self.register_to_driver();
        }
        match IO_DRIVER.with(|d| d.poll_op(&mut self.id, cx.waker())) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(io_result) => Poll::Ready(match io_result.result {
                0 => Ok(Metadata(**self.buf.as_ref().expect("buf present"))),
                err => Err(std::io::Error::from_raw_os_error(-err)),
            }),
        }
    }
}

impl Drop for StatFile {
    fn drop(&mut self) {
        if let Some(token) = self.id.take() {
            // The kernel writes the answer until the cancelled op's CQE.
            let keep = self.buf.take().map(|b| b as Box<dyn Any>);
            let parked = Some(self.fd.park());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep, parked));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::system_time;
    use std::time::{Duration, UNIX_EPOCH};

    fn at(tv_sec: i64, tv_nsec: u32) -> libc::statx_timestamp {
        let mut t: libc::statx_timestamp = unsafe { std::mem::zeroed() };
        t.tv_sec = tv_sec;
        t.tv_nsec = tv_nsec;
        t
    }

    /// A time before 1970 counts its seconds back and its nanoseconds forward.
    #[test]
    fn a_timestamp_converts_on_both_sides_of_the_epoch() {
        assert_eq!(
            system_time(&at(10, 500)),
            UNIX_EPOCH + Duration::new(10, 500)
        );
        assert_eq!(system_time(&at(0, 0)), UNIX_EPOCH);
        assert_eq!(
            system_time(&at(-2, 250_000_000)),
            UNIX_EPOCH - Duration::from_millis(1750)
        );
    }
}
