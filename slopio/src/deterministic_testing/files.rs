//! What the simulated kernel knows about the files it has open.
//!
//! No content and no filesystem: reads stay scripted, and two opens of one path
//! are two unrelated files. What is kept is what `statx` reports and what moves
//! it — a size that follows the writes, the position reads and writes share,
//! the time of the last write — so a program that writes and then asks sees its
//! own writes, without the test having to script the answer.

use std::collections::HashMap;
use std::os::fd::RawFd;
use std::time::{SystemTime, UNIX_EPOCH};

use super::fds::{self, FdKind};
use super::sqe::Sqe;

/// What `statx` reports about a file, as a test describes it.
///
/// The rest is the simulator's: one hard link, two for a directory, owned by
/// uid and gid 0, an inode number that is the descriptor's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileMeta {
    dir: bool,
    size: u64,
    permissions: u16,
    modified: Option<SystemTime>,
}

impl FileMeta {
    /// A regular file of `size` bytes, `0o644`.
    pub fn file(size: u64) -> Self {
        Self {
            dir: false,
            size,
            permissions: 0o644,
            modified: None,
        }
    }

    /// A directory, `0o755`.
    pub fn dir() -> Self {
        Self {
            dir: true,
            size: 0,
            permissions: 0o755,
            modified: None,
        }
    }

    /// The permission bits, `0o600` and the like.
    pub fn permissions(mut self, bits: u32) -> Self {
        assert!(bits <= 0o7777, "{bits:#o} is more than permission bits");
        self.permissions = bits as u16;
        self
    }

    /// When the content last changed. Left unsaid, the file is as old as the
    /// ring: last changed when it was built.
    pub fn modified(mut self, at: SystemTime) -> Self {
        self.modified = Some(at);
        self
    }
}

/// Everything a `statx` answer is made of.
#[derive(Clone, Copy)]
struct Stat {
    dir: bool,
    size: u64,
    permissions: u16,
    modified: SystemTime,
    created: SystemTime,
    ino: u64,
}

impl Stat {
    /// `meta` as the file behind `fd`, dated `born` where it says nothing.
    fn of(fd: RawFd, meta: FileMeta, born: SystemTime) -> Self {
        let modified = meta.modified.unwrap_or(born);
        Self {
            dir: meta.dir,
            size: meta.size,
            permissions: meta.permissions,
            modified,
            created: modified,
            ino: fd as u64,
        }
    }

    fn statx(&self) -> libc::statx {
        // SAFETY: `statx` is integers and padding all the way down, so all
        // zeroes is a value of it.
        let mut st: libc::statx = unsafe { std::mem::zeroed() };
        let kind = if self.dir {
            libc::S_IFDIR
        } else {
            libc::S_IFREG
        };
        st.stx_mask = libc::STATX_BASIC_STATS | libc::STATX_BTIME;
        st.stx_blksize = 4096;
        st.stx_nlink = if self.dir { 2 } else { 1 };
        st.stx_mode = kind as u16 | self.permissions;
        st.stx_ino = self.ino;
        st.stx_size = self.size;
        st.stx_blocks = self.size.div_ceil(512);
        // Mounted `noatime`: reading a file leaves no trace on it.
        st.stx_atime = timestamp(self.created);
        st.stx_btime = timestamp(self.created);
        st.stx_ctime = timestamp(self.modified);
        st.stx_mtime = timestamp(self.modified);
        st
    }
}

fn timestamp(at: SystemTime) -> libc::statx_timestamp {
    // SAFETY: two integers and padding.
    let mut ts: libc::statx_timestamp = unsafe { std::mem::zeroed() };
    match at.duration_since(UNIX_EPOCH) {
        Ok(since) => {
            ts.tv_sec = since.as_secs() as i64;
            ts.tv_nsec = since.subsec_nanos();
        }
        // Before 1970: the seconds count back, the nanoseconds forward.
        Err(before) => {
            let before = before.duration();
            let nanos = before.subsec_nanos();
            ts.tv_sec = -(before.as_secs() as i64) - i64::from(nanos > 0);
            ts.tv_nsec = (1_000_000_000 - nanos) % 1_000_000_000;
        }
    }
    ts
}

struct File {
    stat: Stat,
    /// Where the next read or write at the current position lands.
    pos: u64,
    /// `O_APPEND`: every write lands at the end, wherever `pos` is.
    append: bool,
}

/// `off` of an op that reads or writes at the file's current position and
/// moves it, as `read(2)` and `write(2)` do.
const CURRENT_POSITION: u64 = u64::MAX;

pub(crate) struct Files {
    /// When the ring was built: the date of whatever is older than the run.
    born: SystemTime,
    open: HashMap<RawFd, File>,
}

impl Files {
    pub(crate) fn new(born: SystemTime) -> Self {
        Self {
            born,
            open: HashMap::new(),
        }
    }

    /// The `OPENAT` in `sqe` handed out `fd`, onto the file `declared` says
    /// was there.
    ///
    /// With nothing declared, an open that may create finds nothing and
    /// creates the file, with the mode it asked for, as under a umask of zero;
    /// one that may not finds an empty file as old as the ring. Whether the
    /// flags could have succeeded against that is not judged: an open that
    /// must fail is scripted to.
    pub(crate) fn opened(
        &mut self,
        fd: RawFd,
        sqe: &Sqe,
        declared: Option<FileMeta>,
        now: SystemTime,
    ) {
        let flags = sqe.op_flags as i32;
        let mut stat = match declared {
            Some(meta) => Stat::of(fd, meta, self.born),
            None if flags & libc::O_CREAT != 0 => {
                let created = FileMeta::file(0).permissions(sqe.len & 0o7777);
                Stat::of(fd, created, now)
            }
            None => Stat::of(fd, FileMeta::file(0), self.born),
        };
        // Whatever the access mode: Linux truncates on a read-only open too.
        if flags & libc::O_TRUNC != 0 && !stat.dir {
            stat.size = 0;
            stat.modified = now;
        }
        let file = File {
            stat,
            pos: 0,
            append: flags & libc::O_APPEND != 0,
        };
        self.open.insert(fd, file);
    }

    /// The file behind `fd`, if `fd` is an open simulated file. One the ring
    /// did not open — minted by a test — is an empty file as old as the ring.
    fn of(&mut self, fd: RawFd) -> Option<&mut File> {
        if fds::kind(fd) != Some(FdKind::File) {
            return None;
        }
        let born = self.born;
        Some(self.open.entry(fd).or_insert_with(|| File {
            stat: Stat::of(fd, FileMeta::file(0), born),
            pos: 0,
            append: false,
        }))
    }

    /// The read in `sqe` was handed `n` bytes.
    pub(crate) fn read(&mut self, sqe: &Sqe, n: u64) {
        if sqe.off == CURRENT_POSITION
            && let Some(file) = self.of(sqe.fd)
        {
            file.pos += n;
        }
    }

    /// `n` bytes of the write in `sqe` were accepted.
    pub(crate) fn wrote(&mut self, sqe: &Sqe, n: u64, now: SystemTime) {
        // A write of nothing leaves the file as it was, date included.
        if n == 0 {
            return;
        }
        let Some(file) = self.of(sqe.fd) else {
            return;
        };
        let moves = sqe.off == CURRENT_POSITION;
        let at = match (file.append, moves) {
            (true, _) => file.stat.size,
            (false, true) => file.pos,
            (false, false) => sqe.off,
        };
        let end = at + n;
        if moves {
            file.pos = end;
        }
        file.stat.size = file.stat.size.max(end);
        file.stat.modified = now;
    }

    /// What `statx` on `fd` itself reports, if `fd` is an open simulated file.
    pub(crate) fn stat(&mut self, fd: RawFd) -> Option<libc::statx> {
        self.of(fd).map(|file| file.stat.statx())
    }

    /// What `statx` reports for a file a test described.
    pub(crate) fn described(&self, fd: RawFd, meta: FileMeta) -> libc::statx {
        Stat::of(fd, meta, self.born).statx()
    }

    pub(crate) fn closed(&mut self, fd: RawFd) {
        self.open.remove(&fd);
    }
}

#[cfg(test)]
mod tests {
    use super::timestamp;
    use std::time::{Duration, UNIX_EPOCH};

    /// The inverse of the conversion `Metadata` reads times with.
    #[test]
    fn a_time_converts_on_both_sides_of_the_epoch() {
        let after = timestamp(UNIX_EPOCH + Duration::new(10, 500));
        assert_eq!((after.tv_sec, after.tv_nsec), (10, 500));

        let epoch = timestamp(UNIX_EPOCH);
        assert_eq!((epoch.tv_sec, epoch.tv_nsec), (0, 0));

        let before = timestamp(UNIX_EPOCH - Duration::from_millis(1750));
        assert_eq!((before.tv_sec, before.tv_nsec), (-2, 250_000_000));

        let whole = timestamp(UNIX_EPOCH - Duration::from_secs(3));
        assert_eq!((whole.tv_sec, whole.tv_nsec), (-3, 0));
    }
}
