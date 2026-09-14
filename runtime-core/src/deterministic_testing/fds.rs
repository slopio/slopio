//! Descriptors the simulator hands out — invented, never the kernel's.
//!
//! Numbered from [`FIRST_SIMULATED_FD`], far above anything the kernel hands
//! out, so the runtime's out-of-ring syscalls (`setsockopt`, `getsockname`,
//! `shutdown`, `close`, …) can tell one apart by its number alone and answer it
//! from the table here. Nothing about them reaches the kernel, which is what
//! lets the simulated suites run under Miri.
//!
//! The table is per thread: a simulated runtime runs on the thread that built
//! it, and numbering stays the same from one run to the next.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::os::fd::RawFd;

use crate::io_driver::operations::sockets::SocketWrapper;

/// The first number the simulator hands out. A real descriptor this high
/// would need an `RLIMIT_NOFILE` above a billion.
pub const FIRST_SIMULATED_FD: RawFd = 1 << 30;

/// Where the simulator starts when a bind asks for port 0.
const FIRST_EPHEMERAL_PORT: u16 = 49152;

/// What kind of descriptor an op hands back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FdKind {
    /// A file, for `OPENAT`. Socket calls on it fail with `ENOTSOCK`.
    File,
    /// A TCP socket, for `ACCEPT`. Unconnected: `peer_addr` reports `ENOTCONN`,
    /// and no IO reaches it — sends and receives go through the timeline.
    Socket,
}

struct Open {
    kind: FdKind,
    local: SocketAddr,
}

struct Table {
    next_fd: RawFd,
    next_port: u16,
    open: HashMap<RawFd, Open>,
}

thread_local! {
    static TABLE: RefCell<Table> = RefCell::new(Table {
        next_fd: FIRST_SIMULATED_FD,
        next_port: FIRST_EPHEMERAL_PORT,
        open: HashMap::new(),
    });
}

/// Whether `fd` is one the simulator would have handed out, open or not.
pub fn is_simulated(fd: RawFd) -> bool {
    fd >= FIRST_SIMULATED_FD
}

/// Whether `fd` was handed out and not yet closed — an fd still open at the end
/// of a test is a leak.
pub fn is_open(fd: RawFd) -> bool {
    TABLE.with_borrow(|t| t.open.contains_key(&fd))
}

/// Hand out a fresh descriptor of `kind`. Never reuses a number, so a stale
/// fd cannot alias a newer one.
pub fn mint(kind: FdKind) -> RawFd {
    mint_at(kind, SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)))
}

fn mint_at(kind: FdKind, local: SocketAddr) -> RawFd {
    TABLE.with_borrow_mut(|t| {
        let fd = t.next_fd;
        t.next_fd = fd.checked_add(1).expect("simulated descriptors exhausted");
        t.open.insert(fd, Open { kind, local });
        fd
    })
}

/// A [`SocketWrapper`] over a simulated TCP socket, for tests that need
/// something to call the runtime's socket operations on.
pub fn simulated_socket() -> SocketWrapper {
    SocketWrapper::from_fd(mint(FdKind::Socket))
}

/// `socket(2)` for the family of `addr`.
pub(crate) fn socket(addr: &SocketAddr) -> RawFd {
    let local = match addr {
        SocketAddr::V4(_) => SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0)),
        SocketAddr::V6(_) => SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0)),
    };
    mint_at(FdKind::Socket, local)
}

/// A socket already bound to `addr`; port 0 draws the next ephemeral port.
pub(crate) fn bound_socket(mut addr: SocketAddr) -> RawFd {
    if addr.port() == 0 {
        let port = TABLE.with_borrow_mut(|t| {
            let port = t.next_port;
            t.next_port = port.checked_add(1).unwrap_or(FIRST_EPHEMERAL_PORT);
            port
        });
        addr.set_port(port);
    }
    mint_at(FdKind::Socket, addr)
}

pub(crate) fn close(fd: RawFd) -> io::Result<()> {
    TABLE
        .with_borrow_mut(|t| t.open.remove(&fd))
        .map(drop)
        .ok_or_else(|| io::Error::from_raw_os_error(libc::EBADF))
}

/// `EBADF` unless open, `ENOTSOCK` unless a socket — all a simulated
/// `setsockopt`, `shutdown` or `listen` does.
pub(crate) fn check_socket(fd: RawFd) -> io::Result<()> {
    local_addr(fd).map(drop)
}

pub(crate) fn local_addr(fd: RawFd) -> io::Result<SocketAddr> {
    TABLE.with_borrow(|t| match t.open.get(&fd) {
        None => Err(io::Error::from_raw_os_error(libc::EBADF)),
        Some(Open { kind: FdKind::File, .. }) => Err(io::Error::from_raw_os_error(libc::ENOTSOCK)),
        Some(Open { local, .. }) => Ok(*local),
    })
}

pub(crate) fn peer_addr(fd: RawFd) -> io::Result<SocketAddr> {
    check_socket(fd)?;
    Err(io::Error::from_raw_os_error(libc::ENOTCONN))
}
