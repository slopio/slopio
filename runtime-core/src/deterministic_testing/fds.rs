//! Descriptors the simulator hands out — real ones.
//!
//! Two reasons they cannot be made-up integers. Whatever receives one will
//! eventually `close` it, shutting an unrelated descriptor of the test process.
//! And the runtime makes genuine syscalls outside the ring — `getsockname`,
//! `setsockopt`, `listen`, `shutdown` — which fail with `ENOTSOCK` on anything
//! that is not a socket.

use std::io;
use std::os::fd::RawFd;

use crate::io_driver::operations::sockets::SocketWrapper;

/// What kind of descriptor an op hands back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FdKind {
    /// An anonymous in-memory file, for `OPENAT`. Writable and readable, with
    /// nothing on disk to clean up.
    File,
    /// An unconnected TCP socket, for `ACCEPT`.
    ///
    /// Unconnected is enough, because no IO ever reaches it — sends and
    /// receives go through the timeline. What matters is that the kernel sees a
    /// real TCP socket, so the `IPPROTO_TCP` options the runtime sets are
    /// accepted and `local_addr` parses. `peer_addr` reports `ENOTCONN`, which
    /// is the truth.
    ///
    /// An `AF_UNIX` socketpair would not do: `TCP_NODELAY` and friends return
    /// `ENOPROTOOPT` on it, and its address does not convert to a `SocketAddr`.
    Socket,
}

/// Create a descriptor of `kind`.
pub fn mint(kind: FdKind) -> io::Result<RawFd> {
    let fd = match kind {
        // SAFETY: a 'static NUL-terminated C string, which `memfd_create` only
        // reads. The name need not be unique and is only visible in /proc.
        FdKind::File => unsafe { libc::memfd_create(c"fake-ring".as_ptr(), libc::MFD_CLOEXEC) },
        // SAFETY: plain scalar arguments; no pointers involved.
        FdKind::Socket => unsafe {
            libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0)
        },
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

/// A [`SocketWrapper`] over a real, unconnected TCP socket, for tests that need
/// something to call the runtime's socket operations on.
///
/// No bind, no port, no listener, no traffic — but every socket syscall the
/// runtime makes outside the ring works, and the wrapper's `Drop` closes a
/// descriptor that genuinely exists.
pub fn simulated_socket() -> io::Result<SocketWrapper> {
    use std::os::fd::FromRawFd;
    // SAFETY: `mint` opened the fd and nothing else owns it.
    Ok(unsafe { SocketWrapper::from_raw_fd(mint(FdKind::Socket)?) })
}
