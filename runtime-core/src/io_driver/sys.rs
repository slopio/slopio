//! The syscalls the runtime makes on a descriptor outside the ring.
//!
//! Under `simulation`, a descriptor the simulator invented is answered by its
//! fd table instead of the kernel. Without the feature each call is the bare
//! syscall.

use std::io;
use std::mem::MaybeUninit;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::os::fd::RawFd;

use libc::c_int;

#[cfg(feature = "simulation")]
use crate::deterministic_testing::fds;

/// Whether the current thread's driver runs on the simulated ring.
#[cfg(feature = "simulation")]
fn on_simulated_ring() -> bool {
    super::IO_DRIVER.try_with(|d| d.is_some_and(super::IODriver::is_simulated))
}

fn cvt(ret: c_int) -> io::Result<()> {
    if ret < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Close `fd` now, ignoring the result as `IORING_OP_CLOSE` does.
pub(crate) fn close(fd: RawFd) {
    #[cfg(feature = "simulation")]
    if fds::is_simulated(fd) {
        if fds::close(fd).is_err() {
            panic!("closing simulated fd {fd}, which is not open");
        }
        return;
    }
    unsafe { libc::close(fd) };
}

pub(crate) fn setsockopt<T>(fd: RawFd, level: c_int, name: c_int, value: &T) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    if fds::is_simulated(fd) {
        return fds::check_socket(fd);
    }
    cvt(unsafe {
        libc::setsockopt(
            fd,
            level,
            name,
            value as *const T as *const libc::c_void,
            std::mem::size_of::<T>() as libc::socklen_t,
        )
    })
}

pub(crate) fn shutdown(fd: RawFd, how: c_int) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    if fds::is_simulated(fd) {
        return fds::check_socket(fd);
    }
    cvt(unsafe { libc::shutdown(fd, how) })
}

pub(crate) fn listen(fd: RawFd, backlog: c_int) -> io::Result<()> {
    #[cfg(feature = "simulation")]
    if fds::is_simulated(fd) {
        return fds::check_socket(fd);
    }
    cvt(unsafe { libc::listen(fd, backlog) })
}

/// A TCP socket of `addr`'s family; `ty` carries the `SOCK_*` flags.
pub(crate) fn tcp_socket(addr: &SocketAddr, ty: c_int) -> io::Result<RawFd> {
    #[cfg(feature = "simulation")]
    if on_simulated_ring() {
        return Ok(fds::socket(addr));
    }
    let af = match addr {
        SocketAddr::V4(_) => libc::AF_INET,
        SocketAddr::V6(_) => libc::AF_INET6,
    };
    let fd = unsafe { libc::socket(af, libc::SOCK_STREAM | ty, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(fd)
}

/// A simulated socket bound to `addr`, when on the simulated ring; `None`
/// means bind a real one.
pub(crate) fn simulated_bound_socket(addr: SocketAddr) -> Option<RawFd> {
    #[cfg(feature = "simulation")]
    if on_simulated_ring() {
        return Some(fds::bound_socket(addr));
    }
    let _ = addr;
    None
}

pub(crate) fn local_addr(fd: RawFd) -> io::Result<SocketAddr> {
    #[cfg(feature = "simulation")]
    if fds::is_simulated(fd) {
        return fds::local_addr(fd);
    }
    getname(|sa, len| unsafe { libc::getsockname(fd, sa, len) })
}

pub(crate) fn peer_addr(fd: RawFd) -> io::Result<SocketAddr> {
    #[cfg(feature = "simulation")]
    if fds::is_simulated(fd) {
        return fds::peer_addr(fd);
    }
    getname(|sa, len| unsafe { libc::getpeername(fd, sa, len) })
}

fn getname(
    syscall: impl FnOnce(*mut libc::sockaddr, *mut libc::socklen_t) -> c_int,
) -> io::Result<SocketAddr> {
    let mut storage: MaybeUninit<libc::sockaddr_storage> = MaybeUninit::uninit();
    let mut len = std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t;
    let ret = syscall(
        storage.as_mut_ptr() as *mut libc::sockaddr,
        &mut len,
    );
    if ret < 0 {
        return Err(io::Error::last_os_error());
    }
    let storage = unsafe { storage.assume_init() };
    match storage.ss_family as c_int {
        libc::AF_INET => {
            let sin: libc::sockaddr_in =
                unsafe { *(&storage as *const _ as *const libc::sockaddr_in) };
            let ip = Ipv4Addr::from(sin.sin_addr.s_addr.to_ne_bytes());
            Ok(SocketAddr::V4(SocketAddrV4::new(
                ip,
                u16::from_be(sin.sin_port),
            )))
        }
        libc::AF_INET6 => {
            let sin6: libc::sockaddr_in6 =
                unsafe { *(&storage as *const _ as *const libc::sockaddr_in6) };
            let ip = Ipv6Addr::from(sin6.sin6_addr.s6_addr);
            Ok(SocketAddr::V6(SocketAddrV6::new(
                ip,
                u16::from_be(sin6.sin6_port),
                sin6.sin6_flowinfo,
                sin6.sin6_scope_id,
            )))
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported address family",
        )),
    }
}
