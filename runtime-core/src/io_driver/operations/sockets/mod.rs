use libc::c_int;
use std::io::{self, IoSlice};
use std::mem::MaybeUninit;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};
use std::time::Duration;

use self::send_control_record::{socket_send_control_record, WriteControlRecord};
use self::send_vectored::{socket_send_vectored, WriteVectoredSocket};

pub mod connect;
pub mod managed_recv;
pub mod managed_recv_bundle;
pub mod multi_accept;
pub mod multi_recv;
pub mod recv;
pub mod send;
pub mod send_control_record;
pub mod send_vectored;
pub mod udp;

#[derive(Debug)]
pub struct SocketWrapper {
    pub fd: i32,
}

impl SocketWrapper {
    pub fn fd(&self) -> i32 {
        self.fd
    }

    pub fn vectored_write<'a>(&self, bufs: &'a [IoSlice<'a>]) -> WriteVectoredSocket<'a> {
        socket_send_vectored(self, bufs)
    }

    pub fn send_control_record<'a>(
        &self,
        record_type: u8,
        payload: &'a [u8],
    ) -> WriteControlRecord<'a> {
        socket_send_control_record(self, record_type, payload)
    }

    pub fn set_nodelay(&self, on: bool) -> io::Result<()> {
        self.set_opt(libc::IPPROTO_TCP, libc::TCP_NODELAY, &(on as c_int))
    }

    pub fn set_keepalive(&self, on: bool) -> io::Result<()> {
        self.set_opt(libc::SOL_SOCKET, libc::SO_KEEPALIVE, &(on as c_int))
    }

    pub fn set_reuseaddr(&self, on: bool) -> io::Result<()> {
        self.set_opt(libc::SOL_SOCKET, libc::SO_REUSEADDR, &(on as c_int))
    }

    pub fn set_reuseport(&self, on: bool) -> io::Result<()> {
        self.set_opt(libc::SOL_SOCKET, libc::SO_REUSEPORT, &(on as c_int))
    }

    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        self.set_opt(libc::SOL_SOCKET, libc::SO_RCVBUF, &(size as c_int))
    }

    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        self.set_opt(libc::SOL_SOCKET, libc::SO_SNDBUF, &(size as c_int))
    }

    pub fn set_defer_accept(&self, secs: u32) -> io::Result<()> {
        self.set_opt(libc::IPPROTO_TCP, libc::TCP_DEFER_ACCEPT, &(secs as c_int))
    }

    pub fn set_fastopen(&self, qlen: u32) -> io::Result<()> {
        self.set_opt(libc::IPPROTO_TCP, libc::TCP_FASTOPEN, &(qlen as c_int))
    }

    pub fn set_quickack(&self, on: bool) -> io::Result<()> {
        self.set_opt(libc::IPPROTO_TCP, libc::TCP_QUICKACK, &(on as c_int))
    }

    pub fn set_linger(&self, dur: Option<Duration>) -> io::Result<()> {
        let l = match dur {
            Some(d) => libc::linger {
                l_onoff: 1,
                l_linger: d.as_secs().min(c_int::MAX as u64) as c_int,
            },
            None => libc::linger { l_onoff: 0, l_linger: 0 },
        };
        self.set_opt(libc::SOL_SOCKET, libc::SO_LINGER, &l)
    }

    /// Local end of the connection (`getsockname`). One syscall per call.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        getname(|sa, len| unsafe { libc::getsockname(self.fd, sa, len) })
    }

    /// Remote end of the connection (`getpeername`). One syscall per call.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        getname(|sa, len| unsafe { libc::getpeername(self.fd, sa, len) })
    }

    fn set_opt<T>(&self, level: c_int, name: c_int, value: &T) -> io::Result<()> {
        let ret = unsafe {
            libc::setsockopt(
                self.fd,
                level,
                name,
                value as *const T as *const libc::c_void,
                std::mem::size_of::<T>() as libc::socklen_t,
            )
        };
        if ret < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

impl SocketWrapper {
    pub fn sync_shutdown(self) {
        unsafe {
            libc::shutdown(self.fd, libc::SHUT_RDWR);
        }
        // self is dropped here, which calls close(fd)
    }
}

impl Drop for SocketWrapper {
    fn drop(&mut self) {
        unsafe {
            let fd = self.fd as c_int;
            libc::close(fd); //synchronous close to avoid fd conflicts
        }
    }
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
