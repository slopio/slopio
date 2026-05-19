use libc::c_int;
use std::io::{self, IoSlice};
use std::net::SocketAddr;
use std::time::Duration;

use self::send_control_record::{socket_send_control_record, WriteControlRecord};
use self::send_vectored::{socket_send_vectored, WriteVectoredSocket};

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

pub fn connect_tcp(addr: SocketAddr) -> std::io::Result<SocketWrapper> {
    let fd = unsafe {
        let af = match addr {
            SocketAddr::V4(_) => libc::AF_INET,
            SocketAddr::V6(_) => libc::AF_INET6,
        };
        let fd = libc::socket(af, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
        if fd < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let ret = match addr {
            SocketAddr::V4(v4) => {
                let sin = libc::sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: v4.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(v4.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };
                libc::connect(
                    fd,
                    &sin as *const libc::sockaddr_in as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
                )
            }
            SocketAddr::V6(v6) => {
                let sin6 = libc::sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: v6.port().to_be(),
                    sin6_flowinfo: v6.flowinfo(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: v6.ip().octets(),
                    },
                    sin6_scope_id: v6.scope_id(),
                };
                libc::connect(
                    fd,
                    &sin6 as *const libc::sockaddr_in6 as *const libc::sockaddr,
                    std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t,
                )
            }
        };
        if ret < 0 {
            libc::close(fd);
            return Err(std::io::Error::last_os_error());
        }
        fd
    };
    Ok(SocketWrapper { fd })
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
