use libc::c_int;
use std::net::SocketAddr;

pub mod managed_recv;
pub mod managed_recv_bundle;
pub mod multi_accept;
pub mod multi_recv;
pub mod recv;
pub mod send;
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
