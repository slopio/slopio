use io_uring::opcode;
use io_uring::types::Fd;
use libc::{sockaddr_in, sockaddr_in6, sockaddr_storage, socklen_t};
use std::future::Future;
use std::io;
use std::mem;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::io_driver::IO_DRIVER;
use crate::io_driver::operations::OneshotToken;

use super::SocketWrapper;

pub fn connect_tcp(addr: SocketAddr) -> ConnectTcp {
    ConnectTcp {
        addr,
        socket: None,
        addr_storage: None,
        token: None,
    }
}

struct AddrStorage {
    sa: sockaddr_storage,
    len: socklen_t,
}

pub struct ConnectTcp {
    addr: SocketAddr,
    socket: Option<SocketWrapper>,
    addr_storage: Option<Box<AddrStorage>>,
    token: Option<OneshotToken>,
}

impl ConnectTcp {
    fn submit(&mut self) -> io::Result<()> {
        let af = match self.addr {
            SocketAddr::V4(_) => libc::AF_INET,
            SocketAddr::V6(_) => libc::AF_INET6,
        };
        let fd = unsafe { libc::socket(af, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        self.socket = Some(SocketWrapper::from_fd(fd));

        let mut storage: Box<AddrStorage> = Box::new(AddrStorage {
            sa: unsafe { mem::zeroed() },
            len: 0,
        });
        let dst = &mut storage.sa as *mut sockaddr_storage as *mut u8;
        match self.addr {
            SocketAddr::V4(v4) => {
                let sin = sockaddr_in {
                    sin_family: libc::AF_INET as libc::sa_family_t,
                    sin_port: v4.port().to_be(),
                    sin_addr: libc::in_addr {
                        s_addr: u32::from_ne_bytes(v4.ip().octets()),
                    },
                    sin_zero: [0; 8],
                };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        &sin as *const sockaddr_in as *const u8,
                        dst,
                        mem::size_of::<sockaddr_in>(),
                    );
                }
                storage.len = mem::size_of::<sockaddr_in>() as socklen_t;
            }
            SocketAddr::V6(v6) => {
                let sin6 = sockaddr_in6 {
                    sin6_family: libc::AF_INET6 as libc::sa_family_t,
                    sin6_port: v6.port().to_be(),
                    sin6_flowinfo: v6.flowinfo(),
                    sin6_addr: libc::in6_addr {
                        s6_addr: v6.ip().octets(),
                    },
                    sin6_scope_id: v6.scope_id(),
                };
                unsafe {
                    std::ptr::copy_nonoverlapping(
                        &sin6 as *const sockaddr_in6 as *const u8,
                        dst,
                        mem::size_of::<sockaddr_in6>(),
                    );
                }
                storage.len = mem::size_of::<sockaddr_in6>() as socklen_t;
            }
        }

        let entry = opcode::Connect::new(
            Fd(fd),
            &storage.sa as *const sockaddr_storage as *const libc::sockaddr,
            storage.len,
        )
        .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.token = Some(token);
        self.addr_storage = Some(storage);
        Ok(())
    }
}

impl Future for ConnectTcp {
    type Output = io::Result<SocketWrapper>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.token.is_none() {
            if let Err(e) = self.submit() {
                return Poll::Ready(Err(e));
            }
        }
        let token = self.token.unwrap();
        match IO_DRIVER.with(|d| d.poll_op(token, cx.waker())) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(meta) => {
                self.token = None;
                let _ = self.addr_storage.take();
                if meta.result < 0 {
                    let _ = self.socket.take();
                    Poll::Ready(Err(io::Error::from_raw_os_error(-meta.result)))
                } else {
                    Poll::Ready(Ok(self.socket.take().unwrap()))
                }
            }
        }
    }
}

impl Drop for ConnectTcp {
    fn drop(&mut self) {
        if let Some(token) = self.token {
            let keep = self
                .addr_storage
                .take()
                .map(|b| b as Box<dyn std::any::Any>);
            let parked = self.socket.take().map(|s| s.into_parked());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep, parked));
        }
    }
}
