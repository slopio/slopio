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

/// Connect to `addr`. Defaults: non-blocking fd, `TCP_NODELAY` on. Both are
/// adjustable through the builder methods before the first `.await`.
pub fn connect_tcp(addr: SocketAddr) -> ConnectTcp {
    ConnectTcp {
        addr,
        nodelay: true,
        nonblocking: true,
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
    nodelay: bool,
    nonblocking: bool,
    socket: Option<SocketWrapper>,
    addr_storage: Option<Box<AddrStorage>>,
    token: Option<OneshotToken>,
}

impl ConnectTcp {
    /// `TCP_NODELAY` on the connected socket. Default: on (no Nagle).
    pub fn nodelay(mut self, on: bool) -> Self {
        self.nodelay = on;
        self
    }

    /// `SOCK_NONBLOCK` on the connected socket. Default: on.
    pub fn nonblocking(mut self, on: bool) -> Self {
        self.nonblocking = on;
        self
    }

    fn submit(&mut self) -> io::Result<()> {
        let mut ty = libc::SOCK_CLOEXEC;
        if self.nonblocking {
            ty |= libc::SOCK_NONBLOCK;
        }
        let fd = crate::io_driver::sys::tcp_socket(&self.addr, ty)?;
        let socket = SocketWrapper::from_fd(fd);
        if self.nodelay {
            socket.set_nodelay(true)?;
        }
        self.socket = Some(socket);

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
        match IO_DRIVER.with(|d| d.poll_op(&mut self.token, cx.waker())) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(meta) => {
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
        if let Some(token) = self.token.take() {
            let keep = self
                .addr_storage
                .take()
                .map(|b| b as Box<dyn std::any::Any>);
            let parked = self.socket.take().map(|s| s.into_parked());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep, parked));
        }
    }
}
