mod multi_recv;
mod recv;
mod send;
mod split;

pub use multi_recv::{UdpDatagram, UdpMultiRecv};
pub use recv::{UdpPacket, UdpRecvFrom};
pub use send::UdpSendTo;
pub use split::{UdpRecvHalf, UdpReuniteError, UdpSendHalf, UdpSocketOwner};

use std::{
    net::{Ipv4Addr, Ipv6Addr, SocketAddr},
    os::fd::IntoRawFd,
};

use socket2::{Domain, Protocol, Socket as S, Type};

use crate::io_driver::ParkedFd;
use crate::io_driver::operations::sockets::SocketWrapper;
use crate::io_driver::operations::{IoBuf, IoBufMut};

#[derive(Debug)]
pub struct UdpSocket {
    socket: SocketWrapper,
}

impl UdpSocket {
    pub fn bind(port: u16) -> std::io::Result<Self> {
        Self::bind_addr(SocketAddr::from((Ipv4Addr::UNSPECIFIED, port)))
    }

    pub fn bind_v6(port: u16) -> std::io::Result<Self> {
        Self::bind_addr(SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)))
    }

    pub fn bind_addr(addr: SocketAddr) -> std::io::Result<Self> {
        let fd = match crate::io_driver::sys::simulated_bound_socket(addr) {
            Some(fd) => fd,
            None => {
                let domain = match addr {
                    SocketAddr::V4(_) => Domain::IPV4,
                    SocketAddr::V6(_) => Domain::IPV6,
                };
                let socket = S::new(domain, Type::DGRAM, Some(Protocol::UDP))?;
                socket.set_reuse_port(true)?;
                socket.set_nonblocking(true)?;
                socket.bind(&addr.into())?;
                socket.into_raw_fd()
            }
        };

        Ok(Self {
            socket: SocketWrapper::from_fd(fd),
        })
    }

    pub fn fd(&self) -> i32 {
        self.socket.fd()
    }

    pub(crate) fn park(&self) -> ParkedFd {
        self.socket.park()
    }

    pub fn recv_from<B: IoBufMut>(&self, buf: B) -> UdpRecvFrom<B> {
        UdpRecvFrom::new(self.socket.share(), buf)
    }

    pub fn send_to<B: IoBuf>(&self, buf: B, dest: SocketAddr) -> UdpSendTo<B> {
        UdpSendTo::new(self.socket.share(), buf, dest)
    }
}
