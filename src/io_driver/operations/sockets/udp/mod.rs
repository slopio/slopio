mod recv;
mod send;

pub use recv::{UdpPacket, UdpRecvFrom};
pub use send::UdpSendTo;

use std::{
    mem::MaybeUninit,
    net::{Ipv4Addr, SocketAddr},
    os::fd::IntoRawFd,
};

use socket2::{Domain, Protocol, Socket as S, Type};

use crate::io_driver::operations::sockets::SocketWrapper;

pub struct UdpSocket {
    socket: SocketWrapper,
}

impl UdpSocket {
    pub fn bind(port: u16) -> std::io::Result<Self> {
        let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, port));
        let socket = S::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
        socket.set_reuse_port(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;
        let fd = socket.into_raw_fd();

        Ok(Self {
            socket: SocketWrapper { fd },
        })
    }

    pub fn fd(&self) -> i32 {
        self.socket.fd()
    }

    pub fn recv_from<'a>(&self, buffer: &'a mut [MaybeUninit<u8>]) -> UdpRecvFrom<'a> {
        UdpRecvFrom::new(self.socket.fd(), buffer)
    }

    pub fn send_to<'a>(&self, buffer: &'a [u8], dest: SocketAddr) -> UdpSendTo<'a> {
        UdpSendTo::new(self.socket.fd(), buffer, dest)
    }
}
