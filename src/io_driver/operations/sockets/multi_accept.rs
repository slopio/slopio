use std::{
    collections::VecDeque,
    ffi::c_int,
    net::{Ipv4Addr, SocketAddr},
    os::fd::IntoRawFd,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode, types::Fd};
use socket2::{Domain, Protocol, Socket as S, Type};

use crate::io_driver::{
    IO_DRIVER, MultishotCompletionEvent,
    operations::{IoOP, Stream, sockets::SocketWrapper},
};

pub struct MultiAccept {
    id: Option<usize>,
    fd: i32,
    sockets: VecDeque<MultishotCompletionEvent>,
}

impl From<MultishotCompletionEvent> for SocketWrapper {
    fn from(value: MultishotCompletionEvent) -> Self {
        SocketWrapper { fd: value.result }
    }
}

impl MultiAccept {
    pub fn new(port: u16) -> std::io::Result<Self> {
        let addr = SocketAddr::from((Ipv4Addr::UNSPECIFIED, port));
        let socket = S::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
        socket.set_reuse_port(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&addr.into())?;
        socket.listen(1024)?;
        let fd = socket.into_raw_fd();

        let mut multi_accept = Self {
            id: None,
            fd,
            sockets: Default::default(),
        };
        multi_accept.register_to_driver();
        Ok(multi_accept)
    }
}

impl Drop for MultiAccept {
    fn drop(&mut self) {
        self.driver_cleanup();
    }
}

impl Unpin for MultiAccept {}

impl Stream for MultiAccept {
    type Item = SocketWrapper; // ou Result<TcpStream, Error>

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let waker = cx.waker();
        let id = self.id.unwrap();
        if let Some(event) = self.sockets.pop_front() {
            return Poll::Ready(Some(event.into()));
        }
        if IO_DRIVER
            .with(|d| d.poll_op_multishot(id, waker, &mut self.sockets))
            .is_pending()
        {
            return std::task::Poll::Pending;
        }
        if let Some(fd) = self.sockets.pop_front() {
            Poll::Ready(Some(fd.into()))
        } else {
            Poll::Pending
        }
    }
}

impl IoOP for MultiAccept {
    fn register_to_driver(&mut self) -> crate::io_driver::operations::IoDriverToken {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| d.push_multishot_op(entry, self.fd));
        self.id = Some(token as usize);
        token
    }

    fn to_entry(&self) -> io_uring::squeue::Entry {
        opcode::AcceptMulti::new(Fd(self.fd))
            .flags(libc::SOCK_NONBLOCK)
            .build()
    }

    fn driver_cleanup(&mut self) {
        unsafe {
            libc::close(self.fd as c_int);
        }
    }
}
