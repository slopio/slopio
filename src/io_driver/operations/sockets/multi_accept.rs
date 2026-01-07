use std::{
    ffi::c_int,
    net::TcpListener,
    os::fd::IntoRawFd,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode, types::Fd};

use crate::io_driver::{
    IO_DRIVER, MultishotCompletionEvent,
    operations::{IoOP, Stream, sockets::Socket},
};

pub struct MultiAccept {
    id: Option<usize>,
    fd: i32,
    sockets: Vec<MultishotCompletionEvent>,
}

impl From<MultishotCompletionEvent> for Socket {
    fn from(value: MultishotCompletionEvent) -> Self {
        Socket { fd: value.fd }
    }
}

impl MultiAccept {
    pub fn new(port: u16) -> std::io::Result<Self> {
        let tcp_listener = TcpListener::bind(("0.0.0.0", port))?;
        tcp_listener.set_nonblocking(true)?;
        let fd = tcp_listener.into_raw_fd();
        let mut multi_accept = Self {
            id: None,
            fd,
            sockets: Vec::new(),
        };
        multi_accept.register_to_driver();
        Ok(multi_accept)
    }
}

impl Drop for MultiAccept {
    fn drop(&mut self) {
        self.free_driver();
    }
}
impl Stream for MultiAccept {
    type Item = Socket; // ou Result<TcpStream, Error>

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let waker = cx.waker();
        let id = self.id.unwrap();
        if let Some(event) = self.sockets.pop() {
            return Poll::Ready(Some(event.into()));
        }
        match IO_DRIVER.with(|d| d.poll_op_multishot(id, waker, &mut self.sockets)) {
            std::task::Poll::Pending => {
                return std::task::Poll::Pending;
            }
            std::task::Poll::Ready(_) => {}
        }
        if let Some(fd) = self.sockets.pop() {
            Poll::Ready(Some(fd.into()))
        } else {
            Poll::Pending
        }
    }
}

impl IoOP for MultiAccept {
    fn register_to_driver(&mut self) -> crate::io_driver::operations::IoDriverToken {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| d.push_multishot_op(entry));
        self.id = Some(token as usize);
        token
    }

    fn to_entry(&self) -> io_uring::squeue::Entry {
        opcode::AcceptMulti::new(Fd(self.fd))
            .flags(libc::SOCK_NONBLOCK)
            .build()
    }

    fn free_driver(&mut self) {
        unsafe {
            let _fd = self.fd;
            libc::close(self.fd as c_int);
        }
    }
}
