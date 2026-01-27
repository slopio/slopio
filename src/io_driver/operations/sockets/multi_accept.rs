use std::{
    ffi::c_int,
    net::{Ipv4Addr, SocketAddr},
    os::fd::IntoRawFd,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode, types::Fd};
use runtime_streams::Stream;
use socket2::{Domain, Protocol, Socket as S, Type};

use crate::io_driver::{
    IO_DRIVER, MultiOpQueue, MultiPollResult, MultishotCompletionEvent,
    operations::{IoOP, sockets::SocketWrapper},
};

pub struct MultiAccept {
    id: Option<usize>,
    fd: i32,
    sockets: MultiOpQueue,
}

#[derive(Debug)]
pub enum AcceptError {
    Cancelled,
    Other(i32),
}

impl AcceptError {
    fn from_result(result: i32) -> Self {
        if result == -libc::ECANCELED {
            AcceptError::Cancelled
        } else {
            AcceptError::Other(-result)
        }
    }

    /// Returns true if this error is recoverable and we should continue accepting.
    pub fn can_continue(&self) -> bool {
        matches!(self, AcceptError::Cancelled)
    }
}

impl TryFrom<MultishotCompletionEvent> for SocketWrapper {
    type Error = AcceptError;

    fn try_from(value: MultishotCompletionEvent) -> Result<Self, Self::Error> {
        if value.result < 0 {
            Err(AcceptError::from_result(value.result))
        } else {
            Ok(SocketWrapper { fd: value.result })
        }
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

impl MultiAccept {
    /// Try to drain one valid socket from the queue, skipping errors.
    fn try_drain_one(&mut self) -> Poll<Option<SocketWrapper>> {
        while let Some(event) = self.sockets.pop() {
            match event.try_into() {
                Ok(socket) => return Poll::Ready(Some(socket)),
                Err(e) if e.can_continue() => continue,
                Err(_) => return Poll::Ready(None),
            }
        }
        Poll::Pending
    }
}

impl Stream for MultiAccept {
    type Item = SocketWrapper;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(result) = self.try_drain_one() {
            return Poll::Ready(result);
        }

        let waker = cx.waker();
        let id = self.id.unwrap();

        match IO_DRIVER.with(|d| d.poll_op_multishot(id, waker, &mut self.sockets)) {
            MultiPollResult::Pending => Poll::Pending,
            MultiPollResult::Ready => self.try_drain_one(),
            MultiPollResult::NeedsResubmit => {
                let entry = self.to_entry();
                IO_DRIVER.with(|d| d.resubmit_multishot(id, entry, waker));
                Poll::Pending
            }
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
