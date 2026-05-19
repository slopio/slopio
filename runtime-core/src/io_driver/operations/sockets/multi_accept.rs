use std::{
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
    completion_events::MULTI_OP_FLAG,
    operations::{IoOP, MultishotToken, sockets::SocketWrapper},
};

pub struct MultiAccept {
    id: Option<MultishotToken>,
    socket: SocketWrapper,
    sockets: MultiOpQueue,
    cancel_sent: bool,
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
            socket: SocketWrapper { fd },
            sockets: Default::default(),
            cancel_sent: false,
        };
        multi_accept.register_to_driver();
        Ok(multi_accept)
    }

    pub fn socket(&self) -> &SocketWrapper {
        &self.socket
    }

    pub fn set_backlog(&self, backlog: i32) -> std::io::Result<()> {
        let ret = unsafe { libc::listen(self.socket.fd, backlog) };
        if ret < 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
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
        let shutting_down = IO_DRIVER.with(|d| d.shutdown());

        // On shutdown, ask io_uring to cancel the multishot accept (once).
        if shutting_down && !self.cancel_sent {
            self.cancel_sent = true;
            let token = self.id.unwrap();
            IO_DRIVER.with(|d| d.send_multiop_cancel_event(token.raw() | MULTI_OP_FLAG));
        }

        if let Poll::Ready(result) = self.try_drain_one() {
            return Poll::Ready(result);
        }

        let waker = cx.waker();
        let token = self.id.unwrap();

        match IO_DRIVER.with(|d| d.poll_op_multishot(token, waker, &mut self.sockets)) {
            MultiPollResult::Pending => Poll::Pending,
            MultiPollResult::Ready => {
                let drained = self.try_drain_one();
                // If shutting down and nothing left, we're done.
                if shutting_down && matches!(drained, Poll::Pending) {
                    return Poll::Ready(None);
                }
                drained
            }
            MultiPollResult::NeedsResubmit => {
                // Drain any remaining sockets queued before the pause.
                if let Poll::Ready(result) = self.try_drain_one() {
                    return Poll::Ready(result);
                }
                // During shutdown, don't resubmit — the stream is done.
                if shutting_down {
                    return Poll::Ready(None);
                }
                let entry = self.to_entry();
                IO_DRIVER.with(|d| d.resubmit_multishot(token, entry, waker));
                Poll::Pending
            }
        }
    }
}

impl IoOP for MultiAccept {
    fn register_to_driver(&mut self) {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| d.push_multishot_op(entry, self.socket.fd));
        self.id = Some(token);
    }

    fn to_entry(&self) -> io_uring::squeue::Entry {
        opcode::AcceptMulti::new(Fd(self.socket.fd))
            .flags(libc::SOCK_NONBLOCK)
            .build()
    }

    fn driver_cleanup(&mut self) {
        // fd is closed by SocketWrapper's Drop
    }
}
