use io_uring::{cqueue::buffer_select, opcode, types::Fd};
use runtime_streams::Stream;
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::io_driver::{
    BorrowedBuffer, BufRingSpec, DEFAULT_QUEUE_SIZE, IO_DRIVER, MultiOpQueue, MultiPollResult,
    MultishotCompletionEvent,
    operations::{IoOP, MultishotToken, sockets::SocketWrapper},
};

/// Owns its socket for as long as it streams: the fd cannot close under the
/// armed multishot, and a second stream cannot be armed on the same socket.
/// Send on the connection through [`MultiRecv::socket`]; take the socket back
/// with [`MultiRecv::into_inner`]. Dropping the stream cancels the multishot
/// and then closes the socket, in that order.
pub struct MultiRecv<S: BufRingSpec, const QUEUE_SIZE: usize = DEFAULT_QUEUE_SIZE> {
    id: Option<MultishotToken>,
    /// `Some` until `into_inner` moves the socket out.
    socket: Option<SocketWrapper>,
    reads: MultiOpQueue,
    _spec: PhantomData<S>,
}

#[derive(Debug)]
pub enum CompletionError {
    Eof { flags: u32 },
    ConnectionReset,
    BrokenPipe,
    Cancelled,
    Other(i32),
}

#[allow(unreachable_patterns)]
impl CompletionError {
    pub fn can_continue(&self) -> bool {
        match self {
            Self::Eof { .. } => false,
            Self::ConnectionReset => false,
            Self::BrokenPipe => false,
            Self::Cancelled => true, // Cancellation is recoverable - we'll resubmit
            Self::Other(code) => matches!(-code, libc::EINTR | libc::EAGAIN | libc::EWOULDBLOCK),
        }
    }

    pub fn from_code(result: i32, flags: u32) -> Self {
        match result {
            0 => Self::Eof { flags },
            r if r == -libc::ECONNRESET => Self::ConnectionReset,
            r if r == -libc::EPIPE => Self::BrokenPipe,
            r if r == -libc::ECANCELED => Self::Cancelled,
            _ => Self::Other(result),
        }
    }
}

impl<S: BufRingSpec> MultiRecv<S, DEFAULT_QUEUE_SIZE> {
    pub fn new(socket: SocketWrapper) -> Self {
        Self::with_queue_size(socket)
    }
}

impl<S: BufRingSpec, const QUEUE_SIZE: usize> MultiRecv<S, QUEUE_SIZE> {
    pub fn with_queue_size(socket: SocketWrapper) -> Self {
        let mut multi_recv = Self {
            id: None,
            socket: Some(socket),
            reads: MultiOpQueue::new(QUEUE_SIZE),
            _spec: PhantomData,
        };
        multi_recv.register_to_driver();
        multi_recv
    }

    /// The socket this stream reads from — the write half of the connection,
    /// and socket options.
    pub fn socket(&self) -> &SocketWrapper {
        self.socket.as_ref().expect("socket present")
    }

    /// Stop streaming and take the socket back. Cancels the multishot the
    /// same way `Drop` does; the driver absorbs the cancelled op's final
    /// CQEs and recycles their buffers.
    pub fn into_inner(mut self) -> SocketWrapper {
        self.recycle_pending_buffers();
        self.driver_cleanup();
        // Drop then finds `id` and `socket` both empty and does nothing.
        self.socket.take().expect("socket present")
    }

    fn try_parse(
        &self,
        event: MultishotCompletionEvent,
    ) -> Result<BorrowedBuffer<S>, CompletionError> {
        if event.result <= 0 {
            return Err(CompletionError::from_code(event.result, event.flags));
        }
        let len = event.result as usize;
        let buffer_id = buffer_select(event.flags).unwrap();
        Ok(S::make_buffer(buffer_id, len))
    }
}

impl<S: BufRingSpec, const QUEUE_SIZE: usize> Drop for MultiRecv<S, QUEUE_SIZE> {
    fn drop(&mut self) {
        // Recycle kernel buffers from any unprocessed completion events
        // before cancelling the op, otherwise these buffer ring entries leak.
        self.recycle_pending_buffers();
        self.driver_cleanup();
    }
}

impl<S: BufRingSpec, const QUEUE_SIZE: usize> MultiRecv<S, QUEUE_SIZE> {
    fn recycle_pending_buffers(&mut self) {
        while let Some(event) = self.reads.pop() {
            if event.result > 0 {
                if let Some(buffer_id) = buffer_select(event.flags) {
                    S::recycle_bid(buffer_id);
                }
            }
        }
    }
}

impl<S: BufRingSpec, const QUEUE_SIZE: usize> Unpin for MultiRecv<S, QUEUE_SIZE> {}

impl<S: BufRingSpec, const QUEUE_SIZE: usize> MultiRecv<S, QUEUE_SIZE> {
    fn try_drain_one(&mut self) -> Poll<Option<Result<BorrowedBuffer<S>, CompletionError>>> {
        while let Some(completion_event) = self.reads.pop() {
            match self.try_parse(completion_event) {
                Ok(buf) => return Poll::Ready(Some(Ok(buf))),
                Err(CompletionError::Eof { .. }) => return Poll::Ready(None),
                Err(e) if e.can_continue() => continue,
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
        }
        Poll::Pending
    }
}

impl<S: BufRingSpec, const QUEUE_SIZE: usize> Stream for MultiRecv<S, QUEUE_SIZE> {
    type Item = Result<BorrowedBuffer<S>, CompletionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(result) = self.try_drain_one() {
            return Poll::Ready(result);
        }

        let waker = cx.waker();
        let token = self.id.unwrap();

        match IO_DRIVER.with(|d| d.poll_op_multishot(token, waker, &mut self.reads)) {
            MultiPollResult::Pending => Poll::Pending,
            MultiPollResult::Ready => {
                match self.try_drain_one() {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => {
                        // Queue had only skippable errors, check if we need resubmit
                        if let MultiPollResult::NeedsResubmit =
                            IO_DRIVER.with(|d| d.poll_op_multishot(token, waker, &mut self.reads))
                        {

                            let entry = self.to_entry();
                            IO_DRIVER.with(|d| d.resubmit_multishot(token, entry, waker));
                        }
                        Poll::Pending
                    }
                }
            }
            MultiPollResult::NeedsResubmit => {

                let entry = self.to_entry();
                IO_DRIVER.with(|d| d.resubmit_multishot(token, entry, waker));
                Poll::Pending
            }
        }
    }
}

impl<S: BufRingSpec, const QUEUE_SIZE: usize> IoOP for MultiRecv<S, QUEUE_SIZE> {
    fn register_to_driver(&mut self) {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| d.push_multishot_op(entry, self.socket().fd(), QUEUE_SIZE));
        self.id = Some(token);
    }

    fn to_entry(&self) -> io_uring::squeue::Entry {
        opcode::RecvMulti::new(Fd(self.socket().fd()), S::BGID).build()
    }

    fn driver_cleanup(&mut self) {
        if let Some(token) = self.id.take() {
            // Park the fd ref so a Drop-path close posts only after the
            // cancel's terminal CQE.
            let parked = Some(self.socket().park());
            IO_DRIVER.with(|d| d.cancel_multishot_op_keep(token, S::BGID, None, parked));
        }
    }
}
