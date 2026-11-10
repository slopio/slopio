use crate::streams::Stream;
use io_uring::{cqueue::buffer_select, opcode, types::Fd};
use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use crate::io_driver::{
    BorrowedBuffer, BufRingSpec, DEFAULT_QUEUE_SIZE, IO_DRIVER, MultiOpQueue, MultiPollResult,
    MultishotCompletionEvent,
    operations::{
        IoOP, MultishotToken,
        sockets::{SocketWrapper, split::SocketOwner},
    },
};

/// Owns its socket handle for as long as it streams: the fd cannot close
/// under the armed multishot, and a second stream cannot be armed on the same
/// socket. The handle is a whole [`SocketWrapper`], or an
/// [`OwnedReadHalf`](super::split::OwnedReadHalf) when the write half lives
/// in another task. Send on the connection through [`MultiRecv::socket`].
/// To read the connection another way, [`stop`](MultiRecv::stop) the stream,
/// drain it, then take the handle back with [`MultiRecv::into_inner`].
/// Dropping the stream cancels the multishot and then drops the handle, in
/// that order.
pub struct MultiRecv<
    S: BufRingSpec,
    O: SocketOwner = SocketWrapper,
    const QUEUE_SIZE: usize = DEFAULT_QUEUE_SIZE,
> {
    id: Option<MultishotToken>,
    /// `Some` until `into_inner` moves the socket handle out.
    socket: Option<O>,
    reads: MultiOpQueue,
    /// Completions that may wait to be polled before the multishot pauses.
    queue_limit: usize,
    /// EOF was yielded: every later poll answers `None` without re-arming.
    ended: bool,
    /// `stop` was called: the stream ends where it would have re-armed.
    stopping: bool,
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

pub use crate::io_driver::operations::Continuation;

#[allow(unreachable_patterns)]
impl CompletionError {
    pub fn continuation(&self) -> Continuation {
        match self {
            Self::Eof { .. } => Continuation::NotContinue,
            Self::ConnectionReset => Continuation::NotContinue,
            Self::BrokenPipe => Continuation::NotContinue,
            // Pausing works by cancelling, so this is the driver's own
            // backpressure coming back to be re-armed.
            Self::Cancelled => Continuation::Continue,
            Self::Other(code) => match -code {
                libc::EINTR | libc::EAGAIN | libc::EWOULDBLOCK => Continuation::Continue,
                // The ring was empty. It refills only when another holder
                // recycles, which no poll of this stream can bring about.
                libc::ENOBUFS => Continuation::ContinueLater,
                _ => Continuation::NotContinue,
            },
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

impl<S: BufRingSpec, O: SocketOwner> MultiRecv<S, O, DEFAULT_QUEUE_SIZE> {
    pub fn new(socket: O) -> Self {
        Self::with_queue_size(socket)
    }

    /// [`with_queue_size`](Self::with_queue_size), with the bound chosen at
    /// run time. Each completion waiting to be polled holds a buffer of the
    /// ring every stream on the worker shares, however few bytes it carries;
    /// past `limit` of them the multishot pauses until the stream is polled
    /// again, and the peer's bytes wait in the socket, for TCP to push back
    /// with. So a consumer that stops polling holds about `limit` buffers.
    pub fn with_queue_limit(socket: O, limit: usize) -> Self {
        assert!(
            limit > 0,
            "a queue limit of zero would pause on every completion"
        );
        Self::bounded(socket, limit)
    }
}

impl<S: BufRingSpec, O: SocketOwner, const QUEUE_SIZE: usize> MultiRecv<S, O, QUEUE_SIZE> {
    pub fn with_queue_size(socket: O) -> Self {
        Self::bounded(socket, QUEUE_SIZE)
    }

    fn bounded(socket: O, queue_limit: usize) -> Self {
        Self {
            id: None,
            socket: Some(socket),
            reads: MultiOpQueue::new(queue_limit),
            queue_limit,
            ended: false,
            stopping: false,
            _spec: PhantomData,
        }
    }

    // Armed by the first poll, which also leaves `Init`: no CQE can land in between.
    fn ensure_registered(&mut self) {
        if self.id.is_none() {
            self.register_to_driver();
        }
    }

    /// The socket this stream reads from — the write half of the connection,
    /// and socket options.
    pub fn socket(&self) -> &SocketWrapper {
        self.socket.as_ref().expect("socket present").socket()
    }

    /// Stop reading from the socket without losing what was already read.
    ///
    /// The stream then yields every chunk the kernel had taken off the socket,
    /// in order, and ends with `None`; nothing more is read after that, and
    /// what the peer sends later waits in the socket. Once it has answered
    /// `None`, [`into_inner`](Self::into_inner) loses nothing:
    ///
    /// ```ignore
    /// stream.stop();
    /// while let Some(chunk) = stream.next().await {
    ///     rest.push(chunk?);
    /// }
    /// let socket = stream.into_inner();
    /// ```
    ///
    /// A fatal error is still reported once before the end. `None` does not
    /// tell a stopped stream from a closed peer: the next read on the socket
    /// does.
    pub fn stop(&mut self) {
        if self.stopping || self.ended {
            return;
        }
        self.stopping = true;
        // Never polled: nothing is armed, the next poll ends the stream.
        if let Some(token) = &self.id {
            IO_DRIVER.with(|d| d.stop_multishot(token));
        }
    }

    /// Take the socket handle back. Cancels the multishot the same way `Drop`
    /// does, and like `Drop` discards every chunk the stream had not yielded
    /// yet, which leaves a hole in what the socket reads next. To keep them,
    /// call [`stop`](Self::stop) and drain the stream to `None` first.
    pub fn into_inner(mut self) -> O {
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
            if let Some(bid) = buffer_select(event.flags) {
                S::recycle_bid(bid);
            }
            return Err(CompletionError::from_code(event.result, event.flags));
        }
        let len = event.result as usize;
        let buffer_id = buffer_select(event.flags).unwrap();
        Ok(S::make_buffer(buffer_id, len))
    }
}

impl<S: BufRingSpec, O: SocketOwner, const QUEUE_SIZE: usize> Drop for MultiRecv<S, O, QUEUE_SIZE> {
    fn drop(&mut self) {
        // Recycle kernel buffers from any unprocessed completion events
        // before cancelling the op, otherwise these buffer ring entries leak.
        self.recycle_pending_buffers();
        self.driver_cleanup();
    }
}

impl<S: BufRingSpec, O: SocketOwner, const QUEUE_SIZE: usize> MultiRecv<S, O, QUEUE_SIZE> {
    fn recycle_pending_buffers(&mut self) {
        while let Some(event) = self.reads.pop() {
            // The flag alone says a buffer was consumed, whatever the result.
            if let Some(buffer_id) = buffer_select(event.flags) {
                S::recycle_bid(buffer_id);
            }
        }
    }
}

impl<S: BufRingSpec, O: SocketOwner, const QUEUE_SIZE: usize> Unpin
    for MultiRecv<S, O, QUEUE_SIZE>
{
}

impl<S: BufRingSpec, O: SocketOwner, const QUEUE_SIZE: usize> MultiRecv<S, O, QUEUE_SIZE> {
    fn try_drain_one(&mut self) -> Poll<Option<Result<BorrowedBuffer<S>, CompletionError>>> {
        while let Some(completion_event) = self.reads.pop() {
            match self.try_parse(completion_event) {
                Ok(buf) => return Poll::Ready(Some(Ok(buf))),
                Err(CompletionError::Eof { .. }) => {
                    self.ended = true;
                    return Poll::Ready(None);
                }
                Err(e) => match e.continuation() {
                    Continuation::Continue => continue,
                    // A stopped stream will not re-arm: nothing to come back for.
                    Continuation::ContinueLater if self.stopping => continue,
                    // Armable still: the next poll re-arms, so the consumer
                    // decides when to come back rather than spinning here.
                    Continuation::ContinueLater => return Poll::Ready(Some(Err(e))),
                    // Reported once, then the stream is over.
                    Continuation::NotContinue => {
                        self.ended = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                },
            }
        }
        Poll::Pending
    }
}

impl<S: BufRingSpec, O: SocketOwner, const QUEUE_SIZE: usize> Stream
    for MultiRecv<S, O, QUEUE_SIZE>
{
    type Item = Result<BorrowedBuffer<S>, CompletionError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        if let Poll::Ready(result) = self.try_drain_one() {
            return Poll::Ready(result);
        }
        // Stopped before the first poll: nothing was ever armed.
        if self.stopping && self.id.is_none() {
            self.ended = true;
            return Poll::Ready(None);
        }

        self.ensure_registered();
        let waker = cx.waker();
        let this = &mut *self;

        loop {
            match IO_DRIVER.with(|d| {
                d.poll_op_multishot(
                    this.id.as_ref().expect("registered"),
                    waker,
                    &mut this.reads,
                )
            }) {
                MultiPollResult::Pending => return Poll::Pending,
                // Only skippable errors came out: ask again, the op may be over.
                MultiPollResult::Ready => {
                    if let Poll::Ready(result) = this.try_drain_one() {
                        return Poll::Ready(result);
                    }
                }
                MultiPollResult::NeedsResubmit => {
                    // The paused queue came with the swap, and nothing will wake us for it.
                    if let Poll::Ready(result) = this.try_drain_one() {
                        return Poll::Ready(result);
                    }
                    // The kernel op is over and drained: a stopped stream ends here.
                    if this.stopping {
                        this.ended = true;
                        return Poll::Ready(None);
                    }
                    let entry = this.to_entry();
                    IO_DRIVER.with(|d| {
                        d.resubmit_multishot(this.id.as_ref().expect("registered"), entry, waker)
                    });
                    return Poll::Pending;
                }
            }
        }
    }
}

impl<S: BufRingSpec, O: SocketOwner, const QUEUE_SIZE: usize> IoOP for MultiRecv<S, O, QUEUE_SIZE> {
    fn register_to_driver(&mut self) {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| {
            d.push_buffered_multishot_op(entry, self.socket().fd(), self.queue_limit, S::BGID)
        });
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
