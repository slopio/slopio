use std::{
    marker::PhantomData,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use crate::streams::Stream;
use io_uring::{
    cqueue::buffer_select,
    opcode,
    types::{Fd, RecvMsgOut},
};

use super::{UdpSocket, UdpSocketOwner};
use crate::io_driver::{
    BorrowedBuffer, BufRingSpec, DEFAULT_QUEUE_SIZE, IO_DRIVER, MultiOpQueue, MultiPollResult,
    MultishotCompletionEvent,
    operations::{Continuation, IoOP, MultishotToken, sockets::multi_recv::CompletionError},
    sys,
};

/// Stable storage for the msghdr template, boxed so it survives moves of the
/// owning `UdpMultiRecv`. The kernel writes names into each buffer, not here;
/// only `msg_namelen` is read, and it fits a v4 or v6 peer.
struct MsgHdrBox {
    msghdr: libc::msghdr,
}

impl MsgHdrBox {
    fn new() -> Box<Self> {
        let mut b = Box::new(Self {
            msghdr: unsafe { std::mem::zeroed() },
        });
        b.msghdr.msg_namelen = std::mem::size_of::<libc::sockaddr_in6>() as u32;
        b
    }

    fn msghdr_ptr(&self) -> *const libc::msghdr {
        &self.msghdr as *const _
    }
}

/// Owns its socket handle for as long as it streams, like `MultiRecv` does
/// for TCP. The handle is a whole [`UdpSocket`], or a
/// [`UdpRecvHalf`](super::UdpRecvHalf) when the send half lives in another
/// task. Reply through [`UdpMultiRecv::socket`]'s `send_to`. To receive
/// another way, [`stop`](UdpMultiRecv::stop) the stream, drain it, then take
/// the handle back with [`UdpMultiRecv::into_inner`].
pub struct UdpMultiRecv<
    S: BufRingSpec,
    O: UdpSocketOwner = UdpSocket,
    const QUEUE_SIZE: usize = DEFAULT_QUEUE_SIZE,
> {
    id: Option<MultishotToken>,
    /// `Some` until `into_inner` moves the socket handle out.
    socket: Option<O>,
    reads: MultiOpQueue,
    /// A fatal error was yielded: every later poll answers `None` without re-arming.
    ended: bool,
    /// `stop` was called: the stream ends where it would have re-armed.
    stopping: bool,
    /// `Some` until `driver_cleanup` runs in `Drop`, at which point the Box
    /// is moved into the driver's keepalive slot.
    hdr: Option<Box<MsgHdrBox>>,
    _spec: PhantomData<S>,
}

pub struct UdpDatagram<S: BufRingSpec> {
    buffer: BorrowedBuffer<S>,
    payload_offset: usize,
    payload_len: usize,
    addr: SocketAddr,
    truncated: bool,
}

impl<S: BufRingSpec> UdpDatagram<S> {
    pub fn payload(&self) -> &[u8] {
        let data = self.buffer.as_ref();
        &data[self.payload_offset..self.payload_offset + self.payload_len]
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn len(&self) -> usize {
        self.payload_len
    }

    pub fn is_empty(&self) -> bool {
        self.payload_len == 0
    }

    /// The datagram was larger than the ring's buffers; only `len()` bytes were kept.
    pub fn is_truncated(&self) -> bool {
        self.truncated
    }
}

impl<S: BufRingSpec, O: UdpSocketOwner> UdpMultiRecv<S, O, DEFAULT_QUEUE_SIZE> {
    pub fn new(socket: O) -> Self {
        Self::with_queue_size(socket)
    }
}

impl<S: BufRingSpec, O: UdpSocketOwner, const QUEUE_SIZE: usize> UdpMultiRecv<S, O, QUEUE_SIZE> {
    pub fn with_queue_size(socket: O) -> Self {
        Self {
            id: None,
            socket: Some(socket),
            reads: MultiOpQueue::new(QUEUE_SIZE),
            ended: false,
            stopping: false,
            hdr: Some(MsgHdrBox::new()),
            _spec: PhantomData,
        }
    }

    /// The socket this stream reads from — replies go through its `send_to`.
    pub fn socket(&self) -> &UdpSocket {
        self.socket.as_ref().expect("socket present").socket()
    }

    /// Stop receiving without losing the datagrams already received.
    ///
    /// The stream then yields every datagram the kernel had taken off the
    /// socket, in order, and ends with `None`; nothing more is received after
    /// that, and what arrives later waits in the socket. Once it has answered
    /// `None`, [`into_inner`](Self::into_inner) loses nothing:
    ///
    /// ```ignore
    /// stream.stop();
    /// while let Some(datagram) = stream.next().await {
    ///     rest.push(datagram?);
    /// }
    /// let socket = stream.into_inner();
    /// ```
    ///
    /// A fatal error is still reported once before the end.
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
    /// does, and like `Drop` discards every datagram the stream had not
    /// yielded yet. To keep them, call [`stop`](Self::stop) and drain the
    /// stream to `None` first.
    pub fn into_inner(mut self) -> O {
        self.recycle_pending_buffers();
        self.driver_cleanup();
        // Drop then finds `id` and `socket` both empty and does nothing.
        self.socket.take().expect("socket present")
    }

    fn ensure_registered(&mut self) {
        if self.id.is_none() {
            self.register_to_driver();
        }
    }

    /// `Ok(None)`: a completion that carries no datagram, to be skipped.
    fn try_parse(
        &self,
        event: MultishotCompletionEvent,
    ) -> Result<Option<UdpDatagram<S>>, CompletionError> {
        if event.result <= 0 {
            if let Some(bid) = buffer_select(event.flags) {
                S::recycle_bid(bid);
            }
            if event.result == 0 {
                return Ok(None);
            }
            return Err(CompletionError::from_code(event.result, event.flags));
        }

        let len = event.result as usize;
        let Some(bid) = buffer_select(event.flags) else {
            return Ok(None);
        };
        let buffer = S::make_buffer(bid, len);

        let Ok(parsed) = RecvMsgOut::parse(
            buffer.as_ref(),
            &self.hdr.as_ref().expect("hdr present").msghdr,
        ) else {
            return Ok(None);
        };

        let name = parsed.name_data();
        let mut storage: libc::sockaddr_storage = unsafe { std::mem::zeroed() };
        let copied = name
            .len()
            .min(std::mem::size_of::<libc::sockaddr_storage>());
        unsafe {
            std::ptr::copy_nonoverlapping(name.as_ptr(), &mut storage as *mut _ as *mut u8, copied);
        }
        let Some(addr) = sys::decode_sockaddr(&storage) else {
            return Ok(None);
        };

        let payload = parsed.payload_data();
        let payload_offset = payload.as_ptr() as usize - buffer.as_ref().as_ptr() as usize;
        let payload_len = payload.len();
        let truncated = parsed.is_payload_truncated();

        Ok(Some(UdpDatagram {
            buffer,
            payload_offset,
            payload_len,
            addr,
            truncated,
        }))
    }

    fn try_drain_one(&mut self) -> Poll<Option<Result<UdpDatagram<S>, CompletionError>>> {
        while let Some(event) = self.reads.pop() {
            match self.try_parse(event) {
                Ok(Some(datagram)) => return Poll::Ready(Some(Ok(datagram))),
                Ok(None) => continue,
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

impl<S: BufRingSpec, O: UdpSocketOwner, const QUEUE_SIZE: usize> Drop
    for UdpMultiRecv<S, O, QUEUE_SIZE>
{
    fn drop(&mut self) {
        // Recycle kernel buffers from any unprocessed completion events
        // before cancelling, otherwise buffer-ring entries leak.
        self.recycle_pending_buffers();
        self.driver_cleanup();
    }
}

impl<S: BufRingSpec, O: UdpSocketOwner, const QUEUE_SIZE: usize> UdpMultiRecv<S, O, QUEUE_SIZE> {
    fn recycle_pending_buffers(&mut self) {
        while let Some(event) = self.reads.pop() {
            // The flag alone says a buffer was consumed, whatever the result.
            if let Some(buffer_id) = buffer_select(event.flags) {
                S::recycle_bid(buffer_id);
            }
        }
    }
}

impl<S: BufRingSpec, O: UdpSocketOwner, const QUEUE_SIZE: usize> Unpin
    for UdpMultiRecv<S, O, QUEUE_SIZE>
{
}

impl<S: BufRingSpec, O: UdpSocketOwner, const QUEUE_SIZE: usize> Stream
    for UdpMultiRecv<S, O, QUEUE_SIZE>
{
    type Item = Result<UdpDatagram<S>, CompletionError>;

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

impl<S: BufRingSpec, O: UdpSocketOwner, const QUEUE_SIZE: usize> IoOP
    for UdpMultiRecv<S, O, QUEUE_SIZE>
{
    fn register_to_driver(&mut self) {
        let entry = self.to_entry();
        let token = IO_DRIVER
            .with(|d| d.push_buffered_multishot_op(entry, self.socket().fd(), QUEUE_SIZE, S::BGID));
        self.id = Some(token);
    }

    fn to_entry(&self) -> io_uring::squeue::Entry {
        opcode::RecvMsgMulti::new(
            Fd(self.socket().fd()),
            self.hdr.as_ref().expect("hdr present").msghdr_ptr(),
            S::BGID,
        )
        .build()
    }

    fn driver_cleanup(&mut self) {
        if let Some(token) = self.id.take() {
            // The kernel may still write into msghdr.msg_name → addr for any
            // in-flight datagram before the cancel completes. Hand the Box
            // to the driver as keepalive so it outlives those accesses.
            let keep = self.hdr.take().map(|h| h as Box<dyn std::any::Any>);
            let parked = Some(self.socket().park());
            IO_DRIVER.with(|d| d.cancel_multishot_op_keep(token, S::BGID, keep, parked));
        }
    }
}
