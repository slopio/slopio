use std::{
    marker::PhantomData,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{
    cqueue::buffer_select,
    opcode,
    types::{Fd, RecvMsgOut},
};
use runtime_streams::Stream;

use super::UdpSocket;
use crate::io_driver::{
    BorrowedBuffer, BufRingSpec, DEFAULT_QUEUE_SIZE, IO_DRIVER, MultiOpQueue, MultiPollResult,
    MultishotCompletionEvent,
    operations::{IoOP, MultishotToken},
};

/// Stable storage for the msghdr template, boxed so it survives moves of the
/// owning `UdpMultiRecv`.
struct MsgHdrBox {
    msghdr: libc::msghdr,
    addr: libc::sockaddr_in,
}

impl MsgHdrBox {
    fn new() -> Box<Self> {
        let mut b = Box::new(Self {
            msghdr: unsafe { std::mem::zeroed() },
            addr: unsafe { std::mem::zeroed() },
        });
        b.msghdr.msg_name = &mut b.addr as *mut _ as *mut _;
        b.msghdr.msg_namelen = std::mem::size_of::<libc::sockaddr_in>() as u32;
        b
    }

    fn msghdr_ptr(&self) -> *const libc::msghdr {
        &self.msghdr as *const _
    }
}

/// Owns its socket for as long as it streams, like `MultiRecv` does for TCP.
/// Reply through [`UdpMultiRecv::socket`]'s `send_to`; take the socket back
/// with [`UdpMultiRecv::into_inner`].
pub struct UdpMultiRecv<S: BufRingSpec, const QUEUE_SIZE: usize = DEFAULT_QUEUE_SIZE> {
    id: Option<MultishotToken>,
    /// `Some` until `into_inner` moves the socket out.
    socket: Option<UdpSocket>,
    reads: MultiOpQueue,
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
}

impl<S: BufRingSpec> UdpMultiRecv<S, DEFAULT_QUEUE_SIZE> {
    pub fn new(socket: UdpSocket) -> Self {
        Self::with_queue_size(socket)
    }
}

impl<S: BufRingSpec, const QUEUE_SIZE: usize> UdpMultiRecv<S, QUEUE_SIZE> {
    pub fn with_queue_size(socket: UdpSocket) -> Self {
        Self {
            id: None,
            socket: Some(socket),
            reads: MultiOpQueue::new(QUEUE_SIZE),
            hdr: Some(MsgHdrBox::new()),
            _spec: PhantomData,
        }
    }

    /// The socket this stream reads from — replies go through its `send_to`.
    pub fn socket(&self) -> &UdpSocket {
        self.socket.as_ref().expect("socket present")
    }

    /// Stop streaming and take the socket back. Cancels the multishot the
    /// same way `Drop` does; the driver absorbs the cancelled op's final
    /// CQEs and recycles their buffers.
    pub fn into_inner(mut self) -> UdpSocket {
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

    fn try_parse(&self, event: MultishotCompletionEvent) -> Result<UdpDatagram<S>, ()> {
        if event.result <= 0 {
            if let Some(bid) = buffer_select(event.flags) {
                S::recycle_bid(bid);
            }
            return Err(());
        }

        let len = event.result as usize;
        let bid = buffer_select(event.flags).ok_or(())?;
        let buffer = S::make_buffer(bid, len);

        let parsed = RecvMsgOut::parse(
            buffer.as_ref(),
            &self.hdr.as_ref().expect("hdr present").msghdr,
        )
        .map_err(|_| ())?;

        let name = parsed.name_data();
        if name.len() < std::mem::size_of::<libc::sockaddr_in>() {
            return Err(());
        }

        let sockaddr: libc::sockaddr_in = unsafe { std::ptr::read_unaligned(name.as_ptr().cast()) };
        let ip = Ipv4Addr::from(u32::from_be(sockaddr.sin_addr.s_addr));
        let port = u16::from_be(sockaddr.sin_port);
        let addr = SocketAddr::V4(SocketAddrV4::new(ip, port));

        let payload = parsed.payload_data();
        let payload_offset = payload.as_ptr() as usize - buffer.as_ref().as_ptr() as usize;
        let payload_len = payload.len();

        Ok(UdpDatagram {
            buffer,
            payload_offset,
            payload_len,
            addr,
        })
    }

    fn try_drain_one(&mut self) -> Poll<Option<UdpDatagram<S>>> {
        while let Some(event) = self.reads.pop() {
            if let Ok(datagram) = self.try_parse(event) {
                return Poll::Ready(Some(datagram));
            }
        }
        Poll::Pending
    }
}

impl<S: BufRingSpec, const QUEUE_SIZE: usize> Drop for UdpMultiRecv<S, QUEUE_SIZE> {
    fn drop(&mut self) {
        // Recycle kernel buffers from any unprocessed completion events
        // before cancelling, otherwise buffer-ring entries leak.
        self.recycle_pending_buffers();
        self.driver_cleanup();
    }
}

impl<S: BufRingSpec, const QUEUE_SIZE: usize> UdpMultiRecv<S, QUEUE_SIZE> {
    fn recycle_pending_buffers(&mut self) {
        while let Some(event) = self.reads.pop() {
            // The flag alone says a buffer was consumed, whatever the result.
            if let Some(buffer_id) = buffer_select(event.flags) {
                S::recycle_bid(buffer_id);
            }
        }
    }
}

impl<S: BufRingSpec, const QUEUE_SIZE: usize> Unpin for UdpMultiRecv<S, QUEUE_SIZE> {}

impl<S: BufRingSpec, const QUEUE_SIZE: usize> Stream for UdpMultiRecv<S, QUEUE_SIZE> {
    type Item = UdpDatagram<S>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.ensure_registered();

        if let Poll::Ready(result) = self.try_drain_one() {
            return Poll::Ready(result);
        }

        let waker = cx.waker();
        let token = self.id.unwrap();

        loop {
            match IO_DRIVER.with(|d| d.poll_op_multishot(token, waker, &mut self.reads)) {
                MultiPollResult::Pending => return Poll::Pending,
                // Only skippable errors came out: ask again, the op may be over.
                MultiPollResult::Ready => {
                    if let Poll::Ready(result) = self.try_drain_one() {
                        return Poll::Ready(result);
                    }
                }
                MultiPollResult::NeedsResubmit => {
                    // The paused queue came with the swap, and nothing will wake us for it.
                    if let Poll::Ready(result) = self.try_drain_one() {
                        return Poll::Ready(result);
                    }
                    let entry = self.to_entry();
                    IO_DRIVER.with(|d| d.resubmit_multishot(token, entry, waker));
                    return Poll::Pending;
                }
            }
        }
    }
}

impl<S: BufRingSpec, const QUEUE_SIZE: usize> IoOP for UdpMultiRecv<S, QUEUE_SIZE> {
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
            let keep = self
                .hdr
                .take()
                .map(|h| h as Box<dyn std::any::Any>);
            let parked = Some(self.socket().park());
            IO_DRIVER.with(|d| d.cancel_multishot_op_keep(token, S::BGID, keep, parked));
        }
    }
}
