use std::{
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

use crate::io_driver::{
    BorrowedBuffer, BufGroup, IO_DRIVER, MultiOpQueue, MultiPollResult, MultishotCompletionEvent,
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

pub struct UdpMultiRecv {
    id: Option<MultishotToken>,
    fd: i32,
    bgid: u16,
    reads: MultiOpQueue,
    /// `Some` until `driver_cleanup` runs in `Drop`, at which point the Box
    /// is moved into the driver's keepalive slot.
    hdr: Option<Box<MsgHdrBox>>,
}

pub struct UdpDatagram {
    buffer: BorrowedBuffer,
    payload_offset: usize,
    payload_len: usize,
    addr: SocketAddr,
}

impl UdpDatagram {
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

impl UdpMultiRecv {
    pub fn new(fd: i32, group: BufGroup) -> Self {
        Self {
            id: None,
            fd,
            bgid: group.bgid(),
            reads: Default::default(),
            hdr: Some(MsgHdrBox::new()),
        }
    }

    fn ensure_registered(&mut self) {
        if self.id.is_none() {
            self.register_to_driver();
        }
    }

    fn try_parse(&self, event: MultishotCompletionEvent) -> Result<UdpDatagram, ()> {
        if event.result <= 0 {
            return Err(());
        }

        let len = event.result as usize;
        let bid = buffer_select(event.flags).ok_or(())?;
        let buffer = IO_DRIVER.with(|d| d.get_buffer(self.bgid, bid, len));

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

    fn try_drain_one(&mut self) -> Poll<Option<UdpDatagram>> {
        while let Some(event) = self.reads.pop() {
            if let Ok(datagram) = self.try_parse(event) {
                return Poll::Ready(Some(datagram));
            }
        }
        Poll::Pending
    }
}

impl Drop for UdpMultiRecv {
    fn drop(&mut self) {
        // Recycle kernel buffers from any unprocessed completion events
        // before cancelling, otherwise buffer-ring entries leak.
        self.recycle_pending_buffers();
        self.driver_cleanup();
    }
}

impl UdpMultiRecv {
    fn recycle_pending_buffers(&mut self) {
        while let Some(event) = self.reads.pop() {
            if event.result > 0 {
                if let Some(buffer_id) = buffer_select(event.flags) {
                    drop(IO_DRIVER.with(|d| {
                        d.get_buffer(self.bgid, buffer_id, event.result as usize)
                    }));
                }
            }
        }
    }
}

impl Unpin for UdpMultiRecv {}

impl Stream for UdpMultiRecv {
    type Item = UdpDatagram;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.ensure_registered();

        if let Poll::Ready(result) = self.try_drain_one() {
            return Poll::Ready(result);
        }

        let waker = cx.waker();
        let token = self.id.unwrap();

        match IO_DRIVER.with(|d| d.poll_op_multishot(token, waker, &mut self.reads)) {
            MultiPollResult::Pending => Poll::Pending,
            MultiPollResult::Ready => match self.try_drain_one() {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => {
                    if let MultiPollResult::NeedsResubmit =
                        IO_DRIVER.with(|d| d.poll_op_multishot(token, waker, &mut self.reads))
                    {
                        let entry = self.to_entry();
                        IO_DRIVER.with(|d| d.resubmit_multishot(token, entry, waker));
                    }
                    Poll::Pending
                }
            },
            MultiPollResult::NeedsResubmit => {
                let entry = self.to_entry();
                IO_DRIVER.with(|d| d.resubmit_multishot(token, entry, waker));
                Poll::Pending
            }
        }
    }
}

impl IoOP for UdpMultiRecv {
    fn register_to_driver(&mut self) {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| d.push_multishot_op(entry, self.fd));
        self.id = Some(token);
    }

    fn to_entry(&self) -> io_uring::squeue::Entry {
        opcode::RecvMsgMulti::new(
            Fd(self.fd),
            self.hdr.as_ref().expect("hdr present").msghdr_ptr(),
            self.bgid,
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
            IO_DRIVER.with(|d| d.cancel_multishot_op_keep(token, self.bgid, keep));
        }
    }
}
