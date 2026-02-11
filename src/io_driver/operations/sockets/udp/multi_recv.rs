use std::{
    mem::MaybeUninit,
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
    BUFFER_SIZE, IO_DRIVER, MultiOpQueue, MultiPollResult, MultishotCompletionEvent,
    operations::IoOP,
};

pub struct UdpMultiRecv {
    id: Option<usize>,
    fd: i32,
    reads: MultiOpQueue,
    msghdr: libc::msghdr,
    addr: libc::sockaddr_in,
}

pub struct UdpDatagram {
    buffer: io_uring_rb::buffer::Buffer<BUFFER_SIZE>,
    payload_offset: usize,
    payload_len: usize,
    addr: SocketAddr,
}

impl UdpDatagram {
    pub fn payload(&self) -> &[u8] {
        &self.buffer.as_ref()[self.payload_offset..self.payload_offset + self.payload_len]
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

impl Drop for UdpDatagram {
    fn drop(&mut self) {
        IO_DRIVER.with(|d| d.recycle_buffer(&self.buffer))
    }
}

impl UdpMultiRecv {
    pub fn new(fd: i32) -> Self {
        let mut recv = Self {
            id: None,
            fd,
            reads: Default::default(),
            msghdr: unsafe { MaybeUninit::zeroed().assume_init() },
            addr: unsafe { MaybeUninit::zeroed().assume_init() },
        };

        recv.msghdr.msg_name = &mut recv.addr as *mut _ as *mut _;
        recv.msghdr.msg_namelen = std::mem::size_of::<libc::sockaddr_in>() as u32;

        recv.register_to_driver();
        recv
    }

    fn try_parse(&self, event: MultishotCompletionEvent) -> Result<UdpDatagram, ()> {
        if event.result <= 0 {
            return Err(());
        }

        let len = event.result as usize;
        let bid = buffer_select(event.flags).ok_or(())?;
        let buffer = IO_DRIVER.with(|d| d.get_buffer(bid, len));

        let parsed = RecvMsgOut::parse(buffer.as_ref(), &self.msghdr).map_err(|_| ())?;

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
        self.driver_cleanup();
    }
}

impl Unpin for UdpMultiRecv {}

impl Stream for UdpMultiRecv {
    type Item = UdpDatagram;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(result) = self.try_drain_one() {
            return Poll::Ready(result);
        }

        let waker = cx.waker();
        let id = self.id.unwrap();

        match IO_DRIVER.with(|d| d.poll_op_multishot(id, waker, &mut self.reads)) {
            MultiPollResult::Pending => Poll::Pending,
            MultiPollResult::Ready => match self.try_drain_one() {
                Poll::Ready(result) => Poll::Ready(result),
                Poll::Pending => {
                    if let MultiPollResult::NeedsResubmit =
                        IO_DRIVER.with(|d| d.poll_op_multishot(id, waker, &mut self.reads))
                    {
                        let entry = self.to_entry();
                        IO_DRIVER.with(|d| d.resubmit_multishot(id, entry, waker));
                    }
                    Poll::Pending
                }
            },
            MultiPollResult::NeedsResubmit => {
                let entry = self.to_entry();
                IO_DRIVER.with(|d| d.resubmit_multishot(id, entry, waker));
                Poll::Pending
            }
        }
    }
}

impl IoOP for UdpMultiRecv {
    fn register_to_driver(&mut self) -> crate::io_driver::operations::IoDriverToken {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| d.push_multishot_op(entry, self.fd));
        self.id = Some(token as usize);
        token
    }

    fn to_entry(&self) -> io_uring::squeue::Entry {
        opcode::RecvMsgMulti::new(Fd(self.fd), &self.msghdr as *const _, 0).build()
    }

    fn driver_cleanup(&mut self) {}
}
