use crate::io_driver::operations::sockets::OpSocket;
use std::{
    any::Any,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode, types::Fd};

use crate::io_driver::operations::{IoBufMut, OneshotToken};
use crate::io_driver::{IO_DRIVER, sys};

/// Heap-stable kernel-visible state for an in-flight `RecvMsg` op.
/// `msghdr.msg_name` points into this struct's `addr` (kernel writes the
/// sender address there); `msghdr.msg_iov` points into `iovec`. Boxed so
/// the whole thing has a stable heap address regardless of future moves,
/// and can be handed to the cancel keepalive on drop.
#[allow(dead_code)]
struct UdpRecvState {
    msghdr: libc::msghdr,
    iovec: libc::iovec,
    addr: libc::sockaddr_storage,
}

/// Cancellation-safe UDP recv: takes ownership of the buffer, returns it on
/// completion with the kernel-written byte count reflected in
/// `IoBufMut::set_init`. If dropped mid-flight, both buffer and boxed kernel
/// state are kept alive via the driver's keepalive slot.
pub struct UdpRecvFrom<B: IoBufMut> {
    socket: OpSocket,
    id: Option<OneshotToken>,
    buf: Option<B>,
    state: Option<Box<UdpRecvState>>,
}

impl<B: IoBufMut> UdpRecvFrom<B> {
    pub(super) fn new(socket: OpSocket, buf: B) -> Self {
        // All-zero state is fine: addr will be overwritten by the kernel,
        // msghdr/iovec are populated in `register_to_driver`.
        let state = Box::new(unsafe { std::mem::zeroed::<UdpRecvState>() });
        Self {
            socket,
            id: None,
            buf: Some(buf),
            state: Some(state),
        }
    }

    fn register_to_driver(&mut self) {
        let state = self.state.as_mut().expect("state present");
        let buf = self.buf.as_mut().expect("buf present");

        state.iovec.iov_base = buf.stable_mut_ptr().cast();
        state.iovec.iov_len = buf.bytes_total();

        let addr_ptr = &mut state.addr as *mut _ as *mut libc::c_void;
        let iovec_ptr = &mut state.iovec as *mut libc::iovec;

        state.msghdr.msg_name = addr_ptr;
        state.msghdr.msg_namelen = std::mem::size_of::<libc::sockaddr_storage>() as u32;
        state.msghdr.msg_iov = iovec_ptr;
        state.msghdr.msg_iovlen = 1;

        // MSG_TRUNC: the result is the datagram's real length, even past the buffer.
        let entry = opcode::RecvMsg::new(Fd(self.socket.fd()), &mut state.msghdr)
            .flags(libc::MSG_TRUNC as u32)
            .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

pub struct UdpPacket {
    len: usize,
    addr: SocketAddr,
    truncated: bool,
}

impl UdpPacket {
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// The datagram was larger than the buffer; only `len()` bytes were kept.
    pub fn is_truncated(&self) -> bool {
        self.truncated
    }
}

impl<B: IoBufMut> Future for UdpRecvFrom<B> {
    type Output = (std::io::Result<UdpPacket>, B);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.id.is_none() {
            self.register_to_driver();
        }
        let waker = cx.waker();
        let token = self.id.unwrap();
        match IO_DRIVER.with(|d| d.poll_op(token, waker)) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(io_result) => {
                self.id = None;
                let mut buf = self.buf.take().expect("buf already taken");
                let result = match io_result.result {
                    len @ 0.. => {
                        let full = len as usize;
                        let n = full.min(buf.bytes_total());
                        // SAFETY: the kernel wrote `n` bytes into the buffer
                        // we handed it via `stable_mut_ptr` / `bytes_total`.
                        unsafe {
                            buf.set_init(n);
                        }
                        let state = self.state.as_ref().expect("state present");
                        match sys::decode_sockaddr(&state.addr) {
                            Some(addr) => Ok(UdpPacket {
                                len: n,
                                addr,
                                truncated: full > n,
                            }),
                            None => Err(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "unsupported address family",
                            )),
                        }
                    }
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                };
                Poll::Ready((result, buf))
            }
        }
    }
}

impl<B: IoBufMut> Drop for UdpRecvFrom<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // Both must be kept alive: kernel writes into buf and msghdr→addr
            // until the cancel CQE arrives.
            let state = self.state.take().expect("state present");
            let buf = self.buf.take().expect("buf present");
            let keep: Box<dyn Any> = Box::new((buf, state));
            let parked = Some(self.socket.park());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, Some(keep), parked));
        }
    }
}
