use crate::io_driver::operations::sockets::OpSocket;
use std::{
    any::Any,
    future::Future,
    net::SocketAddr,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode, types::Fd};

use crate::io_driver::operations::{IoBuf, OneshotToken};
use crate::io_driver::{IO_DRIVER, sys};

/// Heap-stable kernel-visible state for an in-flight `SendMsg` op.
/// `msghdr.msg_name` and `msghdr.msg_iov` point into this struct's `addr`
/// and `iovec` fields, so the whole thing must live at a stable address —
/// hence `Box<UdpSendState>` rather than inline-in-future fields.
#[allow(dead_code)]
struct UdpSendState {
    msghdr: libc::msghdr,
    iovec: libc::iovec,
    addr: libc::sockaddr_storage,
    addr_len: libc::socklen_t,
}

/// Cancellation-safe UDP send: takes ownership of the buffer, returns it on
/// completion. If dropped mid-flight, the buffer **and** the boxed kernel
/// state are both kept alive via the driver's keepalive slot until the
/// cancelled CQE arrives.
pub struct UdpSendTo<B: IoBuf> {
    socket: OpSocket,
    id: Option<OneshotToken>,
    buf: Option<B>,
    state: Option<Box<UdpSendState>>,
}

impl<B: IoBuf> UdpSendTo<B> {
    pub(super) fn new(socket: OpSocket, buf: B, dest: SocketAddr) -> Self {
        let (addr, addr_len) = sys::encode_sockaddr(&dest);
        let state = Box::new(UdpSendState {
            msghdr: unsafe { std::mem::zeroed() },
            iovec: unsafe { std::mem::zeroed() },
            addr,
            addr_len,
        });

        Self {
            socket,
            id: None,
            buf: Some(buf),
            state: Some(state),
        }
    }

    fn register_to_driver(&mut self) {
        let state = self.state.as_mut().expect("state present");
        let buf = self.buf.as_ref().expect("buf present");

        state.iovec.iov_base = buf.stable_ptr() as *mut _;
        state.iovec.iov_len = buf.bytes_init();

        let addr_ptr = &mut state.addr as *mut _ as *mut libc::c_void;
        let iovec_ptr = &mut state.iovec as *mut libc::iovec;

        state.msghdr.msg_name = addr_ptr;
        state.msghdr.msg_namelen = state.addr_len;
        state.msghdr.msg_iov = iovec_ptr;
        state.msghdr.msg_iovlen = 1;

        let entry = opcode::SendMsg::new(Fd(self.socket.fd()), &state.msghdr).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<B: IoBuf> Future for UdpSendTo<B> {
    type Output = (std::io::Result<usize>, B);

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
                let buf = self.buf.take().expect("buf already taken");
                let result = match io_result.result {
                    len @ 0.. => Ok(len as usize),
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                };
                Poll::Ready((result, buf))
            }
        }
    }
}

impl<B: IoBuf> Drop for UdpSendTo<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            let state = self.state.take().expect("state present");
            // The kernel state always needs keeping (msghdr/addr/iovec read
            // until cancel completes). The buffer only needs keeping if
            // `B::NEEDS_KEEPALIVE` says so — `&'static` data lives forever
            // and the const branch is elided at monomorphization.
            let parked = Some(self.socket.park());
            let keep: Box<dyn Any> = if B::NEEDS_KEEPALIVE {
                let buf = self.buf.take().expect("buf present");
                Box::new((buf, state))
            } else {
                Box::new(state)
            };
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, Some(keep), parked));
        }
    }
}
