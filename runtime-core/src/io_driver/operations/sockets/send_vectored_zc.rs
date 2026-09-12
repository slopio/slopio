use io_uring::{opcode, types::Fd};
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::send_vectored::{VectoredMsg, send_all_with};
pub use super::send_vectored::DEFAULT_MSG_FLAGS as ZC_DEFAULT_MSG_FLAGS;
use crate::io_driver::{
    IO_DRIVER,
    operations::{IoBuf, OneshotToken, sockets::{OpSocket, SocketWrapper}},
};

/// Zero-copy gather send (`IORING_OP_SENDMSG_ZC`): every chunk goes out in
/// one submission straight from user memory; resolves on the kernel's
/// notification, when all chunks are free again. At most `IOV_MAX` chunks.
pub fn socket_send_vectored_zc<B: IoBuf>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
) -> WriteVectoredSocketZc<B> {
    socket_send_vectored_zc_with_flags(socket, bufs, socket.default_msg_flags())
}

/// As above, overriding the socket's default `msg_flags`. `0` restores
/// short-write behaviour.
pub fn socket_send_vectored_zc_with_flags<B: IoBuf>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
    msg_flags: u32,
) -> WriteVectoredSocketZc<B> {
    submit_from(socket, bufs, 0, 0, msg_flags)
}

/// Covers `bufs[first..]`, skipping `offset` bytes of `bufs[first]`.
fn submit_from<B: IoBuf>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
    first: usize,
    offset: usize,
    msg_flags: u32,
) -> WriteVectoredSocketZc<B> {
    let msg = VectoredMsg::new(&bufs, first, offset);
    WriteVectoredSocketZc {
        socket: socket.share(),
        id: None,
        bufs: Some(bufs),
        msg: Some(msg),
        msg_flags,
    }
}

pub struct WriteVectoredSocketZc<B: IoBuf> {
    socket: OpSocket,
    id: Option<OneshotToken>,
    bufs: Option<Vec<B>>,
    msg: Option<Box<VectoredMsg>>,
    msg_flags: u32,
}

impl<B: IoBuf> WriteVectoredSocketZc<B> {
    fn register_to_driver(&mut self) {
        let msg = self.msg.as_ref().expect("msg taken before submit");
        let entry = opcode::SendMsgZc::new(Fd(self.socket.fd()), &msg.msghdr)
            .flags(self.msg_flags)
            .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<B: IoBuf> Future for WriteVectoredSocketZc<B> {
    type Output = (std::io::Result<usize>, Vec<B>);

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
                let bufs = self.bufs.take().expect("bufs already taken");
                let result = match io_result.result {
                    n @ 0.. => Ok(n as usize),
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                };
                Poll::Ready((result, bufs))
            }
        }
    }
}

impl<B: IoBuf> Drop for WriteVectoredSocketZc<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // msghdr + iovecs are always kernel-read; chunks only when not `'static`.
            let msg = self.msg.take().expect("msg taken");
            let keep: Box<dyn Any> = if B::NEEDS_KEEPALIVE {
                Box::new((self.bufs.take(), msg))
            } else {
                Box::new(msg)
            };
            let parked = Some(self.socket.park());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, Some(keep), parked));
        }
    }
}

/// Zero-copy gather send of every byte across all chunks, looping over short
/// writes and over the `IOV_MAX` chunk limit. Returns the original `Vec<B>`.
pub async fn send_vectored_all_zc<B: IoBuf>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
) -> (std::io::Result<()>, Vec<B>) {
    let msg_flags = socket.default_msg_flags();
    send_all_with(socket, bufs, move |s, b, first, offset| {
        submit_from(s, b, first, offset, msg_flags)
    })
    .await
}
