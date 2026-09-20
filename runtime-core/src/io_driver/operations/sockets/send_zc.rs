use io_uring::{
    opcode::{self},
    types::Fd,
};
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::io_driver::{
    IO_DRIVER,
    operations::{
        IoBuf, OneshotToken,
        sockets::{OpSocket, SocketWrapper},
    },
};

/// Zero-copy send (`IORING_OP_SEND_ZC`): the kernel transmits directly from
/// the user buffer instead of copying it into socket memory. Because the NIC
/// may still be reading the buffer after the send "completes", the operation
/// emits two CQEs — a completion (byte count) and a later notification (buffer
/// released). This future resolves only on the notification, so the buffer is
/// guaranteed free by the time it is handed back.
///
/// Like [`socket_send`](super::send::socket_send) it is cancellation-safe: a
/// mid-flight drop stashes the buffer in the driver's keepalive slot and frees
/// it only once the final CQE arrives.
///
/// Zero-copy is most worthwhile for large payloads; for small ones the kernel
/// may transparently fall back to a copy (a single CQE, no notification),
/// which this future handles like an ordinary send.
pub fn socket_send_zc<B: IoBuf>(socket: &SocketWrapper, buf: B) -> WriteSocketZc<B> {
    socket_send_zc_with_flags(socket, buf, socket.default_msg_flags())
}

/// As above, overriding the socket's default `msg_flags`. `0` restores
/// short-write behaviour.
pub fn socket_send_zc_with_flags<B: IoBuf>(
    socket: &SocketWrapper,
    buf: B,
    msg_flags: u32,
) -> WriteSocketZc<B> {
    WriteSocketZc {
        socket: socket.share(),
        id: None,
        buf: Some(buf),
        msg_flags,
    }
}

pub struct WriteSocketZc<B: IoBuf> {
    socket: OpSocket,
    id: Option<OneshotToken>,
    buf: Option<B>,
    msg_flags: u32,
}

impl<B: IoBuf> WriteSocketZc<B> {
    fn register_to_driver(&mut self) {
        let buf = self.buf.as_ref().expect("buf taken before submit");
        let entry = opcode::SendZc::new(
            Fd(self.socket.fd()),
            buf.stable_ptr(),
            buf.bytes_init() as u32,
        )
        .flags(self.msg_flags as i32)
        .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<B: IoBuf> Future for WriteSocketZc<B> {
    type Output = (std::io::Result<usize>, B);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.id.is_none() {
            self.register_to_driver();
        }
        let waker = cx.waker();
        let token = self.id.unwrap();
        // The two-CQE choreography lives entirely in the slab state machine
        // (Sent -> Notif -> Ended); from here it looks like any single-shot op.
        match IO_DRIVER.with(|d| d.poll_op(token, waker)) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(io_result) => {
                self.id = None;
                let buf = self.buf.take().expect("buf already taken");
                let result = match io_result.result {
                    n @ 0.. => Ok(n as usize),
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                };
                Poll::Ready((result, buf))
            }
        }
    }
}

impl<B: IoBuf> Drop for WriteSocketZc<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // Keep the buffer unless `'static`; park the fd ref so the
            // close posts only after the cancel's terminal CQE.
            let keep = if B::NEEDS_KEEPALIVE {
                self.buf.take().map(|b| Box::new(b) as Box<dyn Any>)
            } else {
                None
            };
            let parked = Some(self.socket.park());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep, parked));
        }
    }
}

/// Send the entire buffer with zero-copy, looping over short writes until every
/// byte is queued or an error occurs. Returns `WriteZero` if the kernel ever
/// accepts zero bytes (which would otherwise spin forever).
pub async fn send_all_zc<B: IoBuf>(socket: &SocketWrapper, buf: B) -> (std::io::Result<()>, B) {
    let total = buf.bytes_init();
    let mut offset = 0;
    let mut buf = buf;
    while offset < total {
        let slice = buf.slice(offset..total);
        let (result, slice) = socket_send_zc(socket, slice).await;
        match result {
            Ok(0) => {
                return (
                    Err(std::io::ErrorKind::WriteZero.into()),
                    slice.into_inner(),
                );
            }
            Ok(n) => offset += n,
            Err(e) => return (Err(e), slice.into_inner()),
        }
        buf = slice.into_inner();
    }
    (Ok(()), buf)
}
