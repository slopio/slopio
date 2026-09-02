use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::{opcode, types::Fd};

use crate::io_driver::{
    IO_DRIVER, PooledBuf,
    operations::{OneshotToken, sockets::{OpSocket, SocketWrapper}},
};

/// Zero-copy send (`IORING_OP_SEND_ZC`) over a pre-registered ("fixed")
/// buffer. Combines the two wins: the kernel transmits directly from the
/// registered region (no copy into socket memory) *and* the buffer is already
/// pinned by registration, so there is no per-op page-pinning.
///
/// Zero-copy sends emit two CQEs — a completion (byte count) and a later
/// notification (buffer released). The future resolves only on the
/// notification and hands the `PooledBuf` back then, so the slot cannot be
/// returned to the pool (and reused) until the kernel is provably done reading.
/// On a mid-flight drop the `PooledBuf` is parked in the driver keepalive and
/// released only once the cancelled op's final (no-`F_MORE`) CQE arrives.
pub fn socket_write_fixed_zc(socket: &SocketWrapper, buf: PooledBuf) -> WriteFixedSocketZc {
    WriteFixedSocketZc {
        socket: socket.share(),
        id: None,
        buf: Some(buf),
    }
}

pub struct WriteFixedSocketZc {
    socket: OpSocket,
    id: Option<OneshotToken>,
    buf: Option<PooledBuf>,
}

impl WriteFixedSocketZc {
    fn register_to_driver(&mut self) {
        let buf = self.buf.as_ref().expect("buf taken before submit");
        // `buf_index` flips IORING_RECVSEND_FIXED_BUF: the kernel reads from
        // the registered region addressed by the index.
        let entry = opcode::SendZc::new(Fd(self.socket.fd()), buf.ptr(), buf.len() as u32)
            .buf_index(Some(buf.buf_index().get()))
            .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl Future for WriteFixedSocketZc {
    type Output = (std::io::Result<usize>, PooledBuf);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.id.is_none() {
            self.register_to_driver();
        }
        let waker = cx.waker();
        let token = self.id.unwrap();
        // Completion + notification are sequenced inside the slab state machine
        // (Sent -> Notif -> Ended); `poll_op` only returns Ready on the latter.
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

impl Drop for WriteFixedSocketZc {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // Mid-flight cancel: stash the buffer so its slot-releasing `Drop`
            // fires only after the cancelled op's final CQE. The `Cancelled`
            // arm waits past any `F_MORE` completion, so an in-flight zero-copy
            // send still releases its slot only on the notification.
            let keep = self.buf.take().map(|b| Box::new(b) as Box<dyn Any>);
            let parked = Some(self.socket.park());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep, parked));
        }
    }
}
