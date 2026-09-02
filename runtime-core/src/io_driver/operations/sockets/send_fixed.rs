use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::{opcode, types::Fd};

use crate::io_driver::{
    IO_DRIVER, PooledBuf,
    operations::{OneshotToken, sockets::{OpSocket, SocketWrapper}},
};

/// Cancellation-safe send over a pre-registered ("fixed") buffer.
///
/// The future takes ownership of the `PooledBuf` and returns it on completion,
/// so the slot is released (via the buffer's `Drop`) exactly once, at the
/// caller's discretion. If the future is dropped mid-flight, the `PooledBuf`
/// is moved into the driver's keepalive and dropped — releasing its slot —
/// only once the cancelled CQE arrives, so the kernel never reads from a slot
/// that has been handed back to the pool.
///
/// `WriteFixedSocket` reads `buf.ptr()..buf.ptr() + buf.len()` from the
/// registered region addressed by `buf.buf_index()`.
pub fn socket_write_fixed(socket: &SocketWrapper, buf: PooledBuf) -> WriteFixedSocket {
    WriteFixedSocket {
        socket: socket.share(),
        id: None,
        buf: Some(buf),
    }
}

pub struct WriteFixedSocket {
    socket: OpSocket,
    id: Option<OneshotToken>,
    buf: Option<PooledBuf>,
}

impl WriteFixedSocket {
    fn register_to_driver(&mut self) {
        let buf = self.buf.as_ref().expect("buf taken before submit");
        let entry =
            opcode::WriteFixed::new(Fd(self.socket.fd()), buf.ptr(), buf.len() as u32, buf.buf_index().get())
                .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl Future for WriteFixedSocket {
    type Output = (std::io::Result<usize>, PooledBuf);

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
                    n @ 0.. => Ok(n as usize),
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                };
                Poll::Ready((result, buf))
            }
        }
    }
}

impl Drop for WriteFixedSocket {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // Mid-flight cancel: move the buffer into the keepalive so its
            // slot-releasing `Drop` fires only after the cancelled CQE — at
            // which point the kernel is guaranteed done with the registered
            // slot.
            let keep = self.buf.take().map(|b| Box::new(b) as Box<dyn Any>);
            let parked = Some(self.socket.park());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep, parked));
        }
    }
}
