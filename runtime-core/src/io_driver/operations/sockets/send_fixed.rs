use std::any::Any;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::{opcode, types::Fd};

use crate::io_driver::{
    IO_DRIVER, PooledBuf,
    operations::{FixedBufIndex, OneshotToken, sockets::SocketWrapper},
};

/// Cancellation-safe send over a pre-registered ("fixed") buffer.
///
/// The buffer is borrowed for the future's lifetime, so the slot cannot be
/// released or otherwise mutated while the kernel may still be reading from
/// it. `WriteFixedSocket` reads `buf.ptr()..buf.ptr() + buf.len()` from the
/// registered region addressed by `buf.buf_index()`.
///
/// `on_cancel` is invoked if (and only if) the future is dropped mid-flight,
/// once the cancelled CQE has arrived and the kernel is guaranteed to no
/// longer touch the registered slot. Typical use: return the buffer index to
/// its pool's free-list (the caller still owns the `PooledBuf` after the
/// future is dropped, so this is the only safe moment to recycle the slot).
/// Not invoked on normal completion or on drop before the first poll.
pub fn socket_write_fixed<'b, F>(
    socket: &SocketWrapper,
    buf: &'b PooledBuf<'_>,
    on_cancel: F,
) -> WriteFixedSocket<'b, F>
where
    F: FnOnce() + Unpin + 'static,
{
    WriteFixedSocket {
        fd: socket.fd(),
        id: None,
        ptr: buf.ptr(),
        len: buf.len() as u32,
        buf_index: buf.buf_index(),
        on_cancel: Some(on_cancel),
        _buf: PhantomData,
    }
}

pub struct WriteFixedSocket<'b, F: FnOnce() + Unpin + 'static> {
    fd: i32,
    id: Option<OneshotToken>,
    ptr: *const u8,
    len: u32,
    buf_index: FixedBufIndex,
    on_cancel: Option<F>,
    _buf: PhantomData<&'b ()>,
}

impl<'b, F: FnOnce() + Unpin + 'static> WriteFixedSocket<'b, F> {
    fn register_to_driver(&mut self) {
        let entry =
            opcode::WriteFixed::new(Fd(self.fd), self.ptr, self.len, self.buf_index.get()).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<'b, F: FnOnce() + Unpin + 'static> Future for WriteFixedSocket<'b, F> {
    type Output = std::io::Result<usize>;

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
                let result = match io_result.result {
                    n @ 0.. => Ok(n as usize),
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                };
                Poll::Ready(result)
            }
        }
    }
}

impl<'b, F: FnOnce() + Unpin + 'static> Drop for WriteFixedSocket<'b, F> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // Mid-flight cancel: move the callback into the keepalive so it
            // fires only after the cancel CQE arrives — at which point the
            // kernel is guaranteed to be done with the registered slot.
            let cb = self.on_cancel.take().expect("on_cancel already consumed");
            let keep: Box<dyn Any> = Box::new(CallOnDrop(Some(cb)));
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, Some(keep)));
        }
    }
}

struct CallOnDrop<F: FnOnce()>(Option<F>);

impl<F: FnOnce()> Drop for CallOnDrop<F> {
    fn drop(&mut self) {
        if let Some(f) = self.0.take() {
            f();
        }
    }
}
