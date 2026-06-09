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

/// Zero-copy send (`IORING_OP_SEND_ZC`) over a pre-registered ("fixed")
/// buffer. Combines the two wins: the kernel transmits directly from the
/// registered region (no copy into socket memory) *and* the buffer is already
/// pinned, so there is no per-op keepalive to stash.
///
/// Zero-copy sends emit two CQEs — a completion (byte count) and a later
/// notification (buffer released). This future resolves only on the
/// notification, so the `&'b PooledBuf` borrow is held — and the slot kept
/// out of its pool's free-list — until the kernel is provably done reading.
///
/// `on_cancel` is invoked if (and only if) the future is dropped mid-flight,
/// once the final CQE has arrived and the kernel is guaranteed to no longer
/// touch the registered slot. Typical use: return the buffer index to its
/// pool's free-list. Not invoked on normal completion or on drop before the
/// first poll.
pub fn socket_write_fixed_zc<'b, F>(
    socket: &SocketWrapper,
    buf: &'b PooledBuf<'_>,
    on_cancel: F,
) -> WriteFixedSocketZc<'b, F>
where
    F: FnOnce() + Unpin + 'static,
{
    WriteFixedSocketZc {
        fd: socket.fd(),
        id: None,
        ptr: buf.ptr(),
        len: buf.len() as u32,
        buf_index: buf.buf_index(),
        on_cancel: Some(on_cancel),
        _buf: PhantomData,
    }
}

pub struct WriteFixedSocketZc<'b, F: FnOnce() + Unpin + 'static> {
    fd: i32,
    id: Option<OneshotToken>,
    ptr: *const u8,
    len: u32,
    buf_index: FixedBufIndex,
    on_cancel: Option<F>,
    _buf: PhantomData<&'b ()>,
}

impl<'b, F: FnOnce() + Unpin + 'static> WriteFixedSocketZc<'b, F> {
    fn register_to_driver(&mut self) {
        // `buf_index` flips IORING_RECVSEND_FIXED_BUF: the kernel reads from
        // the registered region addressed by the index instead of pinning the
        // pointer per-op.
        let entry = opcode::SendZc::new(Fd(self.fd), self.ptr, self.len)
            .buf_index(Some(self.buf_index.get()))
            .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<'b, F: FnOnce() + Unpin + 'static> Future for WriteFixedSocketZc<'b, F> {
    type Output = std::io::Result<usize>;

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
                let result = match io_result.result {
                    n @ 0.. => Ok(n as usize),
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                };
                Poll::Ready(result)
            }
        }
    }
}

impl<'b, F: FnOnce() + Unpin + 'static> Drop for WriteFixedSocketZc<'b, F> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // Mid-flight cancel: move the callback into the keepalive so it
            // fires only after the cancelled op's final (no-F_MORE) CQE — at
            // which point the kernel is guaranteed done with the slot. The
            // `Cancelled` arm waits past any F_MORE completion, so a zero-copy
            // send that was already in flight still releases the slot only on
            // its notification.
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
