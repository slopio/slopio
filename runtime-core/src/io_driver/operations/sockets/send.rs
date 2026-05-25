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
    operations::{IoBuf, OneshotToken, sockets::SocketWrapper},
};

/// Cancellation-safe send: takes ownership of the buffer, returns it on
/// completion. If the future is dropped mid-flight, the buffer is moved into
/// the driver's keepalive slot and only freed once the cancelled CQE arrives
/// — so the kernel never reads from freed memory.
pub fn socket_send<B: IoBuf>(socket: &SocketWrapper, buf: B) -> WriteSocket<B> {
    WriteSocket {
        fd: socket.fd(),
        id: None,
        buf: Some(buf),
    }
}

pub struct WriteSocket<B: IoBuf> {
    fd: i32,
    id: Option<OneshotToken>,
    buf: Option<B>,
}

impl<B: IoBuf> WriteSocket<B> {
    fn register_to_driver(&mut self) {
        let buf = self.buf.as_ref().expect("buf taken before submit");
        let entry =
            opcode::Send::new(Fd(self.fd), buf.stable_ptr(), buf.bytes_init() as u32).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<B: IoBuf> Future for WriteSocket<B> {
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
                    n @ 0.. => Ok(n as usize),
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                };
                Poll::Ready((result, buf))
            }
        }
    }
}

impl<B: IoBuf> Drop for WriteSocket<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // For buffers whose memory outlives any drop (e.g. `&'static [u8]`
            // pointing into `.rodata`), skip the type-erasure allocation —
            // the kernel pointer is valid forever. Const-evaluated, so the
            // unused branch is eliminated at monomorphization.
            let keep = if B::NEEDS_KEEPALIVE {
                self.buf.take().map(|b| Box::new(b) as Box<dyn Any>)
            } else {
                None
            };
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep));
        }
    }
}

/// Send the entire buffer, looping over short writes until every byte is
/// queued or an error occurs. Returns `WriteZero` if the kernel ever
/// accepts zero bytes (which would otherwise spin forever).
pub async fn send_all<B: IoBuf>(socket: &SocketWrapper, buf: B) -> (std::io::Result<()>, B) {
    let total = buf.bytes_init();
    let mut offset = 0;
    let mut buf = buf;
    while offset < total {
        let slice = buf.slice(offset..total);
        let (result, slice) = socket_send(socket, slice).await;
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
