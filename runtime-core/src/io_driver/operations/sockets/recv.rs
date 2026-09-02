use io_uring::opcode::{self};
use io_uring::types::Fd;
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::io_driver::IO_DRIVER;
use crate::io_driver::operations::{IoBufMut, OneshotToken};

use super::{OpSocket, SocketWrapper};

/// Cancellation-safe socket recv: takes ownership of the buffer, returns it
/// on completion with the kernel-written byte count reflected in
/// `IoBufMut::set_init`. If dropped mid-flight, the buffer is moved into the
/// driver's keepalive slot so the kernel never writes into freed memory.
pub struct ClassicRecv<B: IoBufMut> {
    socket: OpSocket,
    id: Option<OneshotToken>,
    buf: Option<B>,
}

impl<B: IoBufMut> ClassicRecv<B> {
    fn from_socket(socket: &SocketWrapper, buf: B) -> Self {
        Self {
            socket: socket.share(),
            id: None,
            buf: Some(buf),
        }
    }

    fn register_to_driver(&mut self) {
        let buf = self.buf.as_mut().expect("buf taken before submit");
        let entry = opcode::Recv::new(
            Fd(self.socket.fd()),
            buf.stable_mut_ptr(),
            buf.bytes_total() as u32,
        )
        .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

pub fn socket_recv<B: IoBufMut>(socket: &SocketWrapper, buf: B) -> ClassicRecv<B> {
    ClassicRecv::from_socket(socket, buf)
}

impl<B: IoBufMut> Future for ClassicRecv<B> {
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
                let mut buf = self.buf.take().expect("buf already taken");
                let result = match io_result.result {
                    n @ 0.. => {
                        let n = n as usize;
                        // SAFETY: the kernel wrote `n` bytes into the buffer
                        // we handed it via `stable_mut_ptr` / `bytes_total`.
                        unsafe {
                            buf.set_init(n);
                        }
                        Ok(n)
                    }
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                };
                Poll::Ready((result, buf))
            }
        }
    }
}

impl<B: IoBufMut> Drop for ClassicRecv<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            let keep = self.buf.take().map(|b| Box::new(b) as Box<dyn Any>);
            let parked = Some(self.socket.park());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep, parked));
        }
    }
}
