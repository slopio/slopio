use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::{opcode, types::Fd};

use crate::io_driver::IO_DRIVER;
use crate::io_driver::operations::{IoBuf, IoBufMut, OneshotToken};

/// Async read from a pipe fd via io_uring. Takes ownership of the buffer and
/// returns it on completion; if dropped mid-flight the buffer is kept alive
/// until the cancelled CQE arrives.
pub struct ReadPipe<B: IoBufMut> {
    fd: i32,
    id: Option<OneshotToken>,
    buf: Option<B>,
}

impl<B: IoBufMut> ReadPipe<B> {
    pub fn new(fd: i32, buf: B) -> Self {
        Self {
            fd,
            id: None,
            buf: Some(buf),
        }
    }

    fn register_to_driver(&mut self) {
        let buf = self.buf.as_mut().expect("buf taken before submit");
        let entry =
            opcode::Read::new(Fd(self.fd), buf.stable_mut_ptr(), buf.bytes_total() as u32).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<B: IoBufMut> Future for ReadPipe<B> {
    type Output = (std::io::Result<usize>, B);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.id.is_none() {
            self.register_to_driver();
        }
        let token = self.id.unwrap();
        match IO_DRIVER.with(|d| d.poll_op(token, cx.waker())) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(io_result) => {
                self.id = None;
                let mut buf = self.buf.take().expect("buf already taken");
                let result = match io_result.result {
                    n @ 0.. => {
                        let n = n as usize;
                        // SAFETY: the kernel wrote `n` bytes into `stable_mut_ptr`.
                        unsafe { buf.set_init(n) };
                        Ok(n)
                    }
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                };
                Poll::Ready((result, buf))
            }
        }
    }
}

impl<B: IoBufMut> Drop for ReadPipe<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // The kernel writes into the buffer until the cancel CQE.
            let keep = self.buf.take().map(|b| Box::new(b) as Box<dyn Any>);
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep, None));
        }
    }
}

/// Async write to a pipe fd via io_uring. Takes ownership of the buffer and
/// returns it on completion; if dropped mid-flight the buffer is kept alive
/// until the cancelled CQE arrives.
pub struct WritePipe<B: IoBuf> {
    fd: i32,
    id: Option<OneshotToken>,
    buf: Option<B>,
}

impl<B: IoBuf> WritePipe<B> {
    pub fn new(fd: i32, buf: B) -> Self {
        Self {
            fd,
            id: None,
            buf: Some(buf),
        }
    }

    fn register_to_driver(&mut self) {
        let buf = self.buf.as_ref().expect("buf taken before submit");
        let entry =
            opcode::Write::new(Fd(self.fd), buf.stable_ptr(), buf.bytes_init() as u32).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<B: IoBuf> Future for WritePipe<B> {
    type Output = (std::io::Result<usize>, B);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.id.is_none() {
            self.register_to_driver();
        }
        let token = self.id.unwrap();
        match IO_DRIVER.with(|d| d.poll_op(token, cx.waker())) {
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

impl<B: IoBuf> Drop for WritePipe<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // The kernel reads the buffer until the cancel CQE.
            let keep = if B::NEEDS_KEEPALIVE {
                self.buf.take().map(|b| Box::new(b) as Box<dyn Any>)
            } else {
                None
            };
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep, None));
        }
    }
}
