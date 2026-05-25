use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::{opcode, types::Fd};

use crate::io_driver::{
    IO_DRIVER,
    operations::{IoBufMut, OneshotToken, files::OpenedFile},
};

impl OpenedFile {
    /// Cancellation-safe read: takes ownership of the buffer, returns it on
    /// completion with the kernel-written byte count reflected in
    /// `IoBufMut::set_init`. If dropped mid-flight, the buffer is moved into
    /// the driver's keepalive slot so the kernel never writes into freed
    /// memory.
    pub fn read<B: IoBufMut>(&self, buf: B) -> ReadFile<B> {
        ReadFile {
            fd: self.fd,
            id: None,
            buf: Some(buf),
        }
    }
}

pub struct ReadFile<B: IoBufMut> {
    fd: i32,
    id: Option<OneshotToken>,
    buf: Option<B>,
}

impl<B: IoBufMut> ReadFile<B> {
    fn register_to_driver(&mut self) {
        let buf = self.buf.as_mut().expect("buf taken before submit");
        let entry = opcode::Read::new(Fd(self.fd), buf.stable_mut_ptr(), buf.bytes_total() as u32)
            .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<B: IoBufMut> Future for ReadFile<B> {
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

impl<B: IoBufMut> Drop for ReadFile<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // Kernel writes into the buffer — must keep it alive until the
            // cancel CQE arrives.
            let keep = self.buf.take().map(|b| Box::new(b) as Box<dyn Any>);
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep));
        }
    }
}
