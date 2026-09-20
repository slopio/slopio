use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::{opcode, types::Fd};

use crate::io_driver::{
    IO_DRIVER,
    operations::{IoBuf, OneshotToken, files::OpenedFile},
};

impl OpenedFile {
    /// Cancellation-safe write: takes ownership of the buffer, returns it on
    /// completion. If the future is dropped mid-flight, the buffer (when
    /// `B::NEEDS_KEEPALIVE`) is moved into the driver's keepalive slot so
    /// the kernel never reads from freed memory.
    pub fn write<B: IoBuf>(&self, buf: B) -> WriteFile<B> {
        WriteFile {
            fd: self.fd,
            id: None,
            buf: Some(buf),
        }
    }
}

pub struct WriteFile<B: IoBuf> {
    fd: i32,
    id: Option<OneshotToken>,
    buf: Option<B>,
}

impl<B: IoBuf> WriteFile<B> {
    fn register_to_driver(&mut self) {
        let buf = self.buf.as_ref().expect("buf taken before submit");
        let entry =
            opcode::Write::new(Fd(self.fd), buf.stable_ptr(), buf.bytes_init() as u32).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<B: IoBuf> Future for WriteFile<B> {
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

impl<B: IoBuf> Drop for WriteFile<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            let keep = if B::NEEDS_KEEPALIVE {
                self.buf.take().map(|b| Box::new(b) as Box<dyn Any>)
            } else {
                None
            };
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep, None));
        }
    }
}

/// Write the entire buffer, looping over short writes until every byte is
/// flushed to the file or an error occurs. Returns the buffer back to the
/// caller alongside the result. Returns `WriteZero` if the kernel ever
/// accepts zero bytes (which would otherwise spin forever).
pub async fn write_all<B: IoBuf>(file: &OpenedFile, buf: B) -> (std::io::Result<()>, B) {
    let total = buf.bytes_init();
    if total == 0 {
        return (Ok(()), buf);
    }
    let mut slice = buf.slice(..);
    loop {
        let (result, returned) = file.write(slice).await;
        slice = returned;
        match result {
            Ok(0) => {
                return (
                    Err(std::io::ErrorKind::WriteZero.into()),
                    slice.into_inner(),
                );
            }
            Ok(n) if n >= slice.len() => return (Ok(()), slice.into_inner()),
            Ok(n) => slice.advance(n),
            Err(e) => return (Err(e), slice.into_inner()),
        }
    }
}
