//! Bridges [`SocketWrapper`] with `tokio::io::{AsyncRead, AsyncWrite}` so it
//! can be used as the `T` in `Codec<T, B>`.

use io_uring::opcode;
use io_uring::types::Fd;
use runtime_core::io_driver::operations::sockets::SocketWrapper;
use runtime_core::io_driver::{IO_DRIVER, OpMetadata};
use std::io;
use std::mem::MaybeUninit;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Size of the internal read buffer. Reads from the socket go here first,
/// then get copied into whatever `ReadBuf` the caller provides.
const READ_BUF_SIZE: usize = 8192;
/// Wraps a [`SocketWrapper`] and implements `tokio::io::AsyncRead` and
/// `tokio::io::AsyncWrite` by driving io_uring ops through the runtime's
/// `IO_DRIVER`.
pub struct TokioSocket {
    inner: SocketWrapper,

    // ---- read state ----
    // Heap-allocated so its address is stable across moves of `TokioSocket`.
    read_buf: Box<[MaybeUninit<u8>; READ_BUF_SIZE]>,
    /// Token of the in-flight `Recv` op, if any.
    read_token: Option<usize>,
    /// Byte range in `read_buf` that has been received but not yet consumed.
    read_start: usize,
    read_end: usize,

    // ---- write state ----
    /// Token of the in-flight `Send` op, if any.
    write_token: Option<usize>,
}

impl TokioSocket {
    pub fn new(socket: SocketWrapper) -> Self {
        TokioSocket {
            inner: socket,
            read_buf: Box::new([MaybeUninit::uninit(); READ_BUF_SIZE]),
            read_token: None,
            read_start: 0,
            read_end: 0,
            write_token: None,
        }
    }

    // pub fn into_inner(self) -> SocketWrapper {
    //     self.inner
    // }
}

impl Drop for TokioSocket {
    fn drop(&mut self) {
        if let Some(id) = self.read_token.take() {
            IO_DRIVER.with(|d| d.cancel_single_op(id, None));
        }
        if let Some(id) = self.write_token.take() {
            IO_DRIVER.with(|d| d.cancel_single_op(id, None));
        }
    }
}

impl AsyncRead for TokioSocket {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        println!("poll_read: remaining={}", buf.remaining());
        loop {
            // 1. Serve buffered data from a previously completed recv.
            if self.read_start < self.read_end {
                let available = self.read_end - self.read_start;
                let to_copy = available.min(buf.remaining());
                // SAFETY: bytes [read_start..read_end] were initialised by io_uring.
                let src = unsafe {
                    let ptr = self.read_buf.as_ptr().add(self.read_start) as *const u8;
                    std::slice::from_raw_parts(ptr, to_copy)
                };
                buf.put_slice(src);
                self.read_start += to_copy;
                if self.read_start == self.read_end {
                    self.read_start = 0;
                    self.read_end = 0;
                }
                return Poll::Ready(Ok(()));
            }

            // 2. Poll an in-flight recv op.
            if let Some(token) = self.read_token {
                match IO_DRIVER.with(|d| d.poll_op(token, cx.waker())) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(OpMetadata { result, .. }) => {
                        self.read_token = None;
                        println!("poll_read: recv completed, result={}", result);
                        if result < 0 {
                            return Poll::Ready(Err(io::Error::from_raw_os_error(-result)));
                        }
                        self.read_start = 0;
                        self.read_end = result as usize;
                        // Loop back to serve the newly received data.
                        continue;
                    }
                }
            }

            // 3. No data and no op in flight — submit a new recv.
            let buf_ptr = self.read_buf.as_mut_ptr() as *mut u8;
            let entry =
                opcode::Recv::new(Fd(self.inner.fd()), buf_ptr, READ_BUF_SIZE as u32).build();
            let token = IO_DRIVER.with(|d| d.push_op(entry));
            self.read_token = Some(token as usize);
            // Loop to poll the op we just submitted.
        }
    }
}

impl AsyncWrite for TokioSocket {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(token) = self.write_token {
            match IO_DRIVER.with(|d| d.poll_op(token, cx.waker())) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(OpMetadata { result, .. }) => {
                    self.write_token = None;
                    return if result < 0 {
                        Poll::Ready(Err(io::Error::from_raw_os_error(-result)))
                    } else {
                        Poll::Ready(Ok(result as usize))
                    };
                }
            }
        }

        // Submit a new send op and poll it immediately.
        let entry = opcode::Send::new(Fd(self.inner.fd()), buf.as_ptr(), buf.len() as u32).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        let token = token as usize;

        match IO_DRIVER.with(|d| d.poll_op(token, cx.waker())) {
            Poll::Pending => {
                self.write_token = Some(token);
                Poll::Pending
            }
            Poll::Ready(OpMetadata { result, .. }) => {
                if result < 0 {
                    Poll::Ready(Err(io::Error::from_raw_os_error(-result)))
                } else {
                    Poll::Ready(Ok(result as usize))
                }
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        // TCP has no user-space flush; all data is handed to the kernel via Send.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}
