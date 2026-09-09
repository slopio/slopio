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
    operations::{IoBuf, OneshotToken, Slice, sockets::{OpSocket, SocketWrapper}},
};

/// Kernel cap on iovecs per submission.
pub(super) const IOV_MAX: usize = libc::UIO_MAXIOV as usize;

/// Cancellation-safe vectored send: takes ownership of every chunk, returns
/// the chunk list on completion. If the future is dropped mid-flight, both
/// the chunks AND the kernel-visible iovec array are moved into the driver's
/// keepalive slot so the kernel can finish reading them after the cancel CQE.
pub fn socket_send_vectored<B: IoBuf>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
) -> WriteVectoredSocket<B> {
    let iovecs = bufs
        .iter()
        .map(|b| libc::iovec {
            iov_base: b.stable_ptr() as *mut _,
            iov_len: b.bytes_init(),
        })
        .collect();
    WriteVectoredSocket {
        socket: socket.share(),
        id: None,
        bufs: Some(bufs),
        iovecs,
    }
}

pub struct WriteVectoredSocket<B: IoBuf> {
    socket: OpSocket,
    id: Option<OneshotToken>,
    bufs: Option<Vec<B>>,
    /// Built once at construction. The Vec's heap allocation is what the
    /// kernel reads as the iovec array — its pointer stays stable across
    /// moves of `self`, but it must outlive any in-flight kernel read.
    iovecs: Vec<libc::iovec>,
}

impl<B: IoBuf> WriteVectoredSocket<B> {
    fn register_to_driver(&mut self) {
        let entry = opcode::Writev::new(
            Fd(self.socket.fd()),
            self.iovecs.as_ptr(),
            self.iovecs.len() as u32,
        )
        .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<B: IoBuf> Future for WriteVectoredSocket<B> {
    type Output = (std::io::Result<usize>, Vec<B>);

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
                let bufs = self.bufs.take().expect("bufs already taken");
                let result = match io_result.result {
                    n @ 0.. => Ok(n as usize),
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                };
                Poll::Ready((result, bufs))
            }
        }
    }
}

impl<B: IoBuf> Drop for WriteVectoredSocket<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // Two layers of memory the kernel reads:
            //   - the iovec array (`self.iovecs`)
            //   - each iovec's `iov_base`, which points into a B
            // Both must outlive any in-flight read. Move both into keepalive.
            //
            // When B's data lives forever (`NEEDS_KEEPALIVE = false`, e.g.
            // `&'static [u8]`), only the iovec array itself needs keeping
            // alive — the const-evaluated branch saves moving the Vec<B>.
            let keep: Option<Box<dyn Any>> = if B::NEEDS_KEEPALIVE {
                self.bufs.take().map(|bufs| {
                    Box::new((bufs, std::mem::take(&mut self.iovecs))) as Box<dyn Any>
                })
            } else {
                Some(Box::new(std::mem::take(&mut self.iovecs)))
            };
            let parked = Some(self.socket.park());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep, parked));
        }
    }
}

/// Send every byte across all chunks, looping over short writes and over the
/// `IOV_MAX` chunk limit. Returns the original `Vec<B>`.
pub async fn send_vectored_all<B: IoBuf>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
) -> (std::io::Result<()>, Vec<B>) {
    send_all_with(socket, bufs, socket_send_vectored).await
}

/// The `_all` loop shared by the copying and zero-copy gather sends. Each
/// iteration submits at most `IOV_MAX` chunks from the first unsent one on.
pub(super) async fn send_all_with<B, S, Fut>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
    submit: S,
) -> (std::io::Result<()>, Vec<B>)
where
    B: IoBuf,
    S: Fn(&SocketWrapper, Vec<Slice<B>>) -> Fut,
    Fut: Future<Output = (std::io::Result<usize>, Vec<Slice<B>>)>,
{
    let total: usize = bufs.iter().map(|b| b.bytes_init()).sum();
    if total == 0 {
        return (Ok(()), bufs);
    }

    let mut chunks: Vec<Slice<B>> = bufs.into_iter().map(|b| b.slice(..)).collect();
    let mut sent = 0usize;
    let mut first = 0usize;

    while sent < total {
        let mut window = chunks.split_off(first);
        let tail = window.split_off(window.len().min(IOV_MAX));
        let (result, window) = submit(socket, window).await;
        chunks.extend(window);
        chunks.extend(tail);
        match result {
            Ok(0) => {
                return (
                    Err(std::io::ErrorKind::WriteZero.into()),
                    unwrap_chunks(chunks),
                );
            }
            Ok(n) => {
                sent += n;
                if sent >= total {
                    break;
                }
                first = advance_chunks(&mut chunks, first, n);
            }
            Err(e) => return (Err(e), unwrap_chunks(chunks)),
        }
    }
    (Ok(()), unwrap_chunks(chunks))
}

fn unwrap_chunks<B: IoBuf>(chunks: Vec<Slice<B>>) -> Vec<B> {
    chunks.into_iter().map(|s| s.into_inner()).collect()
}

/// Consume `n` bytes from `chunks[first..]` in place; returns the new first
/// chunk with bytes left to send.
fn advance_chunks<B: IoBuf>(chunks: &mut [Slice<B>], mut first: usize, mut n: usize) -> usize {
    while n > 0 {
        let len = chunks[first].len();
        if n >= len {
            n -= len;
            first += 1;
        } else {
            chunks[first].advance(n);
            n = 0;
        }
    }
    while first < chunks.len() && chunks[first].is_empty() {
        first += 1;
    }
    first
}
