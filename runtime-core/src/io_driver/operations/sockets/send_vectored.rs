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
    operations::{IoBuf, OneshotToken, Slice, sockets::SocketWrapper},
};

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
        fd: socket.fd(),
        id: None,
        bufs: Some(bufs),
        iovecs,
    }
}

pub struct WriteVectoredSocket<B: IoBuf> {
    fd: i32,
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
            Fd(self.fd),
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
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep));
        }
    }
}

/// Send every byte across all chunks, looping over short writes until the
/// whole list is drained or an error occurs. Returns the original `Vec<B>`
/// alongside the result so the caller keeps their allocations.
///
/// Internally wraps each chunk in a [`Slice`] for per-chunk offset tracking
/// so a partial first write doesn't force a copy of the remaining tail.
pub async fn send_vectored_all<B: IoBuf>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
) -> (std::io::Result<()>, Vec<B>) {
    let total: usize = bufs.iter().map(|b| b.bytes_init()).sum();
    if total == 0 {
        return (Ok(()), bufs);
    }

    // Wrap each buf in a full slice so partial-write advancement only mutates
    // begin offsets — no allocations or copies on the slow path.
    let mut chunks: Vec<Slice<B>> = bufs.into_iter().map(|b| b.slice(..)).collect();
    let mut sent = 0usize;

    while sent < total {
        let (result, returned) = socket_send_vectored(socket, chunks).await;
        chunks = returned;
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
                advance_chunks(&mut chunks, n);
            }
            Err(e) => return (Err(e), unwrap_chunks(chunks)),
        }
    }
    (Ok(()), unwrap_chunks(chunks))
}

fn unwrap_chunks<B: IoBuf>(chunks: Vec<Slice<B>>) -> Vec<B> {
    chunks.into_iter().map(|s| s.into_inner()).collect()
}

/// Consume `n` bytes from the front of the chunk list: drain fully-consumed
/// chunks, advance the boundary chunk's begin offset.
fn advance_chunks<B: IoBuf>(chunks: &mut Vec<Slice<B>>, mut n: usize) {
    let mut to_drain = 0;
    for chunk in chunks.iter_mut() {
        let len = chunk.len();
        if n >= len {
            to_drain += 1;
            n -= len;
            if n == 0 {
                break;
            }
        } else {
            chunk.advance(n);
            break;
        }
    }
    if to_drain > 0 {
        chunks.drain(..to_drain);
    }
}
