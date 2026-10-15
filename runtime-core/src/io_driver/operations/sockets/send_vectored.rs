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
    operations::{
        IoBuf, OneshotToken,
        sockets::{OpSocket, SocketWrapper},
    },
};

/// Kernel cap on iovecs per submission.
pub(super) const IOV_MAX: usize = libc::UIO_MAXIOV as usize;

/// What a socket's `msg_flags` start at, for every send that does not pass its
/// own. Without `MSG_WAITALL` a short send comes back to the `_all` loop for a
/// fresh submission instead of being retried inside the kernel.
///
/// Not valid on a kTLS socket: `tls_sw_sendmsg` rejects every flag outside
/// `MSG_MORE | MSG_DONTWAIT | MSG_NOSIGNAL | MSG_CMSG_COMPAT | MSG_SPLICE_PAGES
/// | MSG_EOR` with `EOPNOTSUPP`. Those sockets take
/// [`set_default_msg_flags(0)`](SocketWrapper::set_default_msg_flags).
pub const DEFAULT_MSG_FLAGS: u32 = libc::MSG_WAITALL as u32;

/// A `msghdr` and the iovec array it points at. Boxed and never moved after
/// construction: the kernel reads both until the op's terminal CQE, so on
/// cancellation the whole box goes to keepalive as one piece.
pub(super) struct VectoredMsg {
    pub(super) msghdr: libc::msghdr,
    /// Read by the kernel through `msghdr.msg_iov`, never by us.
    #[allow(dead_code)]
    iovecs: Vec<libc::iovec>,
}

impl VectoredMsg {
    /// Covers `bufs[first..]`, skipping `offset` bytes of `bufs[first]`.
    pub(super) fn new<B: IoBuf>(bufs: &[B], first: usize, offset: usize) -> Box<Self> {
        let mut msg = Box::new(VectoredMsg {
            msghdr: unsafe { std::mem::zeroed() },
            iovecs: build_iovecs(bufs, first, offset),
        });
        msg.msghdr.msg_iov = msg.iovecs.as_mut_ptr();
        msg.msghdr.msg_iovlen = msg.iovecs.len() as _;
        msg
    }
}

/// The iovec array for `bufs[first..]`, skipping `offset` bytes of
/// `bufs[first]` and stopping at `IOV_MAX` entries. Empty chunks are dropped:
/// a zero-length segment fails a whole `SENDMSG` with `EFAULT`.
fn build_iovecs<B: IoBuf>(bufs: &[B], first: usize, offset: usize) -> Vec<libc::iovec> {
    let mut iovecs = Vec::with_capacity(bufs.len().saturating_sub(first).min(IOV_MAX));
    for (i, b) in bufs.iter().enumerate().skip(first) {
        if iovecs.len() == IOV_MAX {
            break;
        }
        let skip = if i == first { offset } else { 0 };
        let len = b.bytes_init() - skip;
        if len == 0 {
            continue;
        }
        iovecs.push(libc::iovec {
            iov_base: unsafe { (b.stable_ptr() as *mut u8).add(skip) } as *mut _,
            iov_len: len,
        });
    }
    iovecs
}

/// Cancellation-safe vectored send (`IORING_OP_SENDMSG`): takes ownership of
/// every chunk, returns the chunk list on completion. Sends under the socket's
/// [`default_msg_flags`](SocketWrapper::default_msg_flags), so a short send is
/// retried inside the kernel unless that default was changed. If the future is
/// dropped mid-flight, both the chunks AND the kernel-visible `msghdr` are
/// moved into the driver's keepalive slot so the kernel can finish reading them
/// after the cancel CQE.
pub fn socket_send_vectored<B: IoBuf>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
) -> WriteVectoredSocket<B> {
    submit_from(socket, bufs, 0, 0, socket.default_msg_flags())
}

/// As above, overriding the socket's default `msg_flags`. `0` restores
/// short-write behaviour.
pub fn socket_send_vectored_with_flags<B: IoBuf>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
    msg_flags: u32,
) -> WriteVectoredSocket<B> {
    submit_from(socket, bufs, 0, 0, msg_flags)
}

/// Covers `bufs[first..]`, skipping `offset` bytes of `bufs[first]`.
pub(super) fn submit_from<B: IoBuf>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
    first: usize,
    offset: usize,
    msg_flags: u32,
) -> WriteVectoredSocket<B> {
    let msg = VectoredMsg::new(&bufs, first, offset);
    WriteVectoredSocket {
        socket: socket.share(),
        id: None,
        bufs: Some(bufs),
        msg: Some(msg),
        msg_flags,
    }
}

pub struct WriteVectoredSocket<B: IoBuf> {
    socket: OpSocket,
    id: Option<OneshotToken>,
    bufs: Option<Vec<B>>,
    msg: Option<Box<VectoredMsg>>,
    msg_flags: u32,
}

impl<B: IoBuf> WriteVectoredSocket<B> {
    fn register_to_driver(&mut self) {
        let msg = self.msg.as_ref().expect("msg taken before submit");
        let entry = opcode::SendMsg::new(Fd(self.socket.fd()), &msg.msghdr)
            .flags(self.msg_flags)
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
        match IO_DRIVER.with(|d| d.poll_op(&mut self.id, waker)) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(io_result) => {
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
        if let Some(token) = self.id.take() {
            // Three layers of memory the kernel reads: the `msghdr`, the iovec
            // array it names, and each iovec's `iov_base` pointing into a B.
            // The first two travel together in the box; the chunks only need
            // keeping when B's data does not live forever (`NEEDS_KEEPALIVE`,
            // e.g. `&'static [u8]` lives in .rodata) — the const-evaluated
            // branch saves moving the Vec<B>.
            let msg = self.msg.take().expect("msg taken");
            let keep: Box<dyn Any> = if B::NEEDS_KEEPALIVE {
                Box::new((self.bufs.take(), msg))
            } else {
                Box::new(msg)
            };
            let keep = Some(keep);
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
    send_vectored_all_with_flags(socket, bufs, socket.default_msg_flags()).await
}

/// As [`send_vectored_all`], overriding the socket's default `msg_flags` for
/// this call only. `0` costs at most extra submissions on a short send — this
/// loop retries either way.
pub async fn send_vectored_all_with_flags<B: IoBuf>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
    msg_flags: u32,
) -> (std::io::Result<()>, Vec<B>) {
    send_all_with(socket, bufs, move |s, b, first, offset| {
        submit_from(s, b, first, offset, msg_flags)
    })
    .await
}

/// The `_all` loop shared by the gather sends. Each iteration submits at most
/// `IOV_MAX` chunks from the first unsent byte on. The caller's `Vec` is
/// handed straight to each submission and back: the unsent frontier is two
/// integers, never a re-split chunk list.
pub(super) async fn send_all_with<B, S, Fut>(
    socket: &SocketWrapper,
    mut bufs: Vec<B>,
    submit: S,
) -> (std::io::Result<()>, Vec<B>)
where
    B: IoBuf,
    S: Fn(&SocketWrapper, Vec<B>, usize, usize) -> Fut,
    Fut: Future<Output = (std::io::Result<usize>, Vec<B>)>,
{
    let total: usize = bufs.iter().map(|b| b.bytes_init()).sum();
    if total == 0 {
        return (Ok(()), bufs);
    }

    let mut sent = 0usize;
    let mut first = 0usize;
    let mut offset = 0usize;

    while sent < total {
        let (result, back) = submit(socket, bufs, first, offset).await;
        bufs = back;
        match result {
            Ok(0) => return (Err(std::io::ErrorKind::WriteZero.into()), bufs),
            Ok(n) => {
                sent += n;
                if sent >= total {
                    break;
                }
                (first, offset) = advance(&bufs, first, offset, n);
            }
            Err(e) => return (Err(e), bufs),
        }
    }
    (Ok(()), bufs)
}

/// Consume `n` bytes from `bufs[first..]` starting at `offset`; returns the
/// first chunk with bytes left to send and how far into it the next send starts.
pub(super) fn advance<B: IoBuf>(
    bufs: &[B],
    mut first: usize,
    mut offset: usize,
    mut n: usize,
) -> (usize, usize) {
    while n > 0 {
        let avail = bufs[first].bytes_init() - offset;
        if n >= avail {
            n -= avail;
            first += 1;
            offset = 0;
        } else {
            offset += n;
            n = 0;
        }
    }
    while first < bufs.len() && bufs[first].bytes_init() == offset {
        first += 1;
        offset = 0;
    }
    (first, offset)
}
