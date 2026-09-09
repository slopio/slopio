use io_uring::{opcode, types::Fd};
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use super::send_vectored::{IOV_MAX, advance_chunks, unwrap_chunks};
use crate::io_driver::{
    IO_DRIVER,
    operations::{IoBuf, OneshotToken, Slice, sockets::{OpSocket, SocketWrapper}},
};

/// Boxed: `msghdr.msg_iov` points at the Vec's heap and the msghdr itself must not move.
struct VectoredZcState {
    msghdr: libc::msghdr,
    #[allow(dead_code)]
    iovecs: Vec<libc::iovec>,
}

/// Zero-copy gather send (`IORING_OP_SENDMSG_ZC`): every chunk goes out in
/// one submission straight from user memory; resolves on the kernel's
/// notification, when all chunks are free again. At most `IOV_MAX` chunks.
pub fn socket_send_vectored_zc<B: IoBuf>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
) -> WriteVectoredSocketZc<B> {
    let iovecs: Vec<libc::iovec> = bufs
        .iter()
        .map(|b| libc::iovec {
            iov_base: b.stable_ptr() as *mut _,
            iov_len: b.bytes_init(),
        })
        .collect();
    let mut state = Box::new(VectoredZcState {
        msghdr: unsafe { std::mem::zeroed() },
        iovecs,
    });
    state.msghdr.msg_iov = state.iovecs.as_mut_ptr();
    state.msghdr.msg_iovlen = state.iovecs.len() as _;
    WriteVectoredSocketZc {
        socket: socket.share(),
        id: None,
        bufs: Some(bufs),
        state: Some(state),
    }
}

pub struct WriteVectoredSocketZc<B: IoBuf> {
    socket: OpSocket,
    id: Option<OneshotToken>,
    bufs: Option<Vec<B>>,
    state: Option<Box<VectoredZcState>>,
}

impl<B: IoBuf> WriteVectoredSocketZc<B> {
    fn register_to_driver(&mut self) {
        let state = self.state.as_ref().expect("state taken before submit");
        let entry = opcode::SendMsgZc::new(Fd(self.socket.fd()), &state.msghdr).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<B: IoBuf> Future for WriteVectoredSocketZc<B> {
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

impl<B: IoBuf> Drop for WriteVectoredSocketZc<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // msghdr + iovecs are always kernel-read; chunks only when not `'static`.
            let state = self.state.take().expect("state taken");
            let keep: Box<dyn Any> = if B::NEEDS_KEEPALIVE {
                Box::new((self.bufs.take(), state))
            } else {
                Box::new(state)
            };
            let parked = Some(self.socket.park());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, Some(keep), parked));
        }
    }
}

/// Zero-copy gather send of every byte across all chunks, looping over short
/// writes and over the `IOV_MAX` chunk limit. Returns the original `Vec<B>`.
pub async fn send_vectored_all_zc<B: IoBuf>(
    socket: &SocketWrapper,
    bufs: Vec<B>,
) -> (std::io::Result<()>, Vec<B>) {
    let total: usize = bufs.iter().map(|b| b.bytes_init()).sum();
    if total == 0 {
        return (Ok(()), bufs);
    }

    let mut chunks: Vec<Slice<B>> = bufs.into_iter().map(|b| b.slice(..)).collect();
    let mut sent = 0usize;

    while sent < total {
        let tail = chunks.split_off(chunks.len().min(IOV_MAX));
        let (result, mut head) = socket_send_vectored_zc(socket, chunks).await;
        head.extend(tail);
        chunks = head;
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
