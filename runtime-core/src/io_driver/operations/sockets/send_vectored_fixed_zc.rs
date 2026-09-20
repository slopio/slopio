use std::any::Any;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::{opcode, types::Fd};

use super::send_vectored::{VectoredMsg, send_all_with};
use crate::io_driver::{
    IO_DRIVER, PooledBuf, SLAB_BUF_INDEX,
    operations::{
        OneshotToken,
        sockets::{OpSocket, SocketWrapper},
    },
};

const IORING_RECVSEND_FIXED_BUF: u16 = 1 << 2;

/// DANGER, io-uring upgrades: `SendMsgZc` has no `buf_index` setter, so this
/// relies on `build()` leaving it zeroed and on `ioprio` reaching the SQE — the
/// crate's encoding, not its API. `buf_index` unions with `buf_group`; a crate
/// that starts writing it would send from the wrong region. The crate side is
/// pinned by `sendmsg_zc_sqe_encoding` in `tests/fixed_send_vectored_zc.rs`.
const _: () = assert!(
    SLAB_BUF_INDEX.get() == 0,
    "SendMsgZc cannot set buf_index; the slab must be registered as region 0"
);

/// Zero-copy gather send from pre-registered buffers: one submission for every
/// chunk, no per-request page pinning. All chunks must come from the same
/// [`FixedSendBufPool`]; at most `IOV_MAX` non-empty ones per call. Resolves on
/// the zero-copy notification, so slots go back to the pool only once the
/// kernel is done. See [`send_vectored_fixed_all_zc`] to cover more chunks or
/// retry a short send.
pub fn socket_send_vectored_fixed_zc(
    socket: &SocketWrapper,
    bufs: Vec<PooledBuf>,
) -> WriteVectoredFixedSocketZc {
    submit_from(socket, bufs, 0, 0, socket.default_msg_flags())
}

/// As above, overriding the socket's default `msg_flags`. `0` restores
/// short-write behaviour.
pub fn socket_send_vectored_fixed_zc_with_flags(
    socket: &SocketWrapper,
    bufs: Vec<PooledBuf>,
    msg_flags: u32,
) -> WriteVectoredFixedSocketZc {
    submit_from(socket, bufs, 0, 0, msg_flags)
}

/// Covers `bufs[first..]`, skipping `offset` bytes of `bufs[first]`.
fn submit_from(
    socket: &SocketWrapper,
    bufs: Vec<PooledBuf>,
    first: usize,
    offset: usize,
    msg_flags: u32,
) -> WriteVectoredFixedSocketZc {
    let msg = VectoredMsg::new(&bufs, first, offset);

    WriteVectoredFixedSocketZc {
        socket: socket.share(),
        id: None,
        bufs: Some(bufs),
        msg: Some(msg),
        msg_flags,
    }
}

pub struct WriteVectoredFixedSocketZc {
    socket: OpSocket,
    id: Option<OneshotToken>,
    bufs: Option<Vec<PooledBuf>>,
    msg: Option<Box<VectoredMsg>>,
    msg_flags: u32,
}

impl WriteVectoredFixedSocketZc {
    fn register_to_driver(&mut self) {
        let msg = self.msg.as_ref().expect("msg taken before submit");
        // buf_index stays 0 (zeroed SQE) — the slab's region, see the const assert.
        let entry = opcode::SendMsgZc::new(Fd(self.socket.fd()), &msg.msghdr)
            .ioprio(IORING_RECVSEND_FIXED_BUF)
            .flags(self.msg_flags)
            .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl Future for WriteVectoredFixedSocketZc {
    type Output = (io::Result<usize>, Vec<PooledBuf>);

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
                    err => Err(io::Error::from_raw_os_error(-err)),
                };
                Poll::Ready((result, bufs))
            }
        }
    }
}

impl Drop for WriteVectoredFixedSocketZc {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // Kernel reads the iovec array and the slots; hold both to the final CQE.
            let msg = self.msg.take().expect("msg taken");
            let keep: Box<dyn Any> = Box::new((self.bufs.take(), msg));
            let parked = Some(self.socket.park());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, Some(keep), parked));
        }
    }
}

/// Zero-copy gather send of every byte across all slots, looping over short
/// writes and over the `IOV_MAX` chunk limit. Returns the slots to the caller.
pub async fn send_vectored_fixed_all_zc(
    socket: &SocketWrapper,
    bufs: Vec<PooledBuf>,
) -> (io::Result<()>, Vec<PooledBuf>) {
    let msg_flags = socket.default_msg_flags();
    send_all_with(socket, bufs, move |s, b, first, offset| {
        submit_from(s, b, first, offset, msg_flags)
    })
    .await
}
