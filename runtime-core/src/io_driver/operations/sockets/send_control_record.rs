use io_uring::{opcode, types::Fd};
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

const SOL_TLS: libc::c_int = 282;
const TLS_SET_RECORD_TYPE: libc::c_int = 1;

#[repr(C)]
struct ControlBuf {
    words: [usize; 4],
}

/// Heap-stable kernel-visible state for an in-flight `SendMsg` with a cmsg
/// payload. `msghdr.msg_iov` points into `iovec`, which points into `payload`;
/// `msghdr.msg_control` points into `ctrl.words`. Boxed so the address stays
/// stable across future moves and outlives the future when cancelled.
#[allow(dead_code)]
struct ControlRecordState<B> {
    msghdr: libc::msghdr,
    iovec: libc::iovec,
    ctrl: ControlBuf,
    payload: B,
}

/// Cancellation-safe: owns the payload, which is kept alive with the msghdr
/// until the cancelled CQE arrives if the future is dropped mid-flight.
pub fn socket_send_control_record<B: IoBuf>(
    socket: &SocketWrapper,
    record_type: u8,
    payload: B,
) -> WriteControlRecord<B> {
    let state = Box::new(ControlRecordState {
        msghdr: unsafe { std::mem::zeroed() },
        iovec: unsafe { std::mem::zeroed() },
        ctrl: ControlBuf { words: [0; 4] },
        payload,
    });
    WriteControlRecord {
        socket: socket.share(),
        id: None,
        record_type,
        state: Some(state),
    }
}

pub struct WriteControlRecord<B: IoBuf> {
    socket: OpSocket,
    id: Option<OneshotToken>,
    record_type: u8,
    state: Option<Box<ControlRecordState<B>>>,
}

impl<B: IoBuf> WriteControlRecord<B> {
    fn register_to_driver(&mut self) {
        let state = self.state.as_mut().expect("state present");

        state.iovec = libc::iovec {
            iov_base: state.payload.stable_ptr() as *mut _,
            iov_len: state.payload.bytes_init(),
        };

        let ctrl_ptr = state.ctrl.words.as_mut_ptr() as *mut u8;
        let cmsg_space = unsafe { libc::CMSG_SPACE(1) } as libc::size_t;

        unsafe {
            let mut probe: libc::msghdr = std::mem::zeroed();
            probe.msg_control = ctrl_ptr as *mut _;
            probe.msg_controllen = cmsg_space as _;
            let cmsg = libc::CMSG_FIRSTHDR(&probe);
            (*cmsg).cmsg_level = SOL_TLS;
            (*cmsg).cmsg_type = TLS_SET_RECORD_TYPE;
            (*cmsg).cmsg_len = libc::CMSG_LEN(1) as _;
            *libc::CMSG_DATA(cmsg) = self.record_type;
        }

        let iovec_ptr = &mut state.iovec as *mut libc::iovec;
        state.msghdr = libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: iovec_ptr,
            msg_iovlen: 1,
            msg_control: ctrl_ptr as *mut _,
            msg_controllen: cmsg_space as _,
            msg_flags: 0,
        };

        let entry = opcode::SendMsg::new(Fd(self.socket.fd()), &state.msghdr).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<B: IoBuf> Future for WriteControlRecord<B> {
    type Output = std::io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.id.is_none() {
            self.register_to_driver();
        }
        let waker = cx.waker();
        match IO_DRIVER.with(|d| d.poll_op(&mut self.id, waker)) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(io_result) => match io_result.result {
                n @ 0.. => Poll::Ready(Ok(n as usize)),
                err => Poll::Ready(Err(std::io::Error::from_raw_os_error(-err))),
            },
        }
    }
}

impl<B: IoBuf> Drop for WriteControlRecord<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id.take() {
            // Move the boxed msghdr/iovec/ctrl/payload into keepalive — kernel
            // may still dereference msghdr → iovec/ctrl until cancel completes.
            let keep = self.state.take().map(|s| s as Box<dyn Any>);
            let parked = Some(self.socket.park());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep, parked));
        }
    }
}
