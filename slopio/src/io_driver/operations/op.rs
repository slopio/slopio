//! Single-shot ops built from an SQE outside the runtime, owning the memory it points into.

use std::any::Any;
use std::future::Future;
use std::io;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

use io_uring::squeue::Entry;

use crate::io_driver::{IO_DRIVER, operations::OneshotToken};

/// The memory an SQE points into, owned for as long as the kernel may use it.
///
/// # Safety
/// - Every pointer in [`entry`](Self::entry) targets memory owned by `self` that stays
///   valid, at the same address, when `self` moves, until `self` drops.
/// - An fd named by the entry stays open until `self` drops: hold its owner in `self`.
/// - The entry selects no provided buffer (`IOSQE_BUFFER_SELECT`) and is not linked.
/// - [`into_keepalive`](Self::into_keepalive) returns `None` only if dropping `self`
///   before the terminal CQE frees nothing the kernel uses.
pub unsafe trait OpState: 'static {
    /// A non-negative result is a new fd, closed by the driver if the op is dropped.
    const RESULT_IS_FD: bool = false;

    fn entry(&mut self) -> Entry;

    /// What the driver holds after a mid-flight drop; override to reuse an existing box.
    fn into_keepalive(self) -> Option<Box<dyn Any>>
    where
        Self: Sized,
    {
        Some(Box::new(self))
    }
}

/// A single-shot op owning its [`OpState`], submitted at first poll.
///
/// Resolves to the result and the state. Dropped in flight, it cancels and the
/// driver holds the state until the terminal CQE, whose result is lost (an fd is
/// closed if [`OpState::RESULT_IS_FD`]). To survive a lost race, select on `&mut op`
/// instead of moving it in.
///
/// `EBADF` resolves to an error like any other: the entry names its own fd, and the
/// kernel also answers it for an fd opened in the wrong mode. On an fd the runtime
/// vouches for, the driver panics instead.
pub struct Op<S: OpState> {
    state: Option<S>,
    token: Option<OneshotToken>,
    _local: PhantomData<*const ()>,
}

// `S` is never pinned.
impl<S: OpState> Unpin for Op<S> {}

impl<S: OpState> Op<S> {
    pub fn new(state: S) -> Self {
        Self {
            state: Some(state),
            token: None,
            _local: PhantomData,
        }
    }
}

impl<S: OpState> Future for Op<S> {
    type Output = (io::Result<u32>, S);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        if this.token.is_none() {
            let entry = this
                .state
                .as_mut()
                .expect("Op polled after completion")
                .entry();
            this.token = Some(IO_DRIVER.with(|d| d.push_caller_fd_op(entry)));
        }
        let meta = ready!(IO_DRIVER.with(|d| d.poll_op(&mut this.token, cx.waker())));
        let result = match meta.result {
            n @ 0.. => Ok(n as u32),
            err => Err(io::Error::from_raw_os_error(-err)),
        };
        Poll::Ready((
            result,
            this.state.take().expect("state present until Ready"),
        ))
    }
}

impl<S: OpState> Drop for Op<S> {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            let keep = self.state.take().and_then(S::into_keepalive);
            IO_DRIVER.with(|d| {
                if S::RESULT_IS_FD {
                    d.cancel_fd_producing_op(token, keep)
                } else {
                    d.cancel_single_op_keep(token, None, keep, None)
                }
            });
        }
    }
}
