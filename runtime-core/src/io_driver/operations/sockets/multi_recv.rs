use io_uring::{cqueue::buffer_select, opcode, types::Fd};
use runtime_streams::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::io_driver::{
    BorrowedBuffer, BufGroup, IO_DRIVER, MultiOpQueue, MultiPollResult, MultishotCompletionEvent,
    operations::{IoOP, MultishotToken},
};

pub struct MultiRecv {
    id: Option<MultishotToken>,
    fd: i32,
    bgid: u16,
    reads: MultiOpQueue,
}

#[derive(Debug)]
pub enum CompletionError {
    Eof { flags: u32 },
    ConnectionReset,
    BrokenPipe,
    Cancelled,
    Other(i32),
}

#[allow(unreachable_patterns)]
impl CompletionError {
    pub fn can_continue(&self) -> bool {
        match self {
            Self::Eof { .. } => false,
            Self::ConnectionReset => false,
            Self::BrokenPipe => false,
            Self::Cancelled => true, // Cancellation is recoverable - we'll resubmit
            Self::Other(code) => matches!(-code, libc::EINTR | libc::EAGAIN | libc::EWOULDBLOCK),
        }
    }

    pub fn from_code(result: i32, flags: u32) -> Self {
        match result {
            0 => Self::Eof { flags },
            r if r == -libc::ECONNRESET => Self::ConnectionReset,
            r if r == -libc::EPIPE => Self::BrokenPipe,
            r if r == -libc::ECANCELED => Self::Cancelled,
            _ => Self::Other(result),
        }
    }
}

impl MultiRecv {
    pub fn new(fd: i32, group: BufGroup) -> Self {
        let bgid = group.bgid();
        let mut multi_recv = Self {
            id: None,
            fd,
            bgid,
            reads: Default::default(),
        };
        multi_recv.register_to_driver();
        multi_recv
    }

    fn try_parse(
        &self,
        event: MultishotCompletionEvent,
    ) -> Result<BorrowedBuffer, CompletionError> {
        if event.result <= 0 {
            return Err(CompletionError::from_code(event.result, event.flags));
        }
        let len = event.result as usize;
        let buffer_id = buffer_select(event.flags).unwrap();
        let buf = IO_DRIVER.with(|d| d.get_buffer(self.bgid, buffer_id, len));
        Ok(buf)
    }
}

impl Drop for MultiRecv {
    fn drop(&mut self) {
        self.driver_cleanup();
    }
}

impl Unpin for MultiRecv {}

impl MultiRecv {
    fn try_drain_one(&mut self) -> Poll<Option<BorrowedBuffer>> {
        while let Some(completion_event) = self.reads.pop() {
            match self.try_parse(completion_event) {
                Ok(buf) => return Poll::Ready(Some(buf)),
                Err(e) if e.can_continue() => continue,
                Err(CompletionError::Other(r)) if r == -libc::ENOBUFS => {
                    // buffer ring exhausted
                    return Poll::Ready(None);
                }
                Err(_) => return Poll::Ready(None),
            }
        }
        Poll::Pending
    }
}

impl Stream for MultiRecv {
    type Item = BorrowedBuffer;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(result) = self.try_drain_one() {
            return Poll::Ready(result);
        }

        let waker = cx.waker();
        let token = self.id.unwrap();

        match IO_DRIVER.with(|d| d.poll_op_multishot(token, waker, &mut self.reads)) {
            MultiPollResult::Pending => Poll::Pending,
            MultiPollResult::Ready => {
                match self.try_drain_one() {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => {
                        // Queue had only skippable errors, check if we need resubmit
                        if let MultiPollResult::NeedsResubmit =
                            IO_DRIVER.with(|d| d.poll_op_multishot(token, waker, &mut self.reads))
                        {

                            let entry = self.to_entry();
                            IO_DRIVER.with(|d| d.resubmit_multishot(token, entry, waker));
                        }
                        Poll::Pending
                    }
                }
            }
            MultiPollResult::NeedsResubmit => {

                let entry = self.to_entry();
                IO_DRIVER.with(|d| d.resubmit_multishot(token, entry, waker));
                Poll::Pending
            }
        }
    }
}

impl IoOP for MultiRecv {
    fn register_to_driver(&mut self) {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| d.push_multishot_op(entry, self.fd));
        self.id = Some(token);
    }

    fn to_entry(&self) -> io_uring::squeue::Entry {
        opcode::RecvMulti::new(Fd(self.fd), self.bgid).build()
    }

    fn driver_cleanup(&mut self) {
        if let Some(token) = self.id.take() {
            IO_DRIVER.with(|d| d.cancel_multishot_op(token, self.bgid));
        }
    }
}
