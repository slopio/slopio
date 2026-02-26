use io_uring::{cqueue::buffer_select, opcode, types::Fd};
use io_uring_rb::buffer::Buffer;
use runtime_streams::Stream;
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::io_driver::{
    BUFFER_SIZE, IO_DRIVER, MultiOpQueue, MultiPollResult, MultishotCompletionEvent,
    operations::IoOP,
};

pub struct MultiRecv {
    id: Option<usize>,
    fd: i32,
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

impl TryFrom<MultishotCompletionEvent> for BorrowedBuffer {
    type Error = CompletionError;

    fn try_from(event: MultishotCompletionEvent) -> Result<Self, Self::Error> {
        if event.result <= 0 {
            return Err(CompletionError::from_code(event.result, event.flags));
        }
        let len = event.result as usize;
        let buffer_id = buffer_select(event.flags).unwrap();
        let inner = IO_DRIVER.with(|d| d.get_buffer(buffer_id, len));
        Ok(BorrowedBuffer { inner })
    }
}

pub struct BorrowedBuffer {
    inner: Buffer<BUFFER_SIZE>,
}

impl BorrowedBuffer {
    pub fn from_buffer(inner: Buffer<BUFFER_SIZE>) -> Self {
        Self { inner }
    }
}

impl AsRef<[u8]> for BorrowedBuffer {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl AsMut<[u8]> for BorrowedBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        self.inner.as_mut()
    }
}

impl MultiRecv {
    pub fn new(fd: i32) -> Self {
        let mut multi_recv = Self {
            id: None,
            fd,
            reads: Default::default(),
        };
        multi_recv.register_to_driver();
        multi_recv
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
            match completion_event.try_into() {
                Ok(r) => return Poll::Ready(Some(r)),
                Err(e) if e.can_continue() => continue,
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
        let id = self.id.unwrap();

        match IO_DRIVER.with(|d| d.poll_op_multishot(id, waker, &mut self.reads)) {
            MultiPollResult::Pending => Poll::Pending,
            MultiPollResult::Ready => {
                match self.try_drain_one() {
                    Poll::Ready(result) => Poll::Ready(result),
                    Poll::Pending => {
                        // Queue had only skippable errors, check if we need resubmit
                        if let MultiPollResult::NeedsResubmit =
                            IO_DRIVER.with(|d| d.poll_op_multishot(id, waker, &mut self.reads))
                        {
                            let entry = self.to_entry();
                            IO_DRIVER.with(|d| d.resubmit_multishot(id, entry, waker));
                        }
                        Poll::Pending
                    }
                }
            }
            MultiPollResult::NeedsResubmit => {
                let entry = self.to_entry();
                IO_DRIVER.with(|d| d.resubmit_multishot(id, entry, waker));
                Poll::Pending
            }
        }
    }
}

impl IoOP for MultiRecv {
    fn register_to_driver(&mut self) -> crate::io_driver::operations::IoDriverToken {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| d.push_multishot_op(entry, self.fd));
        self.id = Some(token as usize);
        token
    }

    fn to_entry(&self) -> io_uring::squeue::Entry {
        opcode::RecvMulti::new(Fd(self.fd), 0).build()
    }

    fn driver_cleanup(&mut self) {}
}

impl Drop for BorrowedBuffer {
    fn drop(&mut self) {
        IO_DRIVER.with(|d| d.recycle_buffer(&self.inner))
    }
}
