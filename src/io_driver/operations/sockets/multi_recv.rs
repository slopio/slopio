use io_uring::{cqueue::buffer_select, opcode, types::Fd};
use io_uring_buf_ring::BorrowedBuffer;
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

use crate::io_driver::{
    IO_DRIVER, MultishotCompletionEvent,
    operations::{
        IoOP, Stream,
        sockets::{MultiAsyncRead, SocketWrapper},
    },
};

pub struct MultiRecv {
    id: Option<usize>,
    fd: i32,
    reads: VecDeque<MultishotCompletionEvent>,
    reads_this_batch: usize,
}

#[derive(Debug)]
pub enum CompletionError {
    Eof,
    ConnectionReset,
    BrokenPipe,
    Other(i32),
}

#[allow(unreachable_patterns)]
impl CompletionError {
    pub fn can_continue(&self) -> bool {
        match self {
            Self::Eof => false,
            Self::ConnectionReset => false,
            Self::BrokenPipe => false,
            Self::Other(code) => matches!(-code, libc::EINTR | libc::EAGAIN | libc::EWOULDBLOCK),
        }
    }

    fn from_code(code: i32) -> Self {
        match code {
            0 => Self::Eof,
            -104 => Self::ConnectionReset,
            -32 => Self::BrokenPipe,
            _ => Self::Other(code),
        }
    }
}

impl TryFrom<MultishotCompletionEvent> for Read {
    type Error = CompletionError;

    fn try_from(event: MultishotCompletionEvent) -> Result<Self, Self::Error> {
        if event.result <= 0 {
            return Err(CompletionError::from_code(event.result));
        }
        let len = event.result;
        let buffer_id = buffer_select(event.flags).unwrap();
        let data = IO_DRIVER.with(|d| d.get_buffer(buffer_id, len as usize));
        Ok(Read { inner: data })
    }
}

pub struct Read {
    inner: BorrowedBuffer<'static, Vec<u8>>,
}

impl AsRef<[u8]> for Read {
    fn as_ref(&self) -> &[u8] {
        self.inner.as_ref()
    }
}

impl MultiRecv {
    pub fn new(fd: i32) -> Self {
        let mut multi_recv = Self {
            id: None,
            fd,
            reads: Default::default(),
            reads_this_batch: 0,
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
    fn try_drain_one(&mut self, cx: &mut Context<'_>) -> Poll<Option<Read>> {
        const MAX_READS_PER_POLL: usize = 32;

        while let Some(completion_event) = self.reads.pop_front() {
            match completion_event.try_into() {
                Ok(r) => {
                    self.reads_this_batch += 1;
                    // If we've hit the batch limit and there's more to read, yield
                    if self.reads_this_batch >= MAX_READS_PER_POLL && !self.reads.is_empty() {
                        self.reads_this_batch = 0;
                        cx.waker().wake_by_ref();
                    }
                    return Poll::Ready(Some(r));
                }
                Err(e) if e.can_continue() => continue,
                _ => return Poll::Ready(None),
            }
        }
        Poll::Pending
    }
}

impl Stream for MultiRecv {
    type Item = Read;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(result) = self.try_drain_one(cx) {
            return Poll::Ready(result);
        }

        let (waker, id) = (cx.waker(), self.id.unwrap());
        if IO_DRIVER
            .with(|d| d.poll_op_multishot(id, waker, &mut self.reads))
            .is_pending()
        {
            return Poll::Pending;
        }
        self.reads_this_batch = 0;
        self.try_drain_one(cx)
    }
}

impl IoOP for MultiRecv {
    fn register_to_driver(&mut self) -> crate::io_driver::operations::IoDriverToken {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| d.push_multishot_op(entry));
        self.id = Some(token as usize);
        token
    }

    fn to_entry(&self) -> io_uring::squeue::Entry {
        opcode::RecvMulti::new(Fd(self.fd), 0).build()
    }

    fn driver_cleanup(&mut self) {}
}

impl MultiAsyncRead for SocketWrapper {
    type MultiRecv = MultiRecv;

    fn multi_recv(&self) -> Self::MultiRecv {
        MultiRecv::new(self.fd)
    }
}
