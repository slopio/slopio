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
}

pub enum CompletionError {
    Eof,
    ConnectionReset,
    BrokenPipe,
    Other(i32),
}

impl CompletionError {
    fn from_code(code: i32) -> Self {
        match code {
            0 => Self::Eof,
            -104 => Self::ConnectionReset, // ECONNRESET
            -32 => Self::BrokenPipe,       // EPIPE
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

impl Stream for MultiRecv {
    type Item = Read;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(completion_event) = self.reads.pop_front() {
            return Poll::Ready(completion_event.try_into().ok());
        }

        let (waker, id) = (cx.waker(), self.id.unwrap());
        if IO_DRIVER
            .with(|d| d.poll_op_multishot(id, waker, &mut self.reads))
            .is_pending()
        {
            return std::task::Poll::Pending;
        }

        if let Some(completion_event) = self.reads.pop_front() {
            return Poll::Ready(completion_event.try_into().ok());
        }
        Poll::Pending
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
