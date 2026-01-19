use io_uring::{cqueue::buffer_select, opcode, types::Fd};
use io_uring_rb::{buffer::Buffer, buffers_range::BufferRange};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::io_driver::{
    IO_DRIVER, MultiOpQueue, MultishotCompletionEvent,
    operations::{
        IoOP, Stream,
        sockets::{MultiAsyncRead, SocketWrapper},
    },
};

pub struct MultiRecv {
    id: Option<usize>,
    fd: i32,
    reads: MultiOpQueue,
    reads_this_batch: usize,
}

#[derive(Debug)]
pub enum CompletionError {
    Eof { flags: u32 },
    ConnectionReset,
    BrokenPipe,
    Other(i32),
}

#[allow(unreachable_patterns)]
impl CompletionError {
    pub fn can_continue(&self) -> bool {
        match self {
            Self::Eof { .. } => false,
            Self::ConnectionReset => false,
            Self::BrokenPipe => false,
            Self::Other(code) => matches!(-code, libc::EINTR | libc::EAGAIN | libc::EWOULDBLOCK),
        }
    }

    fn from_code(MultishotCompletionEvent { result, flags }: MultishotCompletionEvent) -> Self {
        match result {
            0 => Self::Eof { flags },
            -104 => Self::ConnectionReset,
            -32 => Self::BrokenPipe,
            _ => Self::Other(result),
        }
    }
}

impl TryFrom<MultishotCompletionEvent> for Read {
    type Error = CompletionError;

    fn try_from(event: MultishotCompletionEvent) -> Result<Self, Self::Error> {
        if event.result <= 0 {
            return Err(CompletionError::from_code(event));
        }
        let len = event.result as usize;
        let buffer_id = buffer_select(event.flags).unwrap();
        let inner = IO_DRIVER.with(|d| d.get_buffer_range(buffer_id, len));
        Ok(Read { inner })
    }
}

pub struct Read {
    inner: BufferRange,
}

impl Drop for Read {
    fn drop(&mut self) {
        let inner = &mut self.inner;
        IO_DRIVER.with(|d| d.return_buffer_range(inner));
    }
}

impl Read {
    pub fn as_parts(&self) -> (&[u8], Option<&[u8]>) {
        self.inner.as_parts()
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

        while let Some(completion_event) = self.reads.pop() {
            match completion_event.try_into() {
                Ok(r) => {
                    self.reads_this_batch += 1;
                    if self.reads_this_batch >= MAX_READS_PER_POLL && self.reads.has_content() {
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
        println!("{:?}", self.reads);
        self.reads_this_batch = 0;
        self.try_drain_one(cx)
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
        opcode::RecvMultiBundle::new(Fd(self.fd), 0).build()
    }

    fn driver_cleanup(&mut self) {}
}

impl MultiAsyncRead for SocketWrapper {
    type MultiRecv = MultiRecv;

    fn multi_recv(&self) -> Self::MultiRecv {
        MultiRecv::new(self.fd)
    }
}
