use core::slice;
use io_uring::{cqueue::buffer_select, opcode, types::Fd};
use std::{
    pin::Pin,
    task::{Context, Poll},
};

use crate::io_driver::{
    IO_DRIVER, MultiOpQueue, MultishotCompletionEvent, RING_BUFFER_SIZE,
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
        let len = event.result as usize;
        let buffer_id = buffer_select(event.flags).unwrap();
        let data =
            IO_DRIVER.with(|d| d.get_buffer(buffer_id, std::cmp::min(len, RING_BUFFER_SIZE)));
        let ptr = data.as_ptr();
        std::mem::forget(data);
        Ok(Read {
            inner: ReadInner {
                buffer_id,
                ptr,
                len,
            },
        })
    }
}

struct ReadInner {
    buffer_id: u16,
    ptr: *const u8,
    len: usize,
}

impl Drop for ReadInner {
    fn drop(&mut self) {
        todo!()
    }
}

impl AsRef<[u8]> for ReadInner {
    fn as_ref(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }
}

pub struct Read {
    inner: ReadInner,
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
