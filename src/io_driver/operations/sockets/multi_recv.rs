use io_uring::{opcode, types::Fd};
use std::{
    ffi::c_int,
    pin::Pin,
    task::{Context, Poll},
};

use crate::io_driver::{
    IO_DRIVER, MultishotCompletionEvent,
    operations::{IoOP, Stream},
};

pub struct MultiRecv {
    id: Option<usize>,
    fd: i32,
    reads: Vec<MultishotCompletionEvent>,
}

impl From<MultishotCompletionEvent> for Read {
    fn from(value: MultishotCompletionEvent) -> Self {
        // Extract buffer ID from flags (upper 16 bits)
        // IORING_CQE_F_BUFFER flag indicates buffer ID is present
        let buffer_id = (value.flags >> 16) as u16;
        let len = value.fd as usize; // For recv, result is bytes count

        // Get the actual buffer slice from the driver
        let data = IO_DRIVER.with(|d| d.get_buffer(buffer_id, len));

        Read { buffer_id, data }
    }
}

pub struct Read {
    buffer_id: u16,
    data: &'static [u8],
}

impl Drop for Read {
    fn drop(&mut self) {
        todo!() //give back the buffer to io_uring
    }
}

impl MultiRecv {
    pub fn new(fd: i32) -> std::io::Result<Self> {
        let mut multi_recv = Self {
            id: None,
            fd,
            reads: Vec::new(),
        };
        multi_recv.register_to_driver();
        Ok(multi_recv)
    }
}

impl Drop for MultiRecv {
    fn drop(&mut self) {
        self.free_driver();
    }
}
impl Stream for MultiRecv {
    type Item = Read;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let waker = cx.waker();
        let id = self.id.unwrap();
        if let Some(event) = self.reads.pop() {
            return Poll::Ready(Some(event.into()));
        }
        match IO_DRIVER.with(|d| d.poll_op_multishot(id, waker, &mut self.reads)) {
            std::task::Poll::Pending => {
                return std::task::Poll::Pending;
            }
            std::task::Poll::Ready(_) => {}
        }
        if let Some(event) = self.reads.pop() {
            Poll::Ready(Some(event.into()))
        } else {
            Poll::Pending
        }
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
        opcode::RecvMulti::new(Fd(self.fd), 2).build()
    }

    fn free_driver(&mut self) {
        unsafe {
            libc::close(self.fd as c_int);
        }
    }
}
