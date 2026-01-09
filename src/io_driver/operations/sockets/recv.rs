use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

use io_uring::opcode;
use io_uring::squeue::Entry;
use io_uring::types::Fd;

use crate::io_driver::operations::sockets::AsyncRead;
use crate::io_driver::{
    IO_DRIVER,
    operations::{IoDriverToken, IoOP},
};

use super::Socket;

pub struct RecvSocket<'a> {
    fd: i32,
    id: Option<usize>,
    _data: PhantomData<&'a ()>,
}

impl AsyncRead for Socket {
    type Reader<'a> = RecvSocket<'a>;

    fn recv<'a>(&self, buffer: &'a mut [u8], len: u32) -> Self::Reader<'a> {
        let mut rs = RecvSocket {
            fd: self.fd,
            id: None,
            _data: PhantomData,
        };
        let entry = opcode::Recv::new(Fd(rs.fd), buffer.as_mut_ptr(), len).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        rs.id = Some(token as usize);
        rs
    }
}

impl<'a> Future for RecvSocket<'a> {
    type Output = std::io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.id.is_none() {
            self.register_to_driver();
        }

        let waker = cx.waker();
        let id = self.id.unwrap();

        match IO_DRIVER.with(|d| d.poll_op(id, waker)) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(io_result) => Poll::Ready(match io_result.result {
                bytes_read @ 0.. => Ok(bytes_read as usize),
                err => Err(std::io::Error::from_raw_os_error(-err)),
            }),
        }
    }
}

impl<'a> Drop for RecvSocket<'a> {
    fn drop(&mut self) {}
}

impl<'a> IoOP for RecvSocket<'a> {
    fn register_to_driver(&mut self) -> IoDriverToken {
        unimplemented!()
    }

    fn to_entry(&self) -> Entry {
        unimplemented!() //todo need a new trait for those with buffer
    }

    fn free_driver(&mut self) {
        //no cleaning needed
    }
}
