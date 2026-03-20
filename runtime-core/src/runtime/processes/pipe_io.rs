use std::future::Future;
use std::marker::PhantomData;

use io_uring::{opcode, types::Fd};

use crate::io_driver::IO_DRIVER;
use crate::io_driver::operations::OneshotToken;

/// Async read from a pipe fd via io_uring.
pub struct ReadPipe<'a> {
    id: Option<OneshotToken>,
    _phantom: PhantomData<&'a mut [u8]>,
}

impl<'a> ReadPipe<'a> {
    pub fn new(fd: i32, buf: &'a mut [u8]) -> Self {
        let entry = opcode::Read::new(Fd(fd), buf.as_mut_ptr(), buf.len() as u32).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        Self {
            id: Some(token),
            _phantom: PhantomData,
        }
    }
}

impl<'a> Future for ReadPipe<'a> {
    type Output = std::io::Result<usize>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let waker = cx.waker();
        let token = self.id.unwrap();
        match IO_DRIVER.with(|d| d.poll_op(token, waker)) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(io_result) => {
                self.id = None;
                std::task::Poll::Ready(match io_result.result {
                    bytes_read @ 0.. => Ok(bytes_read as usize),
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                })
            }
        }
    }
}

impl<'a> Drop for ReadPipe<'a> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            IO_DRIVER.with(|d| d.cancel_single_op(token, None))
        }
    }
}

/// Async write to a pipe fd via io_uring.
pub struct WritePipe<'a> {
    id: Option<OneshotToken>,
    _phantom: PhantomData<&'a [u8]>,
}

impl<'a> WritePipe<'a> {
    pub fn new(fd: i32, buf: &'a [u8]) -> Self {
        let entry = opcode::Write::new(Fd(fd), buf.as_ptr(), buf.len() as u32).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        Self {
            id: Some(token),
            _phantom: PhantomData,
        }
    }
}

impl<'a> Future for WritePipe<'a> {
    type Output = std::io::Result<usize>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let waker = cx.waker();
        let token = self.id.unwrap();
        match IO_DRIVER.with(|d| d.poll_op(token, waker)) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(io_result) => {
                self.id = None;
                std::task::Poll::Ready(match io_result.result {
                    bytes_written @ 0.. => Ok(bytes_written as usize),
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                })
            }
        }
    }
}

impl<'a> Drop for WritePipe<'a> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            IO_DRIVER.with(|d| d.cancel_single_op(token, None))
        }
    }
}
