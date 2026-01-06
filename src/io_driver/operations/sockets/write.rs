use std::{future::Future, marker::PhantomData};

use io_uring::{opcode, types::Fd};

use crate::io_driver::{
    IO_DRIVER,
    operations::sockets::{AsyncWrite, Socket},
};

impl AsyncWrite for Socket {
    type Writer<'a> = WriteSocket<'a>;

    fn send<'a>(&self, data: &'a [u8]) -> Self::Writer<'a> {
        let mut ws = WriteSocket {
            fd: self.fd,
            id: None,
            phantom: PhantomData,
        };
        let entry = opcode::Send::new(Fd(ws.fd), data.as_ptr(), data.len() as u32).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        ws.id = Some(token as usize);
        ws
    }
}

pub struct WriteSocket<'a> {
    pub(crate) fd: i32,
    pub(crate) id: Option<usize>,
    phantom: PhantomData<&'a ()>,
}

impl<'a> Future for WriteSocket<'a> {
    type Output = std::io::Result<usize>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let waker = cx.waker();
        let id = self.id.unwrap();
        match IO_DRIVER.with(|d| d.poll_op(id, waker)) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(io_result) => std::task::Poll::Ready(match io_result.result {
                bytes_written @ 0.. => Ok(bytes_written as usize),
                err => Err(std::io::Error::from_raw_os_error(-err)),
            }),
        }
    }
}

impl<'a> Drop for WriteSocket<'a> {
    fn drop(&mut self) {}
}
