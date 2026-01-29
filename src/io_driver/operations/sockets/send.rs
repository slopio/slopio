use io_uring::{
    opcode::{self},
    types::Fd,
};
use std::future::Future;

use crate::io_driver::{IO_DRIVER, operations::sockets::SocketWrapper};

pub fn socket_send<'a>(socket: &SocketWrapper, data: &'a [u8]) -> WriteSocket<'a> {
    WriteSocket::from_socket(socket, data)
}

impl<'a> WriteSocket<'a> {
    fn from_socket(socket: &SocketWrapper, data: &'a [u8]) -> Self {
        WriteSocket {
            fd: socket.fd(),
            id: None,
            data,
        }
    }

    fn register_to_driver(&mut self) {
        let entry =
            opcode::Send::new(Fd(self.fd), self.data.as_ptr(), self.data.len() as u32).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token as usize);
    }
}

pub struct WriteSocket<'a> {
    fd: i32,
    id: Option<usize>,
    data: &'a [u8],
}

impl<'a> Future for WriteSocket<'a> {
    type Output = std::io::Result<usize>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.id.is_none() {
            self.register_to_driver();
        }
        let waker = cx.waker();
        let id = self.id.unwrap();
        match IO_DRIVER.with(|d| d.poll_op(id, waker)) {
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

impl<'a> Drop for WriteSocket<'a> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            IO_DRIVER.with(|d| d.cancel_single_op(id))
        }
    }
}
