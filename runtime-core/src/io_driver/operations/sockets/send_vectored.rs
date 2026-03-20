use io_uring::{
    opcode::{self},
    types::Fd,
};
use std::future::Future;
use std::io::IoSlice;

use crate::io_driver::{IO_DRIVER, operations::{OneshotToken, sockets::SocketWrapper}};

pub fn socket_send_vectored<'a>(
    socket: &SocketWrapper,
    bufs: &'a [IoSlice<'a>],
) -> WriteVectoredSocket<'a> {
    WriteVectoredSocket {
        fd: socket.fd(),
        id: None,
        bufs,
    }
}

pub struct WriteVectoredSocket<'a> {
    fd: i32,
    id: Option<OneshotToken>,
    bufs: &'a [IoSlice<'a>],
}

impl<'a> WriteVectoredSocket<'a> {
    fn register_to_driver(&mut self) {
        let entry = opcode::Writev::new(
            Fd(self.fd),
            self.bufs.as_ptr() as *const libc::iovec,
            self.bufs.len() as u32,
        )
        .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<'a> Future for WriteVectoredSocket<'a> {
    type Output = std::io::Result<usize>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.id.is_none() {
            self.register_to_driver();
        }
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

impl<'a> Drop for WriteVectoredSocket<'a> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            IO_DRIVER.with(|d| d.cancel_single_op(token, None))
        }
    }
}
