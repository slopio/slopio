use io_uring::opcode::{self};
use io_uring::types::Fd;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::io_driver::IO_DRIVER;

use super::SocketWrapper;

pub struct ClassicRecv<'a> {
    fd: i32,
    id: Option<usize>,
    buffer: &'a mut [u8],
    len: usize,
}

impl<'a> ClassicRecv<'a> {
    fn from_socket(socket: &SocketWrapper, buffer: &'a mut [u8], len: usize) -> Self {
        Self {
            fd: socket.fd(),
            id: None,
            len,
            buffer,
        }
    }

    fn register_to_driver(&mut self) {
        let entry =
            opcode::Recv::new(Fd(self.fd), self.buffer.as_mut_ptr(), self.len as u32).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token as usize);
    }
}

pub fn socket_recv<'a>(
    socket: &SocketWrapper,
    buffer: &'a mut [u8],
    len: usize,
) -> ClassicRecv<'a> {
    ClassicRecv::<'a>::from_socket(socket, buffer, len)
}

impl<'a> Future for ClassicRecv<'a> {
    type Output = std::io::Result<usize>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.id.is_none() {
            self.register_to_driver();
        }

        let waker = cx.waker();
        let id = self.id.unwrap();

        match IO_DRIVER.with(|d| d.poll_op(id, waker)) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(io_result) => {
                self.id = None;
                Poll::Ready(match io_result.result {
                    bytes_read @ 0.. => Ok(bytes_read as usize),
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                })
            }
        }
    }
}

impl Drop for ClassicRecv<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            IO_DRIVER.with(|d| d.cancel_single_op(id))
        }
    }
}
