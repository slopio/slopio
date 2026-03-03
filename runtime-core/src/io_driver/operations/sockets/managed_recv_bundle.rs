use std::{
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode, types::Fd};

use crate::io_driver::{BorrowedBuffer, BufGroup, IO_DRIVER, operations::sockets::SocketWrapper};

pub struct ManagedRecvBundle {
    id: Option<usize>,
    fd: i32,
    bgid: u16,
}

pub fn socket_managed_recv_bundle(socket: &SocketWrapper, group: BufGroup) -> ManagedRecvBundle {
    ManagedRecvBundle::from_socket(socket, group)
}

impl ManagedRecvBundle {
    fn from_socket(socket: &SocketWrapper, group: BufGroup) -> Self {
        Self {
            id: None,
            fd: socket.fd(),
            bgid: group.bgid(),
        }
    }

    fn register_to_driver(&mut self) {
        let entry = opcode::RecvBundle::new(Fd(self.fd), self.bgid).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token as usize);
    }
}

impl Future for ManagedRecvBundle {
    type Output = std::io::Result<BorrowedBuffer>;

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
                if io_result.result <= 0 {
                    Poll::Ready(Err(std::io::Error::from_raw_os_error(-io_result.result)))
                } else {
                    use io_uring::cqueue::buffer_select;
                    let len = io_result.result as usize;
                    let buffer_id = buffer_select(io_result.flags).unwrap();
                    Poll::Ready(Ok(
                        IO_DRIVER.with(|d| d.get_buffer(self.bgid, buffer_id, len))
                    ))
                }
            }
        }
    }
}

impl Drop for ManagedRecvBundle {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            IO_DRIVER.with(|d| d.cancel_single_op(id, Some(self.bgid)))
        }
    }
}
