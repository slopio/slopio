use std::{
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode, types::Fd};

use crate::io_driver::{
    IO_DRIVER,
    operations::sockets::{SocketWrapper, multi_recv::BorrowedBuffer},
};

pub struct ManagedRecvBundle {
    id: Option<usize>,
    fd: i32,
}

pub fn socket_managed_recv_bundle(socket: &SocketWrapper) -> ManagedRecvBundle {
    ManagedRecvBundle::from_socket(socket)
}

impl ManagedRecvBundle {
    fn from_socket(socket: &SocketWrapper) -> Self {
        Self {
            id: None,
            fd: socket.fd(),
        }
    }

    fn register_to_driver(&mut self) {
        let entry = opcode::RecvBundle::new(Fd(self.fd), 0).build();
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
                Poll::Ready(BorrowedBuffer::try_from(io_result))
            }
        }
    }
}

impl Drop for ManagedRecvBundle {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            IO_DRIVER.with(|d| d.cancel_single_op(id))
        }
    }
}
