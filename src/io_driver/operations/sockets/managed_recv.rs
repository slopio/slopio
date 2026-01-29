use std::{
    pin::Pin,
    ptr::null,
    task::{Context, Poll},
};

use io_uring::{cqueue::buffer_select, opcode, types::Fd};

use crate::io_driver::{
    IO_DRIVER, OpMetadata,
    operations::sockets::{SocketWrapper, multi_recv_bundle::Read},
};

pub struct ManagedRecv {
    id: Option<usize>,
    fd: i32,
}

pub fn socket_managed_recv(socket: &SocketWrapper) -> ManagedRecv {
    ManagedRecv::from_socket(socket)
}

impl ManagedRecv {
    fn from_socket(socket: &SocketWrapper) -> Self {
        Self {
            id: None,
            fd: socket.fd(),
        }
    }

    fn register_to_driver(&mut self) {
        let entry = opcode::Recv::new(Fd(self.fd), null::<u8>() as _, 0)
            .buf_group(0)
            .build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token as usize);
    }
}

impl TryFrom<OpMetadata> for Read {
    type Error = std::io::Error;

    fn try_from(event: OpMetadata) -> Result<Self, Self::Error> {
        if event.result <= 0 {
            return Err(std::io::Error::from_raw_os_error(-event.result));
        }
        let len = event.result as usize;
        let buffer_id = buffer_select(event.flags).unwrap();
        let inner = IO_DRIVER.with(|d| d.get_buffer(buffer_id, len));
        Ok(Read::from_buffer(inner))
    }
}

impl Future for ManagedRecv {
    type Output = std::io::Result<Read>;

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
                Poll::Ready(Read::try_from(io_result))
            }
        }
    }
}

impl Drop for ManagedRecv {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            IO_DRIVER.with(|d| d.cancel_single_op(id))
        }
    }
}
