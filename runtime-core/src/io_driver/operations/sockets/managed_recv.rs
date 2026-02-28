use std::{
    pin::Pin,
    ptr::null,
    task::{Context, Poll},
};

use io_uring::{cqueue::buffer_select, opcode, types::Fd};

use crate::io_driver::{
    BorrowedBuffer, BufGroup, IO_DRIVER, OpMetadata,
    operations::sockets::SocketWrapper,
};

pub struct ManagedRecv {
    id: Option<usize>,
    fd: i32,
    bgid: u16,
}

pub fn socket_managed_recv(socket: &SocketWrapper, group: BufGroup) -> ManagedRecv {
    ManagedRecv::from_socket(socket, group)
}

impl ManagedRecv {
    fn from_socket(socket: &SocketWrapper, group: BufGroup) -> Self {
        Self {
            id: None,
            fd: socket.fd(),
            bgid: group.bgid(),
        }
    }

    fn register_to_driver(&mut self) {
        let entry = opcode::Recv::new(Fd(self.fd), null::<u8>() as _, 0)
            .buf_group(self.bgid)
            .build()
            .flags(io_uring::squeue::Flags::BUFFER_SELECT);
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token as usize);
    }

    fn parse_completion(&self, event: OpMetadata) -> std::io::Result<BorrowedBuffer> {
        if event.result <= 0 {
            return Err(std::io::Error::from_raw_os_error(-event.result));
        }
        let len = event.result as usize;
        let buffer_id = buffer_select(event.flags).unwrap();
        Ok(IO_DRIVER.with(|d| d.get_buffer(self.bgid, buffer_id, len)))
    }
}

impl Future for ManagedRecv {
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
                Poll::Ready(self.parse_completion(io_result))
            }
        }
    }
}

impl Drop for ManagedRecv {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            IO_DRIVER.with(|d| d.cancel_single_op(id, Some(self.bgid)))
        }
    }
}
