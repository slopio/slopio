use std::{
    marker::PhantomData,
    pin::Pin,
    ptr::null,
    task::{Context, Poll},
};

use io_uring::{cqueue::buffer_select, opcode, types::Fd};

use crate::io_driver::{
    BorrowedBuffer, BufRingSpec, IO_DRIVER, OpMetadata,
    operations::{
        OneshotToken,
        sockets::{OpSocket, SocketWrapper},
    },
};

pub struct ManagedRecv<S: BufRingSpec> {
    id: Option<OneshotToken>,
    socket: OpSocket,
    _spec: PhantomData<S>,
}

pub fn socket_managed_recv<S: BufRingSpec>(socket: &SocketWrapper) -> ManagedRecv<S> {
    ManagedRecv::from_socket(socket)
}

impl<S: BufRingSpec> ManagedRecv<S> {
    fn from_socket(socket: &SocketWrapper) -> Self {
        Self {
            id: None,
            socket: socket.share(),
            _spec: PhantomData,
        }
    }

    fn register_to_driver(&mut self) {
        let entry = opcode::Recv::new(Fd(self.socket.fd()), null::<u8>() as _, 0)
            .buf_group(S::BGID)
            .build()
            .flags(io_uring::squeue::Flags::BUFFER_SELECT);
        let token = IO_DRIVER.with(|d| d.push_buffered_op(entry, S::BGID));
        self.id = Some(token);
    }

    fn parse_completion(&self, event: OpMetadata) -> std::io::Result<BorrowedBuffer<S>> {
        if event.result <= 0 {
            // The flag alone says a buffer was consumed, whatever the result.
            if let Some(bid) = buffer_select(event.flags) {
                S::recycle_bid(bid);
            }
            return Err(std::io::Error::from_raw_os_error(-event.result));
        }
        let len = event.result as usize;
        let buffer_id = buffer_select(event.flags).unwrap();
        Ok(S::make_buffer(buffer_id, len))
    }
}

impl<S: BufRingSpec> Unpin for ManagedRecv<S> {}

impl<S: BufRingSpec> Future for ManagedRecv<S> {
    type Output = std::io::Result<BorrowedBuffer<S>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.id.is_none() {
            self.register_to_driver();
        }

        let waker = cx.waker();
        let token = self.id.unwrap();

        match IO_DRIVER.with(|d| d.poll_op(token, waker)) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(io_result) => {
                self.id = None;
                Poll::Ready(self.parse_completion(io_result))
            }
        }
    }
}

impl<S: BufRingSpec> Drop for ManagedRecv<S> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            let parked = Some(self.socket.park());
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, Some(S::BGID), None, parked))
        }
    }
}
