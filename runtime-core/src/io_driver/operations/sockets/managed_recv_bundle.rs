use std::{
    marker::PhantomData,
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::{opcode, types::Fd};

use crate::io_driver::{
    BorrowedBuffer, BufRingSpec, IO_DRIVER,
    operations::{OneshotToken, sockets::SocketWrapper},
};

pub struct ManagedRecvBundle<S: BufRingSpec> {
    id: Option<OneshotToken>,
    fd: i32,
    _spec: PhantomData<S>,
}

pub fn socket_managed_recv_bundle<S: BufRingSpec>(
    socket: &SocketWrapper,
) -> ManagedRecvBundle<S> {
    ManagedRecvBundle::from_socket(socket)
}

impl<S: BufRingSpec> ManagedRecvBundle<S> {
    fn from_socket(socket: &SocketWrapper) -> Self {
        Self {
            id: None,
            fd: socket.fd(),
            _spec: PhantomData,
        }
    }

    fn register_to_driver(&mut self) {
        let entry = opcode::RecvBundle::new(Fd(self.fd), S::BGID).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<S: BufRingSpec> Unpin for ManagedRecvBundle<S> {}

impl<S: BufRingSpec> Future for ManagedRecvBundle<S> {
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
                if io_result.result <= 0 {
                    Poll::Ready(Err(std::io::Error::from_raw_os_error(-io_result.result)))
                } else {
                    use io_uring::cqueue::buffer_select;
                    let len = io_result.result as usize;
                    let buffer_id = buffer_select(io_result.flags).unwrap();
                    Poll::Ready(Ok(S::make_buffer(buffer_id, len)))
                }
            }
        }
    }
}

impl<S: BufRingSpec> Drop for ManagedRecvBundle<S> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            IO_DRIVER.with(|d| d.cancel_single_op(token, Some(S::BGID)))
        }
    }
}
