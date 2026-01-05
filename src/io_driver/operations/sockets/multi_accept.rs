use std::{net::TcpListener, os::fd::IntoRawFd};

use io_uring::{opcode, types::Fd};

use crate::io_driver::{
    IO_DRIVER,
    operations::{IoOP, Stream},
};

struct MultiAccept {
    id: Option<usize>,
    fd: i32,
}

struct Socket {
    fd: i32,
}

impl MultiAccept {
    pub fn new(port: u16) -> std::io::Result<Self> {
        let tcp_listener = TcpListener::bind(("0.0.0.0", port))?;
        tcp_listener.set_nonblocking(true)?;
        let fd = tcp_listener.into_raw_fd();
        let mut multi_accept = Self { id: None, fd };
        multi_accept.register_to_driver();
        Ok(multi_accept)
    }
}

impl Stream for MultiAccept {
    type Item = Socket;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}

impl Drop for MultiAccept {
    fn drop(&mut self) {
        self.free_driver();
    }
}

impl IoOP for MultiAccept {
    fn register_to_driver(&mut self) -> crate::io_driver::operations::IoDriverToken {
        let entry = self.to_entry();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token as usize);
        token
    }

    fn to_entry(&self) -> io_uring::squeue::Entry {
        opcode::AcceptMulti::new(Fd(self.fd))
            .flags(libc::SOCK_NONBLOCK)
            .build()
    }

    fn free_driver(&mut self) {
        todo!()
    }
}
