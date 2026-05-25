use io_uring::{
    opcode::{self},
    types::Fd,
};
use std::any::Any;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::io_driver::{
    IO_DRIVER,
    operations::{IoBuf, OneshotToken, sockets::SocketWrapper},
};

pub fn socket_send<'a>(socket: &SocketWrapper, data: &'a [u8]) -> WriteSocket<'a> {
    WriteSocket::from_socket(socket, data)
}

/// Cancellation-safe send: takes ownership of the buffer, returns it on
/// completion. If the future is dropped mid-flight, the buffer is moved into
/// the driver's keepalive slot and only freed once the cancelled CQE arrives
/// — so the kernel never reads from freed memory.
///
/// Use this in `select!` / timeout / anywhere the future might be dropped
/// before it completes. For await-to-completion code paths, [`socket_send`]
/// (which takes `&[u8]`) is fine and avoids the ownership transfer.
pub fn socket_send_owned<B: IoBuf>(socket: &SocketWrapper, buf: B) -> WriteSocketOwned<B> {
    WriteSocketOwned {
        fd: socket.fd(),
        id: None,
        buf: Some(buf),
    }
}

pub struct WriteSocketOwned<B: IoBuf> {
    fd: i32,
    id: Option<OneshotToken>,
    buf: Option<B>,
}

impl<B: IoBuf> WriteSocketOwned<B> {
    fn register_to_driver(&mut self) {
        let buf = self.buf.as_ref().expect("buf taken before submit");
        let entry =
            opcode::Send::new(Fd(self.fd), buf.stable_ptr(), buf.bytes_init() as u32).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        self.id = Some(token);
    }
}

impl<B: IoBuf> Future for WriteSocketOwned<B> {
    type Output = (std::io::Result<usize>, B);

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
                let buf = self.buf.take().expect("buf already taken");
                let result = match io_result.result {
                    n @ 0.. => Ok(n as usize),
                    err => Err(std::io::Error::from_raw_os_error(-err)),
                };
                Poll::Ready((result, buf))
            }
        }
    }
}

impl<B: IoBuf> Drop for WriteSocketOwned<B> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            // Move the buffer into the driver's keepalive slot. It stays alive
            // until the cancelled CQE arrives and frees the slab entry.
            let keep = self.buf.take().map(|b| Box::new(b) as Box<dyn Any>);
            IO_DRIVER.with(|d| d.cancel_single_op_keep(token, None, keep));
        }
    }
}

/// Send the entire buffer, looping over short writes until every byte is
/// queued or an error occurs. Returns `WriteZero` if the kernel ever
/// accepts zero bytes (which would otherwise spin forever).
pub async fn send_all(socket: &SocketWrapper, data: &[u8]) -> std::io::Result<()> {
    let mut remaining = data;
    while !remaining.is_empty() {
        let n = socket_send(socket, remaining).await?;
        if n == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        remaining = &remaining[n..];
    }
    Ok(())
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
        self.id = Some(token);
    }
}

pub struct WriteSocket<'a> {
    fd: i32,
    id: Option<OneshotToken>,
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

impl<'a> Drop for WriteSocket<'a> {
    fn drop(&mut self) {
        if let Some(token) = self.id {
            IO_DRIVER.with(|d| d.cancel_single_op(token, None))
        }
    }
}
