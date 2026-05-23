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

/// Send every byte across all iovecs, looping over short writes. The
/// fast path (kernel writes the whole list in one go) does no allocation;
/// only partial completions pay a single `Vec` copy to walk the remaining
/// iovecs. Returns `WriteZero` if the kernel ever accepts zero bytes.
pub async fn send_vectored_all<'a>(
    socket: &SocketWrapper,
    bufs: &'a [IoSlice<'a>],
) -> std::io::Result<()> {
    let total: usize = bufs.iter().map(|s| s.len()).sum();
    let n = socket_send_vectored(socket, bufs).await?;
    if n == total {
        return Ok(());
    }
    if n == 0 {
        return Err(std::io::ErrorKind::WriteZero.into());
    }

    // Slow path: copy + advance + retry until drained.
    let mut owned: Vec<IoSlice<'a>> = bufs.to_vec();
    let mut slice: &mut [IoSlice<'a>] = &mut owned[..];
    IoSlice::advance_slices(&mut slice, n);
    while !slice.is_empty() {
        let m = socket_send_vectored(socket, slice).await?;
        if m == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut slice, m);
    }
    Ok(())
}
