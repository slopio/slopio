use std::{
    io::{IoSliceMut, Result},
    pin::Pin,
    task::Context,
};

use io_uring_rb::buffer::Buffer;

trait Read {
    fn poll_read_provided_buffers(
        self: Pin<&mut Self>,
        cx: &mut Context,
    ) -> std::task::Poll<Result<usize>>;

    fn poll_read_classic(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> std::task::Poll<Result<usize>>;

    fn poll_read_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [IoSliceMut],
    ) -> std::task::Poll<Result<usize>>;
}

trait ReadExt: Read {
    fn read_classic(buffer: &mut [u8], len: usize) -> Result<Buffer>;
    fn read(len: usize) -> Result<Buffer>;
}
