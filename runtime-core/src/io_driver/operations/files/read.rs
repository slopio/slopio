use std::{future::Future, marker::PhantomData};

use io_uring::{opcode, types::Fd};

use crate::io_driver::{
    IO_DRIVER,
    operations::{OneshotToken, files::{AsyncFileRead, OpenedFile}},
};

impl AsyncFileRead for OpenedFile {
    type Reader<'a> = ReadFile<'a>;

    fn read<'a>(&self, buffer: &'a mut [u8], len: u32) -> Self::Reader<'a> {
        let mut rf = ReadFile {
            fd: self.fd,
            id: None,
            phantom: PhantomData,
        };
        let entry = opcode::Read::new(Fd(rf.fd), buffer.as_mut_ptr(), len).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        rf.id = Some(token);
        rf
    }
}

pub struct ReadFile<'a> {
    pub(crate) fd: i32,
    pub(crate) id: Option<OneshotToken>,
    phantom: PhantomData<&'a ()>,
}

impl<'a> Future for ReadFile<'a> {
    type Output = std::io::Result<usize>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let waker = cx.waker();
        let token = self.id.unwrap();
        match IO_DRIVER.with(|d| d.poll_op(token, waker)) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(io_result) => std::task::Poll::Ready(match io_result.result {
                bytes_read @ 0.. => Ok(bytes_read as usize),
                err => Err(std::io::Error::from_raw_os_error(-err)),
            }),
        }
    }
}

impl<'a> Drop for ReadFile<'a> {
    fn drop(&mut self) {}
}
