use std::{future::Future, marker::PhantomData};

use io_uring::{opcode, types::Fd};

use crate::io_driver::{
    IO_DRIVER,
    operations::files::{AsyncFileWrite, OpenedFile},
};

impl AsyncFileWrite for OpenedFile {
    type Writer<'a> = WriteFile<'a>;

    fn write<'a>(&self, data: &'a [u8]) -> Self::Writer<'a> {
        let mut wf = WriteFile {
            fd: self.fd,
            id: None,
            phantom: PhantomData,
        };
        let entry = opcode::Write::new(Fd(wf.fd), data.as_ptr(), data.len() as u32).build();
        let token = IO_DRIVER.with(|d| d.push_op(entry));
        wf.id = Some(token as usize);
        wf
    }
}

pub struct WriteFile<'a> {
    pub(crate) fd: i32,
    pub(crate) id: Option<usize>,
    phantom: PhantomData<&'a ()>,
}

impl<'a> Future for WriteFile<'a> {
    type Output = std::io::Result<usize>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let waker = cx.waker();
        let id = self.id.unwrap();
        match IO_DRIVER.with(|d| d.poll_op(id, waker)) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(io_result) => std::task::Poll::Ready(match io_result.result {
                bytes_written @ 0.. => Ok(bytes_written as usize),
                err => Err(std::io::Error::from_raw_os_error(-err)),
            }),
        }
    }
}

impl<'a> Drop for WriteFile<'a> {
    fn drop(&mut self) {}
}
