use crate::io_driver::IO_DRIVER;

pub struct Demo {}

pub struct DemoFut {
    id: usize,
}

impl Future for DemoFut {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        IO_DRIVER.with(|d| d.progress(self.id, cx.waker()))
    }
}

impl IntoFuture for Demo {
    type Output = ();

    type IntoFuture = DemoFut;

    fn into_future(self) -> Self::IntoFuture {
        let id = IO_DRIVER.with(|d| d.submit_op(super::OpCode));
        Self::IntoFuture { id }
    }
}
