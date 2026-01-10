use std::task::Context;

use pin_project::pin_project;
use std::future::Future;

#[pin_project]
pub struct JoinHandle<F1, F2>
where
    F1: Future,
    F2: Future,
{
    #[pin]
    fut1: F1,
    #[pin]
    fut2: F2,
    fu1_res: Option<F1::Output>,
    fu2_res: Option<F2::Output>,
}

pub fn join<F1, F2>(f1: F1, f2: F2) -> JoinHandle<F1, F2>
where
    F1: Future,
    F2: Future,
{
    JoinHandle {
        fut1: f1,
        fut2: f2,
        fu1_res: None,
        fu2_res: None,
    }
}

impl<F1, F2> Future for JoinHandle<F1, F2>
where
    F1: Future,
    F2: Future,
{
    type Output = (F1::Output, F2::Output);

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        let this = self.project();

        if this.fu1_res.is_none() {
            match this.fut1.poll(cx) {
                std::task::Poll::Ready(output) => {
                    *this.fu1_res = Some(output);
                }
                std::task::Poll::Pending => {}
            }
        }

        if this.fu2_res.is_none() {
            match this.fut2.poll(cx) {
                std::task::Poll::Ready(output) => {
                    *this.fu2_res = Some(output);
                }
                std::task::Poll::Pending => {}
            }
        }

        if let (Some(res1), Some(res2)) = (this.fu1_res.take(), this.fu2_res.take()) {
            std::task::Poll::Ready((res1, res2))
        } else {
            std::task::Poll::Pending
        }
    }
}
