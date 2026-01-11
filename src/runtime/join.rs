use std::future::Future;
use std::pin::Pin;
use std::task::Context;

use pin_project::pin_project;

#[pin_project]
pub struct JoinHandle<F1, F2>
where
    F1: Future,
    F2: Future,
{
    #[pin]
    f1: WrappedFut<F1>,
    #[pin]
    f2: WrappedFut<F2>,
}

enum WrappedFut<T: Future> {
    Future(T),
    Result(T::Output),
    Done,
}

impl<T: Future> WrappedFut<T> {
    fn take_output(self: Pin<&mut Self>) -> T::Output {
        let this = unsafe { self.get_unchecked_mut() };
        let result = std::mem::replace(this, WrappedFut::Done);
        match result {
            WrappedFut::Result(res) => res,
            _ => unreachable!(),
        }
    }

    fn output_ready(&self) -> bool {
        match self {
            WrappedFut::Future(_) => false,
            WrappedFut::Result(_) => true,
            WrappedFut::Done => unreachable!(),
        }
    }
}

impl<T: Future> Future for WrappedFut<T> {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = unsafe { self.as_mut().get_unchecked_mut() };
        match this {
            WrappedFut::Future(f) => {
                match unsafe { Pin::new_unchecked(f).poll(cx) } {
                    std::task::Poll::Ready(result) => *this = WrappedFut::Result(result),
                    std::task::Poll::Pending => {}
                };
            }
            _ => {}
        };
        std::task::Poll::Ready(())
    }
}

impl<F: Future> From<F> for WrappedFut<F> {
    fn from(val: F) -> Self {
        WrappedFut::Future(val)
    }
}

pub fn join<F1, F2>(f1: F1, f2: F2) -> JoinHandle<F1, F2>
where
    F1: Future,
    F2: Future,
{
    JoinHandle {
        f1: f1.into(),
        f2: f2.into(),
    }
}

impl<F1, F2> Future for JoinHandle<F1, F2>
where
    F1: Future,
    F2: Future,
{
    type Output = (F1::Output, F2::Output);

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        let mut this = self.project();
        let _ = this.f1.as_mut().poll(cx);
        let _ = this.f2.as_mut().poll(cx);
        if this.f1.output_ready() && this.f2.output_ready() {
            std::task::Poll::Ready((this.f1.take_output(), this.f2.take_output()))
        } else {
            std::task::Poll::Pending
        }
    }
}
