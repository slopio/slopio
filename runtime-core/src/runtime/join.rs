use pin_project::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::Context;

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
        if let WrappedFut::Future(f) = this {
            match unsafe { Pin::new_unchecked(f).poll(cx) } {
                std::task::Poll::Ready(result) => *this = WrappedFut::Result(result),
                std::task::Poll::Pending => {}
            };
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

/// Generates `joinN`: drives N heterogeneous futures concurrently, completing
/// with a tuple of all outputs once every one is ready. Reuses [`WrappedFut`]
/// for per-future state, so it allocates nothing. Completion is decided by
/// re-reading each future's readiness every poll (never an accumulating
/// counter), which stays correct under coalesced wakeups.
macro_rules! impl_join_n {
    ($join_ty:ident, $join_fn:ident, $($F:ident => $f:ident),+) => {
        #[pin_project]
        pub struct $join_ty<$($F: Future),+> {
            $(#[pin] $f: WrappedFut<$F>,)+
        }

        pub fn $join_fn<$($F: Future),+>($($f: $F),+) -> $join_ty<$($F),+> {
            $join_ty {
                $($f: $f.into(),)+
            }
        }

        impl<$($F: Future),+> Future for $join_ty<$($F),+> {
            type Output = ($($F::Output,)+);

            fn poll(
                self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> std::task::Poll<Self::Output> {
                let mut this = self.project();
                $(let _ = this.$f.as_mut().poll(cx);)+
                if true $(&& this.$f.output_ready())+ {
                    std::task::Poll::Ready(($(this.$f.as_mut().take_output(),)+))
                } else {
                    std::task::Poll::Pending
                }
            }
        }
    };
}

impl_join_n!(Join3, join3, F1 => f1, F2 => f2, F3 => f3);
impl_join_n!(Join4, join4, F1 => f1, F2 => f2, F3 => f3, F4 => f4);
impl_join_n!(Join5, join5, F1 => f1, F2 => f2, F3 => f3, F4 => f4, F5 => f5);
impl_join_n!(Join6, join6, F1 => f1, F2 => f2, F3 => f3, F4 => f4, F5 => f5, F6 => f6);
impl_join_n!(Join7, join7, F1 => f1, F2 => f2, F3 => f3, F4 => f4, F5 => f5, F6 => f6, F7 => f7);
impl_join_n!(Join8, join8, F1 => f1, F2 => f2, F3 => f3, F4 => f4, F5 => f5, F6 => f6, F7 => f7, F8 => f8);
