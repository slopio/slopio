use pin_project::pin_project;
use std::future::Future;

#[pin_project]
pub struct Select<F1: Future, F2: Future> {
    launched: bool,
    #[pin]
    f1: F1,
    #[pin]
    f2: F2,
}

pub enum SelectResult<T1, T2> {
    First(T1),
    Second(T2),
}

pub fn select<F1: Future, F2: Future>(f1: F1, f2: F2) -> Select<F1, F2> {
    Select {
        launched: false,
        f1,
        f2,
    }
}

impl<F1, F2> Future for Select<F1, F2>
where
    F1: Future,
    F2: Future,
{
    type Output = SelectResult<F1::Output, F2::Output>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        let this = self.project();
        if let std::task::Poll::Ready(res) = this.f1.poll(cx) {
            return std::task::Poll::Ready(SelectResult::First(res));
        }
        if let std::task::Poll::Ready(res) = this.f2.poll(cx) {
            return std::task::Poll::Ready(SelectResult::Second(res));
        }
        std::task::Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::main_thread;
    use std::future::ready;

    #[test]
    fn test_select_first_ready() {
        let result = main_thread(async {
            let fut1 = ready(42);
            let fut2 = async {
                // This would normally be pending
                100
            };
            select(fut1, fut2).await
        });

        match result {
            SelectResult::First(val) => assert_eq!(val, 42),
            SelectResult::Second(_) => panic!("Expected first future to complete"),
        }
    }

    #[test]
    fn test_select_second_ready() {
        let result = main_thread(async {
            let fut1 = async { 10 };
            let fut2 = ready("hello");
            select(fut1, fut2).await
        });

        // Either could complete first in this case
        match result {
            SelectResult::First(val) => assert_eq!(val, 10),
            SelectResult::Second(val) => assert_eq!(val, "hello"),
        }
    }

    #[test]
    fn test_select_different_types() {
        let result = main_thread(async {
            let fut1 = ready(vec![1, 2, 3]);
            let fut2 = ready(Some(99));
            select(fut1, fut2).await
        });

        match result {
            SelectResult::First(val) => assert_eq!(val, vec![1, 2, 3]),
            SelectResult::Second(val) => assert_eq!(val, Some(99)),
        }
    }

    #[test]
    fn test_select_with_computation() {
        let result = main_thread(async {
            let fut1 = async {
                let mut sum = 0;
                for i in 1..=5 {
                    sum += i;
                }
                sum
            };
            let fut2 = async { "fast" };
            select(fut1, fut2).await
        });

        match result {
            SelectResult::First(val) => assert_eq!(val, 15),
            SelectResult::Second(val) => assert_eq!(val, "fast"),
        }
    }
}
