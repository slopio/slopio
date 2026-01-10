use pin_project::pin_project;

#[pin_project]
struct Select<F1: Future, F2: Future> {
    launched: bool,
    #[pin]
    f1: F1,
    #[pin]
    f2: F2,
    // res: Option<SelectResult<F1, F2>>,
}

enum SelectResult<T1, T2> {
    First(T1),
    Second(T2),
}

fn select<F1: Future, F2: Future>(f1: F1, f2: F2) -> Select<F1, F2> {
    Select {
        launched: false,
        f1,
        f2,
        // res: None,
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
        todo!()
    }
}
/*
 *  poll f1, poll2, set started
 *  awake :
 *  f1:ready? return f1::Output
 *  f2:ready? return f2::Output
 *
 */
