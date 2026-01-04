use std::{
    pin::{Pin, pin},
    task::{Context, Poll},
};

fn main() {
    let mut rt = runtime::runtime::RuntimeInner::default();
    let k = rt.block_on(async {
        let counter = Counter {
            current: 0,
            max: 10,
        };
        let mut counter = pin!(counter);
        while let Some(i) = counter.next().await {
            println!("{i}");
        }
    });
}

trait Stream {
    type Item;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>)
    -> std::task::Poll<Option<Self::Item>>;
}

struct Next<'l, T: ?Sized>
where
    T: Unpin,
{
    inner: &'l mut T,
}

impl<S: Stream + ?Sized> StreamExt for S {}

trait StreamExt: Stream {
    fn next(&mut self) -> Next<'_, Self>
    where
        Self: Unpin,
    {
        Next { inner: self }
    }
}

struct Counter {
    current: u32,
    max: u32,
}

impl<T: ?Sized + Stream + Unpin> Future for Next<'_, T> {
    type Output = Option<T::Item>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.inner).poll_next(cx)
    }
}

impl Stream for Counter {
    type Item = u32;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let mut me = self.as_mut();
        if me.current >= me.max {
            return Poll::Ready(None);
        }
        let old = me.current;
        me.current += 1;
        return Poll::Ready(Some(old));
    }
}
