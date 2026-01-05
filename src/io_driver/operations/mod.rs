use std::{
    pin::Pin,
    task::{Context, Poll},
};

use io_uring::squeue::Entry;

pub(crate) mod files;
pub(crate) mod sockets;

pub(crate) type IoDriverToken = u64;

trait IoOP: Drop {
    fn register_to_driver(&mut self) -> IoDriverToken;

    fn free_driver(&mut self);

    fn to_entry(&self) -> Entry;
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

impl<T: ?Sized + Stream + Unpin> Future for Next<'_, T> {
    type Output = Option<T::Item>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut *self.inner).poll_next(cx)
    }
}
