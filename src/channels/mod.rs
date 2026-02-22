use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

struct SharedState<T> {
    data: VecDeque<T>,
    waker: Option<Waker>,
    sender_dropped: bool,
    receiver_dropped: bool,
}

impl<T> SharedState<T> {
    #[inline]
    fn new() -> Self {
        Self {
            data: VecDeque::new(),
            waker: None,
            sender_dropped: false,
            receiver_dropped: false,
        }
    }

    #[inline]
    fn wake(&mut self) {
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }
}

struct Shared<T>(*mut UnsafeCell<SharedState<T>>);

impl<T> Shared<T> {
    fn new() -> Self {
        Self(Box::into_raw(Box::new(UnsafeCell::new(SharedState::new()))))
    }
}

pub struct Sender<T> {
    shared: Shared<T>,
}

pub struct Receiver<T> {
    shared: Shared<T>,
}

impl<T> Sender<T> {
    #[inline]
    pub fn send(&self, element: T) {
        let state = unsafe { &mut *(*self.shared.0).get() };
        if state.receiver_dropped {
            return;
        }
        let was_empty = state.data.is_empty();
        state.data.push_back(element);
        if was_empty {
            state.wake();
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let state = unsafe { &mut *(*self.shared.0).get() };
        state.sender_dropped = true;
        state.wake();
        if state.receiver_dropped {
            drop(unsafe { Box::from_raw(self.shared.0) });
        }
    }
}

#[inline]
pub fn unbounded_channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Shared::new();
    (
        Sender {
            shared: Shared(shared.0),
        },
        Receiver { shared },
    )
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let state = unsafe { &mut *(*self.shared.0).get() };
        state.receiver_dropped = true;
        state.data.clear();
        if state.sender_dropped {
            drop(unsafe { Box::from_raw(self.shared.0) });
        }
    }
}

pub struct RecvFuture<'a, T> {
    shared: &'a Shared<T>,
}

impl<'a, T> Future for RecvFuture<'a, T> {
    type Output = Option<T>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = unsafe { &mut *(*self.shared.0).get() };

        if let Some(e) = state.data.pop_front() {
            return Poll::Ready(Some(e));
        }

        if state.sender_dropped {
            return Poll::Ready(None);
        }

        match &state.waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => state.waker = Some(cx.waker().clone()),
        }

        Poll::Pending
    }
}

pub struct RecvAllFuture<'a, T> {
    shared: &'a Shared<T>,
}

impl<'a, T> Future for RecvAllFuture<'a, T> {
    type Output = Option<Vec<T>>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = unsafe { &mut *(*self.shared.0).get() };

        if !state.data.is_empty() {
            let items = state.data.drain(..).collect();
            return Poll::Ready(Some(items));
        }

        if state.sender_dropped {
            return Poll::Ready(None);
        }

        match &state.waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => state.waker = Some(cx.waker().clone()),
        }

        Poll::Pending
    }
}

impl<T> Receiver<T> {
    #[inline]
    pub fn recv(&self) -> RecvFuture<'_, T> {
        RecvFuture {
            shared: &self.shared,
        }
    }

    #[inline]
    pub fn recv_all(&self) -> RecvAllFuture<'_, T> {
        RecvAllFuture {
            shared: &self.shared,
        }
    }
}
