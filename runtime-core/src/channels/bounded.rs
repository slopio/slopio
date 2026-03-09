use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

struct SharedState<T> {
    data: VecDeque<T>,
    capacity: usize,
    recv_waker: Option<Waker>,
    send_waker: Option<Waker>,
    sender_dropped: bool,
    receiver_dropped: bool,
}

impl<T> SharedState<T> {
    fn new(capacity: usize) -> Self {
        Self {
            data: VecDeque::new(),
            capacity,
            recv_waker: None,
            send_waker: None,
            sender_dropped: false,
            receiver_dropped: false,
        }
    }

    #[inline]
    fn wake_recv(&mut self) {
        if let Some(waker) = self.recv_waker.take() {
            waker.wake();
        }
    }

    #[inline]
    fn wake_send(&mut self) {
        if let Some(waker) = self.send_waker.take() {
            waker.wake();
        }
    }
}

struct Shared<T>(*mut UnsafeCell<SharedState<T>>);

impl<T> Shared<T> {
    fn new(capacity: usize) -> Self {
        Self(Box::into_raw(Box::new(UnsafeCell::new(SharedState::new(
            capacity,
        )))))
    }
}

pub struct Sender<T> {
    shared: Shared<T>,
}

pub struct Receiver<T> {
    shared: Shared<T>,
}

pub struct SendFuture<'a, T> {
    shared: &'a Shared<T>,
    element: Option<T>,
}

impl<T> Unpin for SendFuture<'_, T> {}

impl<'a, T> Future for SendFuture<'a, T> {
    type Output = Result<(), T>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let state = unsafe { &mut *(*this.shared.0).get() };

        if state.receiver_dropped {
            return Poll::Ready(Err(this.element.take().unwrap()));
        }

        if state.data.len() < state.capacity {
            state.data.push_back(this.element.take().unwrap());
            state.wake_recv();
            return Poll::Ready(Ok(()));
        }

        match &state.send_waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => state.send_waker = Some(cx.waker().clone()),
        }

        Poll::Pending
    }
}

impl<T> Sender<T> {
    #[inline]
    pub fn send(&self, element: T) -> SendFuture<'_, T> {
        SendFuture {
            shared: &self.shared,
            element: Some(element),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let state = unsafe { &mut *(*self.shared.0).get() };
        state.sender_dropped = true;
        if state.receiver_dropped {
            drop(unsafe { Box::from_raw(self.shared.0) });
        } else {
            state.wake_recv();
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let state = unsafe { &mut *(*self.shared.0).get() };
        state.receiver_dropped = true;
        state.data.clear();
        if state.sender_dropped {
            drop(unsafe { Box::from_raw(self.shared.0) });
        } else {
            state.wake_send();
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
            state.wake_send();
            return Poll::Ready(Some(e));
        }

        if state.sender_dropped {
            return Poll::Ready(None);
        }

        match &state.recv_waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => state.recv_waker = Some(cx.waker().clone()),
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
            state.wake_send();
            return Poll::Ready(Some(items));
        }

        if state.sender_dropped {
            return Poll::Ready(None);
        }

        match &state.recv_waker {
            Some(w) if w.will_wake(cx.waker()) => {}
            _ => state.recv_waker = Some(cx.waker().clone()),
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

#[inline]
pub fn bounded_channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let shared = Shared::new(capacity);
    (
        Sender {
            shared: Shared(shared.0),
        },
        Receiver { shared },
    )
}
