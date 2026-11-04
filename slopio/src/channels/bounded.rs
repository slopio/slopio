use std::{
    cell::UnsafeCell,
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use super::wait_list::{Left, WaitList};

struct SharedState<T> {
    data: VecDeque<T>,
    capacity: usize,
    receivers: WaitList,
    senders: WaitList,
    sender_count: usize,
    receiver_dropped: bool,
}

impl<T> SharedState<T> {
    fn new(capacity: usize) -> Self {
        Self {
            data: VecDeque::new(),
            capacity,
            receivers: WaitList::new(),
            senders: WaitList::new(),
            sender_count: 1,
            receiver_dropped: false,
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
    waiter: Option<u64>,
}

impl<T> Unpin for SendFuture<'_, T> {}

impl<'a, T> Future for SendFuture<'a, T> {
    type Output = Result<(), T>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let state = unsafe { &mut *(*this.shared.0).get() };

        if state.receiver_dropped {
            state.senders.leave(&mut this.waiter);
            return Poll::Ready(Err(this.element.take().unwrap()));
        }

        if state.data.len() < state.capacity {
            state.senders.leave(&mut this.waiter);
            state.data.push_back(this.element.take().unwrap());
            state.receivers.wake_one();
            return Poll::Ready(Ok(()));
        }

        state.senders.park(&mut this.waiter, cx.waker());
        Poll::Pending
    }
}

impl<T> Drop for SendFuture<'_, T> {
    fn drop(&mut self) {
        let state = unsafe { &mut *(*self.shared.0).get() };
        // Woken for a free slot it will never take: hand the wake on.
        if state.senders.leave(&mut self.waiter) == Left::Woken && state.data.len() < state.capacity
        {
            state.senders.wake_one();
        }
    }
}

impl<T> Sender<T> {
    #[inline]
    pub fn send(&self, element: T) -> SendFuture<'_, T> {
        SendFuture {
            shared: &self.shared,
            element: Some(element),
            waiter: None,
        }
    }
}

impl<T> Clone for Sender<T> {
    #[inline]
    fn clone(&self) -> Self {
        let state = unsafe { &mut *(*self.shared.0).get() };
        state.sender_count += 1;
        Self {
            shared: Shared(self.shared.0),
        }
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let state = unsafe { &mut *(*self.shared.0).get() };
        state.sender_count -= 1;
        if state.sender_count > 0 {
            return;
        }
        if state.receiver_dropped {
            drop(unsafe { Box::from_raw(self.shared.0) });
        } else {
            state.receivers.wake_all();
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let state = unsafe { &mut *(*self.shared.0).get() };
        state.receiver_dropped = true;
        // Dropped last: an item may own the last Sender, whose drop frees the state.
        let data = std::mem::take(&mut state.data);
        if state.sender_count == 0 {
            drop(unsafe { Box::from_raw(self.shared.0) });
        } else {
            state.senders.wake_all();
        }
        drop(data);
    }
}

pub struct RecvFuture<'a, T> {
    shared: &'a Shared<T>,
    waiter: Option<u64>,
}

impl<'a, T> Future for RecvFuture<'a, T> {
    type Output = Option<T>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let state = unsafe { &mut *(*this.shared.0).get() };

        if let Some(e) = state.data.pop_front() {
            state.receivers.leave(&mut this.waiter);
            state.senders.wake_one();
            return Poll::Ready(Some(e));
        }

        if state.sender_count == 0 {
            state.receivers.leave(&mut this.waiter);
            return Poll::Ready(None);
        }

        state.receivers.park(&mut this.waiter, cx.waker());
        Poll::Pending
    }
}

impl<T> Drop for RecvFuture<'_, T> {
    fn drop(&mut self) {
        pass_on_recv_wake(self.shared, &mut self.waiter);
    }
}

pub struct RecvAllFuture<'a, T> {
    shared: &'a Shared<T>,
    waiter: Option<u64>,
}

impl<'a, T> Future for RecvAllFuture<'a, T> {
    type Output = Option<Vec<T>>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let state = unsafe { &mut *(*this.shared.0).get() };

        if !state.data.is_empty() {
            state.receivers.leave(&mut this.waiter);
            let items: Vec<T> = state.data.drain(..).collect();
            state.senders.wake_n(items.len());
            return Poll::Ready(Some(items));
        }

        if state.sender_count == 0 {
            state.receivers.leave(&mut this.waiter);
            return Poll::Ready(None);
        }

        state.receivers.park(&mut this.waiter, cx.waker());
        Poll::Pending
    }
}

impl<T> Drop for RecvAllFuture<'_, T> {
    fn drop(&mut self) {
        pass_on_recv_wake(self.shared, &mut self.waiter);
    }
}

/// A receive future woken for an item it will never take hands the wake on.
fn pass_on_recv_wake<T>(shared: &Shared<T>, waiter: &mut Option<u64>) {
    let state = unsafe { &mut *(*shared.0).get() };
    if state.receivers.leave(waiter) == Left::Woken && !state.data.is_empty() {
        state.receivers.wake_one();
    }
}

impl<T> Receiver<T> {
    #[inline]
    pub fn recv(&self) -> RecvFuture<'_, T> {
        RecvFuture {
            shared: &self.shared,
            waiter: None,
        }
    }

    #[inline]
    pub fn recv_all(&self) -> RecvAllFuture<'_, T> {
        RecvAllFuture {
            shared: &self.shared,
            waiter: None,
        }
    }
}

/// Panics if `capacity` is 0: a send would never find room.
#[inline]
pub fn bounded_channel<T>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    assert!(capacity > 0, "bounded_channel capacity must be at least 1");
    let shared = Shared::new(capacity);
    (
        Sender {
            shared: Shared(shared.0),
        },
        Receiver { shared },
    )
}
