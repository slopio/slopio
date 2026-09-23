use std::{
    cell::UnsafeCell,
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
};

use super::wait_list::{Left, WaitList};

struct SharedState<T> {
    data: Vec<T>,
    receivers: WaitList,
    sender_dropped: bool,
    receiver_dropped: bool,
}

impl<T> SharedState<T> {
    #[inline]
    fn new() -> Self {
        Self {
            data: Vec::new(),
            receivers: WaitList::new(),
            sender_dropped: false,
            receiver_dropped: false,
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
    pub fn push(&self, element: T) {
        let state = unsafe { &mut *(*self.shared.0).get() };
        if state.receiver_dropped {
            return;
        }
        state.data.push(element);
        state.receivers.wake_one();
    }

    pub fn extend(&self, slice: &[T])
    where
        T: Copy,
    {
        if slice.is_empty() {
            return;
        }
        let state = unsafe { &mut *(*self.shared.0).get() };
        if state.receiver_dropped {
            return;
        }
        state.data.extend_from_slice(slice);
        state.receivers.wake_one();
    }
}

impl<T> Drop for Sender<T> {
    fn drop(&mut self) {
        let state = unsafe { &mut *(*self.shared.0).get() };
        state.sender_dropped = true;
        if state.receiver_dropped {
            drop(unsafe { Box::from_raw(self.shared.0) });
        } else {
            state.receivers.wake_all();
        }
    }
}

impl<T> Receiver<T> {
    // Commented out, not deleted, pending a check of the repos outside this
    // workspace — nothing in here has ever called it, but it is reachable
    // through the `runtime` facade.
    //
    // It is unsound. The `&mut Vec<T>` points into state the `Sender` shares,
    // and a sender is a separate object, so the borrow checker permits this:
    //
    //     let v = rx.get_mut();
    //     tx.push(1);            // second &mut to the same SharedState
    //     v.push(2);
    //
    // Two live `&mut` to one allocation, from safe code. Quieter than the
    // `peek` that used to sit above it — that one reallocated the buffer out
    // from under the slice it had handed out, so it failed natively too, while
    // this shows no symptom at all until Miri calls it a forbidden reborrow.
    //
    // #[inline]
    // pub fn get_mut(&mut self) -> &mut Vec<T> {
    //     let state = unsafe { &mut *(*self.shared.0).get() };
    //     &mut state.data
    // }

    #[inline]
    pub fn take(&self) -> Vec<T> {
        let state = unsafe { &mut *(*self.shared.0).get() };
        mem::take(&mut state.data)
    }

    #[inline]
    pub fn recv(&self) -> RecvFuture<'_, T> {
        RecvFuture {
            shared: &self.shared,
            waiter: None,
        }
    }
}

impl<T> Drop for Receiver<T> {
    fn drop(&mut self) {
        let state = unsafe { &mut *(*self.shared.0).get() };
        state.receiver_dropped = true;
        // Dropped last: an item may own the Sender, whose drop frees the state.
        let data = mem::take(&mut state.data);
        if state.sender_dropped {
            drop(unsafe { Box::from_raw(self.shared.0) });
        }
        drop(data);
    }
}

pub struct RecvFuture<'a, T> {
    shared: &'a Shared<T>,
    waiter: Option<u64>,
}

impl<'a, T> Future for RecvFuture<'a, T> {
    type Output = Option<Vec<T>>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let state = unsafe { &mut *(*this.shared.0).get() };

        if !state.data.is_empty() {
            state.receivers.leave(&mut this.waiter);
            return Poll::Ready(Some(mem::take(&mut state.data)));
        }

        if state.sender_dropped {
            state.receivers.leave(&mut this.waiter);
            return Poll::Ready(None);
        }

        state.receivers.park(&mut this.waiter, cx.waker());
        Poll::Pending
    }
}

impl<T> Drop for RecvFuture<'_, T> {
    fn drop(&mut self) {
        let state = unsafe { &mut *(*self.shared.0).get() };
        // Woken for a batch it will never take: hand the wake on.
        if state.receivers.leave(&mut self.waiter) == Left::Woken && !state.data.is_empty() {
            state.receivers.wake_one();
        }
    }
}

#[inline]
pub fn vec_channel<T>() -> (Sender<T>, Receiver<T>) {
    let shared = Shared::new();
    (
        Sender {
            shared: Shared(shared.0),
        },
        Receiver { shared },
    )
}
