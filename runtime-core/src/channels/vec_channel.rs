use std::{
    cell::UnsafeCell,
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll, Waker},
};

struct SharedState<T> {
    data: Vec<T>,
    waker: Option<Waker>,
    sender_dropped: bool,
    receiver_dropped: bool,
}

impl<T> SharedState<T> {
    #[inline]
    fn new() -> Self {
        Self {
            data: Vec::new(),
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
    pub fn push(&self, element: T) {
        let state = unsafe { &mut *(*self.shared.0).get() };
        if state.receiver_dropped {
            return;
        }
        let was_empty = state.data.is_empty();
        state.data.push(element);
        if was_empty {
            state.wake();
        }
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
        let was_empty = state.data.is_empty();
        state.data.extend_from_slice(slice);
        if was_empty {
            state.wake();
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
            state.wake();
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
        }
    }
}

pub struct RecvFuture<'a, T> {
    shared: &'a Shared<T>,
}

impl<'a, T> Future for RecvFuture<'a, T> {
    type Output = Option<Vec<T>>;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let state = unsafe { &mut *(*self.shared.0).get() };

        if !state.data.is_empty() {
            return Poll::Ready(Some(mem::take(&mut state.data)));
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
