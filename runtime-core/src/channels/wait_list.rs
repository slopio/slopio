use std::{collections::VecDeque, task::Waker};

/// The futures parked on one side of a channel, woken in FIFO order.
/// Allocates only when a future first parks.
pub(super) struct WaitList {
    waiters: VecDeque<(u64, Waker)>,
    next_id: u64,
}

/// What [`WaitList::leave`] found for a future's slot.
#[derive(PartialEq)]
pub(super) enum Left {
    /// The future never parked, or already left.
    NotParked,
    /// Still queued: nothing was owed to it.
    Queued,
    /// Woken but gone before acting on it: the wake must be passed on.
    Woken,
}

impl WaitList {
    pub(super) fn new() -> Self {
        Self {
            waiters: VecDeque::new(),
            next_id: 0,
        }
    }

    /// Park the future owning `slot`, or refresh its waker if still queued.
    pub(super) fn park(&mut self, slot: &mut Option<u64>, waker: &Waker) {
        if let Some(id) = *slot
            && let Some((_, queued)) = self.waiters.iter_mut().find(|(i, _)| *i == id)
        {
            if !queued.will_wake(waker) {
                queued.clone_from(waker);
            }
            return;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.waiters.push_back((id, waker.clone()));
        *slot = Some(id);
    }

    /// Take the future owning `slot` out of the list.
    pub(super) fn leave(&mut self, slot: &mut Option<u64>) -> Left {
        let Some(id) = slot.take() else {
            return Left::NotParked;
        };
        match self.waiters.iter().position(|(i, _)| *i == id) {
            Some(pos) => {
                self.waiters.remove(pos);
                Left::Queued
            }
            None => Left::Woken,
        }
    }

    pub(super) fn wake_one(&mut self) {
        if let Some((_, waker)) = self.waiters.pop_front() {
            waker.wake();
        }
    }

    pub(super) fn wake_n(&mut self, n: usize) {
        for _ in 0..n {
            if self.waiters.is_empty() {
                return;
            }
            self.wake_one();
        }
    }

    pub(super) fn wake_all(&mut self) {
        while let Some((_, waker)) = self.waiters.pop_front() {
            waker.wake();
        }
    }
}
