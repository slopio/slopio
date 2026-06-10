use std::task::Waker;

#[derive(Default)]
pub struct TimerStorage {
    inner: slab::Slab<Timer>,
}

enum Timer {
    Waiting(Waker),
    Done,
    Cancelled,
}

impl TimerStorage {
    pub(crate) fn create(&mut self, waker: &Waker) -> usize {
        self.inner.insert(Timer::Waiting(waker.clone()))
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    pub(crate) fn drop(&mut self, id: usize) {
        let timer = self.inner.get_mut(id).unwrap();
        match timer {
            Timer::Waiting(_) => {
                *timer = Timer::Cancelled;
                return;
            }
            Timer::Done => {}
            Timer::Cancelled => return,
        }
        self.inner.remove(id);
    }

    pub(crate) fn poll(&mut self, id: usize, waker: &Waker) -> std::task::Poll<()> {
        let timer = self.inner.get_mut(id).unwrap();
        match timer {
            Timer::Waiting(r_waker) => {
                if !r_waker.will_wake(waker) {
                    *r_waker = waker.clone();
                }
                std::task::Poll::Pending
            }
            // Fired: the id was already drained from its bucket by `wake`,
            // so the slot can be released here. Callers forget their id once
            // they observe `Ready`.
            Timer::Done => {
                self.inner.remove(id);
                std::task::Poll::Ready(())
            }
            // Cancelled ids are still parked in a bucket; the slot is freed
            // when the bucket fires (`wake`). Removing it here would let the
            // slab reuse the id while the bucket still references it.
            Timer::Cancelled => std::task::Poll::Ready(()),
        }
    }

    pub(crate) fn wake(&mut self, id: usize) {
        let timer = self.inner.get_mut(id).unwrap();
        match timer {
            Timer::Waiting(waker) => {
                waker.wake_by_ref();
                *timer = Timer::Done;
                return;
            }
            Timer::Done => unreachable!(),
            Timer::Cancelled => {}
        }
        self.inner.remove(id);
    }
}
