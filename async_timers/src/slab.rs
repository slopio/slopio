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
        let timers = self.inner.get_mut(id).unwrap();
        if let Timer::Waiting(r_waker) = timers {
            if !r_waker.will_wake(waker) {
                *r_waker = waker.clone();
            }
            return std::task::Poll::Pending;
        }
        std::task::Poll::Ready(())
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
