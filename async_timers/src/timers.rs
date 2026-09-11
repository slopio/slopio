use std::collections::VecDeque;
use std::collections::vec_deque::Drain;
use std::task::{Poll, Waker};
use std::time::{Duration, Instant, SystemTime};

use crate::{DurationTooLong, MS_TICK};

struct Deadline {
    at: u64,
    count: u32,
}

impl Deadline {
    fn new(at: u64) -> Self {
        Self { at, count: 1 }
    }

    fn incr(&mut self) {
        self.count += 1;
    }
}

struct Deadlines {
    inner: VecDeque<Deadline>,
}

impl Deadlines {
    fn new() -> Self {
        Self {
            inner: VecDeque::new(),
        }
    }

    fn push(&mut self, at: u64) {
        match self.inner.back_mut() {
            Some(last) if last.at == at => last.incr(),
            _ => self.inner.push_back(Deadline::new(at)),
        }
    }
}

struct Wakers {
    inner: VecDeque<Waker>,
}

impl Wakers {
    fn new() -> Self {
        Self {
            inner: VecDeque::new(),
        }
    }

    fn push(&mut self, waker: Waker) {
        self.inner.push_back(waker);
    }

    fn pop(&mut self) -> Option<Waker> {
        self.inner.pop_front()
    }

    fn pop_n(&mut self, n: usize) -> Drain<'_, Waker> {
        self.inner.drain(..n.min(self.inner.len()))
    }
}

pub struct Timers {
    deadlines: Deadlines,
    duration_ticks: u64,
    ticks: u64,
}

#[allow(unused_variables)]
impl Timers {
    #[must_use]
    pub fn new(duration: Duration) -> Self {
        Self {
            deadlines: Deadlines::new(),
            duration_ticks: duration.as_millis() as u64 / MS_TICK,
            ticks: 0,
        }
    }

    fn push_deadline(&mut self) {
        self.deadlines.push(self.ticks + self.duration_ticks);
    }

    pub fn tick(&mut self) {
        self.tick_at(Instant::now(), SystemTime::now());
    }

    pub fn tick_at(&mut self, now: Instant, now_wall: SystemTime) {
        todo!()
    }

    pub fn now(&self) -> SystemTime {
        todo!()
    }

    pub fn active_timers(&self) -> usize {
        todo!()
    }

    pub fn init_timer(
        &mut self,
        duration: Duration,
        waker: &Waker,
    ) -> Result<usize, DurationTooLong> {
        todo!()
    }

    pub fn drop(&mut self, id: usize) {
        todo!()
    }

    pub fn poll(&mut self, id: usize, waker: &Waker) -> Poll<()> {
        todo!()
    }

    pub fn next_deadline(&self) -> Option<Duration> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use std::task::Wake;

    type Log = Arc<Mutex<Vec<usize>>>;

    struct Recorder {
        id: usize,
        log: Log,
    }

    impl Wake for Recorder {
        fn wake(self: Arc<Self>) {
            self.log.lock().unwrap().push(self.id);
        }
    }

    fn recording_waker(log: &Log, id: usize) -> Waker {
        Waker::from(Arc::new(Recorder {
            id,
            log: Arc::clone(log),
        }))
    }

    fn waker() -> Waker {
        struct Noop;
        impl Wake for Noop {
            fn wake(self: Arc<Self>) {}
        }
        Waker::from(Arc::new(Noop))
    }

    fn groups(d: &Deadlines) -> Vec<(u64, u32)> {
        d.inner.iter().map(|g| (g.at, g.count)).collect()
    }

    #[test]
    fn deadlines_start_empty() {
        assert!(groups(&Deadlines::new()).is_empty());
    }

    #[test]
    fn first_push_opens_a_group_of_one() {
        let mut d = Deadlines::new();
        d.push(500);
        assert_eq!(groups(&d), [(500, 1)]);
    }

    #[test]
    fn the_same_deadline_increments_instead_of_opening_a_group() {
        let mut d = Deadlines::new();
        d.push(500);
        d.push(500);
        d.push(500);
        assert_eq!(groups(&d), [(500, 3)]);
    }

    #[test]
    fn a_later_deadline_opens_a_new_group() {
        let mut d = Deadlines::new();
        d.push(500);
        d.push(501);
        assert_eq!(groups(&d), [(500, 1), (501, 1)]);
    }

    #[test]
    fn runs_of_equal_deadlines_collapse_one_group_each() {
        let mut d = Deadlines::new();
        for at in [500, 500, 503, 507, 507, 507] {
            d.push(at);
        }
        assert_eq!(groups(&d), [(500, 2), (503, 1), (507, 3)]);
    }

    #[test]
    fn only_the_last_group_is_extended() {
        let mut d = Deadlines::new();
        d.push(500);
        d.push(501);
        d.push(500);
        assert_eq!(groups(&d), [(500, 1), (501, 1), (500, 1)]);
    }

    #[test]
    fn wakers_start_empty() {
        assert!(Wakers::new().pop().is_none());
    }

    #[test]
    fn wakers_pop_in_the_order_they_were_pushed() {
        let (a, b) = (waker(), waker());
        let mut w = Wakers::new();
        w.push(a.clone());
        w.push(b.clone());

        assert!(w.pop().unwrap().will_wake(&a));
        assert!(w.pop().unwrap().will_wake(&b));
        assert!(w.pop().is_none());
    }

    #[test]
    fn pop_n_takes_a_prefix_and_leaves_the_rest() {
        let (a, b, c) = (waker(), waker(), waker());
        let mut w = Wakers::new();
        w.push(a.clone());
        w.push(b.clone());
        w.push(c.clone());

        let taken: Vec<Waker> = w.pop_n(2).collect();
        assert_eq!(taken.len(), 2);
        assert!(taken[0].will_wake(&a));
        assert!(taken[1].will_wake(&b));

        assert!(w.pop().unwrap().will_wake(&c));
        assert!(w.pop().is_none());
    }

    #[test]
    fn pop_n_past_the_end_takes_everything() {
        let mut w = Wakers::new();
        w.push(waker());
        w.push(waker());

        assert_eq!(w.pop_n(10).count(), 2);
        assert!(w.pop().is_none());
    }

    #[test]
    fn pop_n_of_zero_takes_nothing() {
        let mut w = Wakers::new();
        w.push(waker());

        assert_eq!(w.pop_n(0).count(), 0);
        assert!(w.pop().is_some());
    }


}
