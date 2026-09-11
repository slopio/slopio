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

struct CancelField {
    words: VecDeque<u64>,
    head_bit: usize,
    len: usize,
}

impl CancelField {
    fn new() -> Self {
        Self {
            words: VecDeque::new(),
            head_bit: 0,
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn push(&mut self) {
        let p = self.head_bit + self.len;
        if p == self.words.len() * 64 {
            self.words.push_back(0);
        }
        self.len += 1;
    }

    fn cancel(&mut self, i: usize) {
        assert!(i < self.len, "index {i} past the end of {}", self.len);
        let p = self.head_bit + i;
        self.words[p >> 6] |= 1 << (p & 63);
    }

    fn is_cancelled(&self, i: usize) -> bool {
        assert!(i < self.len, "index {i} past the end of {}", self.len);
        let p = self.head_bit + i;
        self.words[p >> 6] & (1 << (p & 63)) != 0
    }

    fn pop_n(&mut self, n: usize) {
        let n = n.min(self.len);
        self.head_bit += n;
        self.len -= n;
        while self.head_bit >= 64 {
            self.words.pop_front();
            self.head_bit -= 64;
        }
    }

    fn first_live(&self) -> Option<usize> {
        let start = self.head_bit;
        let end = self.head_bit + self.len;
        let mut p = start;
        while p < end {
            let widx = p >> 6;
            let bit = p & 63;
            let mut word = (!self.words[widx] >> bit) << bit;
            let word_end = (widx + 1) * 64;
            if end < word_end {
                let top = end - widx * 64;
                word &= (1u64 << top) - 1;
            }
            if word != 0 {
                return Some(widx * 64 + word.trailing_zeros() as usize - start);
            }
            p = word_end;
        }
        None
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



    fn model_of(f: &CancelField) -> Vec<bool> {
        (0..f.len()).map(|i| f.is_cancelled(i)).collect()
    }

    fn first_live_of(model: &[bool]) -> Option<usize> {
        model.iter().position(|cancelled| !cancelled)
    }

    #[test]
    fn cancel_field_starts_empty() {
        let f = CancelField::new();
        assert_eq!(f.len(), 0);
        assert!(f.is_empty());
        assert_eq!(f.first_live(), None);
    }

    #[test]
    fn pushed_slots_are_live() {
        let mut f = CancelField::new();
        f.push();
        f.push();
        assert_eq!(model_of(&f), [false, false]);
        assert_eq!(f.first_live(), Some(0));
    }

    #[test]
    fn cancel_marks_only_its_own_slot() {
        let mut f = CancelField::new();
        for _ in 0..5 {
            f.push();
        }
        f.cancel(2);
        assert_eq!(model_of(&f), [false, false, true, false, false]);
        assert_eq!(f.first_live(), Some(0));
    }

    #[test]
    fn first_live_skips_a_cancelled_prefix() {
        let mut f = CancelField::new();
        for _ in 0..5 {
            f.push();
        }
        f.cancel(0);
        f.cancel(1);
        f.cancel(2);
        assert_eq!(f.first_live(), Some(3));
    }

    #[test]
    fn first_live_is_none_when_everything_is_cancelled() {
        let mut f = CancelField::new();
        for _ in 0..70 {
            f.push();
        }
        for i in 0..70 {
            f.cancel(i);
        }
        assert_eq!(f.first_live(), None);
    }

    #[test]
    fn pop_n_shifts_the_indices_down() {
        let mut f = CancelField::new();
        for _ in 0..5 {
            f.push();
        }
        f.cancel(3);
        f.pop_n(2);
        assert_eq!(model_of(&f), [false, true, false]);
        assert_eq!(f.first_live(), Some(0));
    }

    #[test]
    fn pop_n_past_the_end_empties_it() {
        let mut f = CancelField::new();
        f.push();
        f.push();
        f.pop_n(10);
        assert!(f.is_empty());
        assert_eq!(f.first_live(), None);
    }

    #[test]
    fn a_word_is_released_once_fully_consumed() {
        let mut f = CancelField::new();
        for _ in 0..200 {
            f.push();
        }
        assert_eq!(f.words.len(), 4);
        f.pop_n(128);
        assert_eq!(f.words.len(), 2);
        assert_eq!(f.len(), 72);
    }

    #[test]
    fn a_slot_reached_after_a_word_boundary_is_live() {
        let mut f = CancelField::new();
        for _ in 0..64 {
            f.push();
        }
        for i in 0..64 {
            f.cancel(i);
        }
        f.pop_n(64);
        for _ in 0..64 {
            f.push();
        }
        assert_eq!(model_of(&f), [false; 64], "a fresh slot must never inherit a bit");
        assert_eq!(f.first_live(), Some(0));
    }

    #[test]
    fn crossing_word_boundaries_keeps_indices_aligned() {
        let mut f = CancelField::new();
        let mut model: Vec<bool> = Vec::new();

        for round in 0..40 {
            for _ in 0..37 {
                f.push();
                model.push(false);
            }
            let target = round % 37;
            f.cancel(target);
            model[target] = true;

            assert_eq!(model_of(&f), model, "round {round} after cancel");
            assert_eq!(f.first_live(), first_live_of(&model), "round {round}");

            f.pop_n(19);
            model.drain(..19);
            assert_eq!(model_of(&f), model, "round {round} after pop");
            assert_eq!(f.first_live(), first_live_of(&model), "round {round}");
        }
    }

    #[test]
    fn stays_consistent_with_a_reference_model() {
        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        let mut f = CancelField::new();
        let mut model: Vec<bool> = Vec::new();
        let mut max_len = 0;

        for step in 0..20_000 {
            match next() % 10 {
                0..=5 => {
                    let n = 1 + next() % 40;
                    for _ in 0..n {
                        f.push();
                        model.push(false);
                    }
                }
                6..=7 if !model.is_empty() => {
                    let i = (next() as usize) % model.len();
                    f.cancel(i);
                    model[i] = true;
                }
                _ => {
                    let n = (next() as usize) % (model.len() + 1);
                    f.pop_n(n);
                    model.drain(..n);
                }
            }

            assert_eq!(f.len(), model.len(), "step {step}");
            assert_eq!(f.is_empty(), model.is_empty(), "step {step}");
            assert_eq!(model_of(&f), model, "step {step}");
            assert_eq!(f.first_live(), first_live_of(&model), "step {step}");
            assert!(
                f.words.len() <= (f.head_bit + f.len()) / 64 + 1,
                "step {step}: {} words held for {} bits",
                f.words.len(),
                f.len(),
            );
            max_len = max_len.max(model.len());
        }

        assert!(
            max_len > 500,
            "the run stayed too small to cross word boundaries: {max_len}",
        );
    }

    #[test]
    #[ignore]
    fn cancel_field_cost() {
        use std::hint::black_box;
        use std::time::Instant;

        const N: usize = 2_000_000;

        let mut f = CancelField::new();
        let t = Instant::now();
        for _ in 0..N {
            f.push();
        }
        let push = t.elapsed().as_nanos() as f64 / N as f64;

        let mut rng = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let idx: Vec<usize> = (0..N).map(|_| next() as usize % N).collect();

        let t = Instant::now();
        for &i in &idx {
            f.cancel(i);
        }
        let cancel = t.elapsed().as_nanos() as f64 / N as f64;

        let t = Instant::now();
        let mut acc = 0usize;
        for &i in &idx {
            acc += black_box(f.is_cancelled(i)) as usize;
        }
        let test = t.elapsed().as_nanos() as f64 / N as f64;
        black_box(acc);

        println!(
            "push {push:.2} ns   cancel {cancel:.2} ns   is_cancelled {test:.2} ns  ({} of {N} set)",
            acc,
        );

        for prefix in [64usize, 1_000, 10_000, 100_000] {
            let mut f = CancelField::new();
            for _ in 0..prefix + 1 {
                f.push();
            }
            for i in 0..prefix {
                f.cancel(i);
            }
            const REPS: usize = 20_000;
            let t = Instant::now();
            for _ in 0..REPS {
                black_box(f.first_live());
            }
            let per = t.elapsed().as_nanos() as f64 / REPS as f64;
            println!(
                "first_live over {prefix:>7} cancelled: {per:>9.1} ns  ({:.3} ns/entry)",
                per / prefix as f64,
            );
        }

        let mut f = CancelField::new();
        for _ in 0..N {
            f.push();
        }
        let t = Instant::now();
        let mut popped = 0;
        while !f.is_empty() {
            f.pop_n(37);
            popped += 1;
        }
        println!(
            "pop_n(37) {:.2} ns/call over {popped} calls",
            t.elapsed().as_nanos() as f64 / popped as f64,
        );
    }
}
