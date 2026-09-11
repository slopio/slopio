use std::collections::VecDeque;
use std::collections::vec_deque::Drain;
use std::task::Waker;
use std::time::Duration;

mod active_field;

use active_field::ActiveField;

// The tick the driver counts in. Deadlines are whole ticks of it, and a store's
// duration must be one too.
const MS_TICK: u64 = 10;

/// Names one armed timer in the store that handed it out. The duration is part
/// of the type, so a token cannot be replayed against a store of another one.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Token<const MILLIS: u64>(u64);

impl<const MILLIS: u64> Token<MILLIS> {
    fn raw(self) -> u64 {
        self.0
    }
}

/// One counter per tick, indexed by `at % SLOTS`.
///
/// All the timers in a store share a duration, so the live deadlines never span
/// more than the duration itself and a ring of that many slots holds them all
/// without ever wrapping onto a live one.
struct Deadlines<const MILLIS: u64> {
    counts: Box<[u32]>,
    // Every slot before this one is zero; deadlines live in `[swept, swept + SLOTS)`.
    swept: u64,
    len: u32,
}

impl<const MILLIS: u64> Deadlines<MILLIS> {
    // Deadlines span `[oldest, oldest + duration]` inclusive, one more than the
    // duration counts in ticks.
    const SLOTS: u64 = MILLIS / MS_TICK + 1;

    fn new() -> Self {
        Self {
            counts: vec![0; Self::SLOTS as usize].into_boxed_slice(),
            swept: 0,
            len: 0,
        }
    }

    #[inline]
    fn slot(at: u64) -> usize {
        (at % Self::SLOTS) as usize
    }

    fn push(&mut self, at: u64) {
        // An empty ring has no window yet; the first deadline opens one.
        if self.len == 0 {
            self.swept = at;
        }
        debug_assert!(at >= self.swept, "deadlines must not go backwards");
        debug_assert!(
            at - self.swept < Self::SLOTS,
            "deadline {at} past the window opened at {}: `tick` is not being \
             driven with the clock that arms",
            self.swept,
        );
        // Clamping keeps a window overrun to an early fire rather than letting
        // the index wrap onto a live slot.
        let at = at.clamp(self.swept, self.swept + Self::SLOTS - 1);
        self.counts[Self::slot(at)] += 1;
        self.len += 1;
    }

    fn pop_passed(&mut self, now: u64) -> usize {
        // A clock that jumped leaves nothing live; never walk more slots than
        // the ring holds.
        self.swept = self.swept.max(now.saturating_sub(Self::SLOTS));
        let mut n = 0;
        while self.swept < now {
            n += std::mem::take(&mut self.counts[Self::slot(self.swept)]) as usize;
            self.swept += 1;
        }
        self.len -= n as u32;
        n
    }

    fn pop_n(&mut self, n: u32) {
        let mut n = n.min(self.len);
        self.len -= n;
        while n > 0 {
            let slot = Self::slot(self.swept);
            let count = self.counts[slot];
            if count > n {
                self.counts[slot] = count - n;
                return;
            }
            n -= count;
            self.counts[slot] = 0;
            self.swept += 1;
        }
    }

    // Deadline of the entry `i` places behind the oldest. Slots hold counts, not
    // entries, so the answer is where the running total first passes `i` — a
    // step or two in from `swept` whenever a tick armed more than a handful.
    fn at_index(&self, i: u32) -> Option<u64> {
        if i >= self.len {
            return None;
        }
        let mut seen = 0;
        for at in self.swept..self.swept + Self::SLOTS {
            seen += self.counts[Self::slot(at)];
            if seen > i {
                return Some(at);
            }
        }
        None
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

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn get(&self, i: usize) -> &Waker {
        &self.inner[i]
    }

    fn pop(&mut self) -> Option<Waker> {
        self.inner.pop_front()
    }

    fn pop_n(&mut self, n: usize) -> Drain<'_, Waker> {
        self.inner.drain(..n.min(self.inner.len()))
    }
}

/// A timer store where every timer runs for `MILLIS`.
///
/// One duration per store is what makes the three queues FIFOs: arming later
/// can only end later, so deadlines are non-decreasing by construction and
/// nothing is ever sorted or searched.
pub struct Timers<const MILLIS: u64> {
    deadlines: Deadlines<MILLIS>,
    wakers: Wakers,
    active: ActiveField,
    popped: u64,
}

#[allow(unused_variables)]
impl<const MILLIS: u64> Timers<MILLIS> {
    const DURATION_TICKS: u64 = MILLIS / MS_TICK;

    // A duration the tick cannot express would silently round down, and the
    // ring is sized from it, so both are settled here rather than at arm time.
    const VALID: () = assert!(
        MILLIS >= MS_TICK && MILLIS % MS_TICK == 0,
        "a Timers duration must be a whole number of 10 ms ticks",
    );

    #[must_use]
    pub fn new() -> Self {
        let () = Self::VALID;
        Self {
            deadlines: Deadlines::new(),
            wakers: Wakers::new(),
            active: ActiveField::new(),
            popped: 0,
        }
    }

    fn pushed(&self) -> Token<MILLIS> {
        Token(self.popped + self.wakers.len() as u64)
    }

    fn push(&mut self, waker: Waker, now: u64) -> Token<MILLIS> {
        let token = self.pushed();
        self.deadlines.push(now + Self::DURATION_TICKS);
        self.wakers.push(waker);
        self.active.push();
        debug_assert_eq!(self.wakers.len(), self.active.len());
        token
    }

    fn drop_front(&mut self, n: usize) {
        self.wakers.pop_n(n);
        self.active.pop_n(n);
        self.popped += n as u64;
        debug_assert_eq!(self.wakers.len(), self.active.len());
    }

    fn pop_n(&mut self, n: usize) {
        let n = n.min(self.wakers.len());
        self.deadlines.pop_n(n as u32);
        self.drop_front(n);
    }

    fn index_of(&self, token: Token<MILLIS>) -> Option<usize> {
        let i = token.raw().checked_sub(self.popped)? as usize;
        (i < self.wakers.len()).then_some(i)
    }

    fn cancel(&mut self, token: Token<MILLIS>) {
        let Some(i) = self.index_of(token) else {
            return;
        };
        self.active.cancel(i);
        if i == 0 {
            let n = self.active.first().unwrap_or(self.active.len());
            self.pop_n(n);
        }
    }

    pub fn tick(&mut self, now: u64) {
        let n = self.deadlines.pop_passed(now);
        if n == 0 {
            return;
        }
        let wakers = &self.wakers;
        for i in self.active.iter(n) {
            wakers.get(i).wake_by_ref();
        }
        self.drop_front(n);
    }

    /// Time from `now` until [`tick`](Self::tick) has something to do, or None
    /// if nothing armed is still waiting.
    ///
    /// Exact rather than a bound: a cancelled entry keeps its place in the
    /// queues until the clock reaches it, so the answer is read off the first
    /// entry still active, not off the oldest deadline held.
    pub fn next_deadline(&self, now: u64) -> Option<Duration> {
        let i = self.active.first()?;
        let at = self.deadlines.at_index(i as u32)?;
        // Firing wants the clock strictly past `at`, so never under one tick —
        // a zero wait would spin the loop.
        let ticks = at.saturating_sub(now) + 1;
        Some(Duration::from_millis(ticks * MS_TICK))
    }
}

impl<const MILLIS: u64> Default for Timers<MILLIS> {
    fn default() -> Self {
        Self::new()
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

    // The five seconds every `Timers` in these tests runs for.
    const D: u64 = 5_000;
    const TICKS: u64 = D / MS_TICK;

    // The occupied slots, oldest first — the shape the RLE deque used to hold
    // directly, read back off the ring.
    fn groups<const M: u64>(d: &Deadlines<M>) -> Vec<(u64, u32)> {
        (d.swept..d.swept + Deadlines::<M>::SLOTS)
            .filter_map(|at| {
                let count = d.counts[Deadlines::<M>::slot(at)];
                (count != 0).then_some((at, count))
            })
            .collect()
    }

    fn deadlines() -> Deadlines<D> {
        Deadlines::new()
    }

    #[test]
    fn deadlines_start_empty() {
        assert!(groups(&deadlines()).is_empty());
    }

    #[test]
    fn first_push_opens_a_group_of_one() {
        let mut d = deadlines();
        d.push(500);
        assert_eq!(groups(&d), [(500, 1)]);
    }

    #[test]
    fn the_same_deadline_increments_instead_of_opening_a_group() {
        let mut d = deadlines();
        d.push(500);
        d.push(500);
        d.push(500);
        assert_eq!(groups(&d), [(500, 3)]);
    }

    #[test]
    fn a_later_deadline_opens_a_new_group() {
        let mut d = deadlines();
        d.push(500);
        d.push(501);
        assert_eq!(groups(&d), [(500, 1), (501, 1)]);
    }

    #[test]
    fn runs_of_equal_deadlines_collapse_one_group_each() {
        let mut d = deadlines();
        for at in [500, 500, 503, 507, 507, 507] {
            d.push(at);
        }
        assert_eq!(groups(&d), [(500, 2), (503, 1), (507, 3)]);
    }

    #[test]
    fn a_deadline_is_counted_by_its_tick_not_by_arrival() {
        let mut d = deadlines();
        d.push(500);
        d.push(501);
        d.push(500);
        assert_eq!(
            groups(&d),
            [(500, 2), (501, 1)],
            "a slot is keyed by its deadline, so a repeat joins the earlier one",
        );
    }

    #[test]
    fn the_window_spans_a_whole_duration() {
        let mut d = deadlines();
        d.push(500);
        d.push(500 + TICKS);
        assert_eq!(
            groups(&d),
            [(500, 1), (500 + TICKS, 1)],
            "the oldest and the newest live deadline must not share a slot",
        );
    }

    #[test]
    fn pop_passed_removes_the_groups_behind_the_clock() {
        let mut d = deadlines();
        for at in [500, 500, 503, 507, 507, 507] {
            d.push(at);
        }

        assert_eq!(d.pop_passed(500), 0, "500 is not behind 500");
        assert_eq!(groups(&d), [(500, 2), (503, 1), (507, 3)]);

        assert_eq!(d.pop_passed(504), 3);
        assert_eq!(groups(&d), [(507, 3)]);

        assert_eq!(d.pop_passed(9_999), 3);
        assert!(groups(&d).is_empty());
        assert_eq!(d.pop_passed(9_999), 0);
    }

    #[test]
    fn a_clock_jump_past_the_window_drains_the_ring() {
        let mut d = deadlines();
        for at in [500, 600, 999] {
            d.push(at);
        }
        assert_eq!(d.pop_passed(50_000), 3, "everything armed is long overdue");
        assert!(groups(&d).is_empty());
        assert_eq!(d.len, 0);
    }

    #[test]
    fn pop_n_takes_from_the_oldest_slots() {
        let mut d = deadlines();
        for at in [500, 500, 503, 507, 507, 507] {
            d.push(at);
        }

        d.pop_n(3);
        assert_eq!(groups(&d), [(507, 3)]);

        d.pop_n(1);
        assert_eq!(groups(&d), [(507, 2)]);

        d.pop_n(99);
        assert!(groups(&d).is_empty());
        assert_eq!(d.len, 0);
    }

    #[test]
    fn the_ring_is_reusable_once_fully_swept() {
        let mut d = deadlines();
        d.push(500);
        assert_eq!(d.pop_passed(501), 1);

        // Far past the first window: an empty ring opens a fresh one.
        d.push(90_000);
        assert_eq!(groups(&d), [(90_000, 1)]);
        assert_eq!(d.pop_passed(90_001), 1);
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

    fn total_of<const M: u64>(d: &Deadlines<M>) -> u32 {
        d.len
    }

    #[test]
    fn pushing_a_timer_advances_all_three_queues() {
        let mut t = Timers::<D>::new();
        t.push(waker(), 0);

        assert_eq!(total_of(&t.deadlines), 1);
        assert_eq!(t.wakers.len(), 1);
        assert_eq!(t.active.len(), 1);
        assert!(t.active.is_active(0));
    }

    #[test]
    fn the_three_queues_stay_the_same_size() {
        let mut t = Timers::<D>::new();
        for n in 1..=200 {
            t.push(waker(), 0);
            assert_eq!(total_of(&t.deadlines) as usize, n);
            assert_eq!(t.wakers.len(), n);
            assert_eq!(t.active.len(), n);
        }
    }

    #[test]
    fn a_timer_lands_one_duration_ahead_of_the_clock() {
        let mut t = Timers::<D>::new();
        t.push(waker(), 0);
        t.push(waker(), 300);

        let groups = groups(&t.deadlines);
        assert_eq!(groups, [(500, 1), (800, 1)]);
    }

    #[test]
    fn timers_armed_on_the_same_tick_share_a_deadline_entry() {
        let mut t = Timers::<D>::new();
        t.push(waker(), 0);
        t.push(waker(), 0);
        t.push(waker(), 1);

        assert_eq!(groups(&t.deadlines), [(500, 2), (501, 1)]);
        assert_eq!(t.wakers.len(), 3);
        assert_eq!(t.active.len(), 3);
    }

    #[test]
    fn pushed_timers_come_back_in_order() {
        let (a, b, c) = (waker(), waker(), waker());
        let mut t = Timers::<D>::new();
        t.push(a.clone(), 0);
        t.push(b.clone(), 0);
        t.push(c.clone(), 0);

        assert!(t.wakers.pop().unwrap().will_wake(&a));
        assert!(t.wakers.pop().unwrap().will_wake(&b));
        assert!(t.wakers.pop().unwrap().will_wake(&c));
    }

    #[test]
    fn a_token_resolves_to_its_position() {
        let mut t = Timers::<D>::new();
        let a = t.push(waker(), 0);
        let b = t.push(waker(), 0);
        let c = t.push(waker(), 0);

        assert_eq!((a, b, c), (Token(0), Token(1), Token(2)));
        assert_eq!(t.index_of(a), Some(0));
        assert_eq!(t.index_of(b), Some(1));
        assert_eq!(t.index_of(c), Some(2));
    }

    #[test]
    fn tokens_shift_when_the_front_is_dropped() {
        let mut t = Timers::<D>::new();
        let tokens: Vec<Token<D>> = (0..5).map(|_| t.push(waker(), 0)).collect();

        t.pop_n(2);

        assert_eq!(t.index_of(tokens[0]), None);
        assert_eq!(t.index_of(tokens[1]), None);
        assert_eq!(t.index_of(tokens[2]), Some(0));
        assert_eq!(t.index_of(tokens[3]), Some(1));
        assert_eq!(t.index_of(tokens[4]), Some(2));
    }

    #[test]
    fn a_token_handed_out_later_is_still_found_after_pops() {
        let mut t = Timers::<D>::new();
        for _ in 0..10 {
            t.push(waker(), 0);
        }
        t.pop_n(10);
        let token = t.push(waker(), 0);

        assert_eq!(token, Token(10));
        assert_eq!(t.index_of(token), Some(0));
    }

    #[test]
    fn a_token_beyond_what_was_handed_out_resolves_to_nothing() {
        let mut t = Timers::<D>::new();
        t.push(waker(), 0);
        assert_eq!(t.index_of(Token(7)), None);
    }

    #[test]
    fn popping_keeps_the_three_queues_the_same_size() {
        let mut t = Timers::<D>::new();
        for i in 0..50 {
            t.push(waker(), i / 7);
        }
        t.pop_n(23);

        assert_eq!(total_of(&t.deadlines), 27);
        assert_eq!(t.wakers.len(), 27);
        assert_eq!(t.active.len(), 27);
    }

    #[test]
    fn popping_past_the_end_empties_everything() {
        let mut t = Timers::<D>::new();
        for _ in 0..5 {
            t.push(waker(), 0);
        }
        t.pop_n(100);

        assert_eq!(total_of(&t.deadlines), 0);
        assert_eq!(t.wakers.len(), 0);
        assert_eq!(t.active.len(), 0);
        assert_eq!(t.popped, 5);
    }

    #[test]
    fn tokens_track_positions_through_random_traffic() {
        let mut rng = 0x1234_5678_9abc_def1u64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        let mut t = Timers::<D>::new();
        let mut live: VecDeque<Token<D>> = VecDeque::new();
        let mut max_len = 0;
        let mut now = 0u64;

        for step in 0..10_000 {
            if next() % 3 == 0 && !live.is_empty() {
                let n = (next() as usize) % (live.len() + 1);
                t.pop_n(n);
                live.drain(..n);
            } else {
                now += next() % 3;
                let n = 1 + next() % 20;
                for _ in 0..n {
                    live.push_back(t.push(waker(), now));
                }
            }

            assert_eq!(t.wakers.len(), live.len(), "step {step}");
            assert_eq!(t.active.len(), live.len(), "step {step}");
            assert_eq!(total_of(&t.deadlines) as usize, live.len(), "step {step}");
            for (i, &token) in live.iter().enumerate() {
                assert_eq!(t.index_of(token), Some(i), "step {step}, token {token:?}");
            }
            if let Some(front) = live.front().and_then(|f| f.raw().checked_sub(1)) {
                assert_eq!(t.index_of(Token(front)), None, "step {step}");
            }
            assert_eq!(t.index_of(t.pushed()), None, "step {step}");
            max_len = max_len.max(live.len());
        }

        assert!(max_len > 100, "the run stayed too small: {max_len}");
    }

    /// The driver's own loop — tick, arm, cancel, every tick — run long enough
    /// for the deadline ring to wrap several times. The ring only holds one
    /// duration's worth of slots, so this is where a slot reused too early
    /// would show up as a timer firing at the wrong tick.
    #[test]
    fn the_ring_wraps_without_mixing_up_deadlines() {
        let mut rng = 0xfeed_face_dead_beefu64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };

        let log: Log = Arc::default();
        let mut t = Timers::<D>::new();
        // Every armed timer, by the tick it must fire on: `arm + TICKS + 1`.
        let mut expected: Vec<Vec<usize>> = vec![Vec::new(); 4_000 + TICKS as usize + 2];
        let mut pending: VecDeque<(u64, Token<D>, usize)> = VecDeque::new();
        let mut id = 0usize;
        let mut fired = 0usize;

        for now in 0..4_000u64 {
            t.tick(now);

            let due = std::mem::take(&mut expected[now as usize]);
            assert_eq!(*log.lock().unwrap(), due, "tick {now}");
            fired += due.len();
            log.lock().unwrap().clear();

            for _ in 0..next() % 8 {
                let token = t.push(recording_waker(&log, id), now);
                expected[(now + TICKS + 1) as usize].push(id);
                pending.push_back((now + TICKS + 1, token, id));
                id += 1;
            }

            // Cancel an arbitrary live timer; it must then never be logged.
            if next() % 3 == 0 && !pending.is_empty() {
                let i = (next() as usize) % pending.len();
                let (at, token, cancelled) = pending[i];
                t.cancel(token);
                expected[at as usize].retain(|logged| *logged != cancelled);
                pending.remove(i);
            }
            while pending.front().is_some_and(|(at, ..)| *at <= now) {
                pending.pop_front();
            }
        }

        assert!(
            fired > 4_000,
            "the run never wrapped the ring enough to matter: {fired} fired",
        );
    }

    #[test]
    fn nothing_fires_before_the_deadline() {
        let log: Log = Arc::default();
        let mut t = Timers::<D>::new();
        t.push(recording_waker(&log, 0), 0);

        t.tick(499);
        t.tick(500);
        assert!(
            log.lock().unwrap().is_empty(),
            "500 is the deadline, not past it"
        );
        assert_eq!(t.wakers.len(), 1);
    }

    #[test]
    fn a_timer_fires_once_the_clock_is_past_its_deadline() {
        let log: Log = Arc::default();
        let mut t = Timers::<D>::new();
        t.push(recording_waker(&log, 0), 0);

        t.tick(501);
        assert_eq!(*log.lock().unwrap(), [0]);
        assert_eq!(t.wakers.len(), 0);
        assert_eq!(t.active.len(), 0);
        assert_eq!(total_of(&t.deadlines), 0);
        assert_eq!(t.popped, 1);
    }

    #[test]
    fn cancelled_timers_are_dropped_without_firing() {
        let log: Log = Arc::default();
        let mut t = Timers::<D>::new();
        t.push(recording_waker(&log, 0), 0);
        t.push(recording_waker(&log, 1), 0);
        t.push(recording_waker(&log, 2), 0);
        t.active.cancel(1);

        t.tick(501);
        assert_eq!(*log.lock().unwrap(), [0, 2]);
        assert_eq!(t.wakers.len(), 0);
    }

    #[test]
    fn only_the_groups_behind_the_clock_fire() {
        let log: Log = Arc::default();
        let mut t = Timers::<D>::new();
        t.push(recording_waker(&log, 0), 0);
        t.push(recording_waker(&log, 1), 0);
        t.push(recording_waker(&log, 2), 10);
        t.push(recording_waker(&log, 3), 20);

        t.tick(501);
        assert_eq!(*log.lock().unwrap(), [0, 1]);
        assert_eq!(t.wakers.len(), 2);

        t.tick(511);
        assert_eq!(*log.lock().unwrap(), [0, 1, 2]);
        assert_eq!(t.wakers.len(), 1);

        t.tick(9_999);
        assert_eq!(*log.lock().unwrap(), [0, 1, 2, 3]);
        assert_eq!(t.wakers.len(), 0);
    }

    #[test]
    fn a_clock_jump_fires_every_group_it_passed() {
        let log: Log = Arc::default();
        let mut t = Timers::<D>::new();
        for i in 0..5 {
            t.push(recording_waker(&log, i), i as u64 * 10);
        }

        t.tick(600);
        assert_eq!(*log.lock().unwrap(), [0, 1, 2, 3, 4]);
        assert_eq!(t.wakers.len(), 0);
        assert_eq!(t.active.len(), 0);
        assert_eq!(total_of(&t.deadlines), 0);
    }

    #[test]
    fn a_tick_with_nothing_due_changes_nothing() {
        let mut t = Timers::<D>::new();
        t.push(waker(), 0);
        t.tick(0);
        t.tick(100);

        assert_eq!(t.wakers.len(), 1);
        assert_eq!(t.popped, 0);
    }

    #[test]
    fn firing_shifts_the_tokens_of_the_survivors() {
        let mut t = Timers::<D>::new();
        let early = t.push(waker(), 0);
        let late = t.push(waker(), 50);

        t.tick(501);
        assert_eq!(t.index_of(early), None);
        assert_eq!(t.index_of(late), Some(0));
    }

    #[test]
    fn firing_a_fully_cancelled_batch_wakes_nobody() {
        let log: Log = Arc::default();
        let mut t = Timers::<D>::new();
        for i in 0..70 {
            t.push(recording_waker(&log, i), 0);
        }
        for i in 0..70 {
            t.active.cancel(i);
        }

        t.tick(501);
        assert!(log.lock().unwrap().is_empty());
        assert_eq!(t.wakers.len(), 0);
        assert_eq!(t.popped, 70);
    }

    #[test]
    fn cancelling_the_head_reclaims_the_dead_prefix() {
        let mut t = Timers::<D>::new();
        let tokens: Vec<Token<D>> = (0..5).map(|_| t.push(waker(), 0)).collect();

        t.cancel(tokens[1]);
        t.cancel(tokens[2]);
        assert_eq!(t.wakers.len(), 5, "holes wait for the head");

        t.cancel(tokens[0]);
        assert_eq!(t.wakers.len(), 2, "the head jumps over the whole dead run");
        assert_eq!(t.index_of(tokens[3]), Some(0));
        assert_eq!(total_of(&t.deadlines), 2);
    }

    #[test]
    fn cancelling_everything_empties_the_queues() {
        let mut t = Timers::<D>::new();
        let tokens: Vec<Token<D>> = (0..100).map(|_| t.push(waker(), 0)).collect();
        for token in tokens.iter().rev() {
            t.cancel(*token);
        }
        assert_eq!(t.wakers.len(), 0);
        assert_eq!(t.active.len(), 0);
        assert_eq!(total_of(&t.deadlines), 0);
    }

    #[test]
    fn a_cancelled_timer_does_not_fire() {
        let log: Log = Arc::default();
        let mut t = Timers::<D>::new();
        let a = t.push(recording_waker(&log, 0), 0);
        t.push(recording_waker(&log, 1), 0);
        t.cancel(a);

        t.tick(501);
        assert_eq!(*log.lock().unwrap(), [1]);
    }

    #[test]
    fn cancelling_a_stale_token_is_a_no_op() {
        let mut t = Timers::<D>::new();
        let a = t.push(waker(), 0);
        t.cancel(a);
        t.cancel(a);
        assert_eq!(t.wakers.len(), 0);
    }

    fn ticks_of(d: Duration) -> u64 {
        d.as_millis() as u64 / MS_TICK
    }

    #[test]
    fn nothing_armed_has_no_deadline() {
        let t = Timers::<D>::new();
        assert_eq!(t.next_deadline(0), None);
    }

    #[test]
    fn the_deadline_counts_down_as_the_clock_moves() {
        let mut t = Timers::<D>::new();
        t.push(waker(), 0);

        assert_eq!(ticks_of(t.next_deadline(0).unwrap()), TICKS + 1);
        assert_eq!(ticks_of(t.next_deadline(100).unwrap()), TICKS - 100 + 1);
    }

    /// The contract the event loop leans on: the tick `next_deadline` names is
    /// the first one that fires, and the one before it fires nothing.
    #[test]
    fn next_deadline_names_the_tick_that_fires() {
        let log: Log = Arc::default();
        let mut t = Timers::<D>::new();
        t.push(recording_waker(&log, 0), 40);

        let target = 40 + ticks_of(t.next_deadline(40).unwrap());

        t.tick(target - 1);
        assert!(log.lock().unwrap().is_empty(), "a tick too early");
        t.tick(target);
        assert_eq!(*log.lock().unwrap(), [0]);
    }

    #[test]
    fn a_deadline_the_clock_has_reached_is_still_a_tick_away() {
        let mut t = Timers::<D>::new();
        t.push(waker(), 0);

        let wait = t.next_deadline(TICKS).unwrap();
        assert_eq!(wait, Duration::from_millis(MS_TICK));
        assert!(wait > Duration::ZERO, "a zero wait would spin the loop");
        assert_eq!(
            t.next_deadline(9_999).unwrap(),
            Duration::from_millis(MS_TICK)
        );
    }

    /// `tick` drops the entries it fired without looking at what it uncovers,
    /// so the head is routinely a cancelled entry. Answering from it would wake
    /// the loop early, for a timer that will never fire.
    #[test]
    fn a_cancelled_head_is_skipped() {
        let mut t = Timers::<D>::new();
        let early = t.push(waker(), 0);
        let later = t.push(waker(), 10);
        t.push(waker(), 40);

        assert_eq!(ticks_of(t.next_deadline(0).unwrap()), TICKS + 1);

        // Cancelling at the head reclaims it, so the answer moves on by itself.
        t.cancel(early);
        assert_eq!(ticks_of(t.next_deadline(0).unwrap()), TICKS + 10 + 1);

        // Cancelling away from the head leaves the entry in place; the answer
        // must step over it anyway.
        t.cancel(later);
        assert_eq!(
            ticks_of(t.next_deadline(0).unwrap()),
            TICKS + 40 + 1,
            "the dead entry is still in the queues",
        );
    }

    #[test]
    fn a_fully_cancelled_queue_has_no_deadline() {
        let mut t = Timers::<D>::new();
        for i in 0..70 {
            t.push(waker(), i);
        }
        // Straight at the field, so the head reclaim never runs and the queues
        // keep all seventy dead entries.
        for i in 0..70 {
            t.active.cancel(i);
        }
        assert_eq!(t.wakers.len(), 70);
        assert_eq!(t.next_deadline(0), None);
    }

    #[test]
    fn the_deadline_follows_the_survivors_across_a_fire() {
        let log: Log = Arc::default();
        let mut t = Timers::<D>::new();
        t.push(recording_waker(&log, 0), 0);
        let dead = t.push(recording_waker(&log, 1), 5);
        t.push(recording_waker(&log, 2), 20);
        t.cancel(dead);

        t.tick(TICKS + 1);
        assert_eq!(*log.lock().unwrap(), [0], "the first one fired");
        assert_eq!(t.wakers.len(), 2, "the cancelled entry is still held");
        // Armed at 20, so due at TICKS + 20 and fired at TICKS + 21 — twenty
        // ticks from the TICKS + 1 we are standing on.
        assert_eq!(
            ticks_of(t.next_deadline(TICKS + 1).unwrap()),
            20,
            "answered from entry 2, over the cancelled entry 1",
        );
    }

    #[test]
    #[ignore]
    fn against_the_wheel() {
        use crate::TimeWheel;
        use std::time::{Instant, SystemTime};

        const TICKS: u64 = 3_000;
        const DURATION_TICKS: u64 = 500;

        for arms_per_tick in [10usize, 100, 500] {
            // Completion latencies: 95% cancel, mostly fast with a 2% tail.
            let mut rng = 0x1234_5678_9abc_def1u64;
            let mut next = move || {
                rng ^= rng << 13;
                rng ^= rng >> 7;
                rng ^= rng << 17;
                rng
            };
            let total = arms_per_tick * TICKS as usize;
            let mut cancel_at: Vec<Option<u64>> = Vec::with_capacity(total);
            for tick in 0..TICKS {
                for _ in 0..arms_per_tick {
                    cancel_at.push(if next() % 100 < 95 {
                        let latency = if next() % 100 < 2 {
                            100 + next() % 300
                        } else {
                            1 + next() % 30
                        };
                        Some(tick + latency.min(DURATION_TICKS - 1))
                    } else {
                        None
                    });
                }
            }
            let horizon = (TICKS + DURATION_TICKS + 1) as usize;
            let mut schedule: Vec<Vec<usize>> = vec![Vec::new(); horizon];
            for (i, at) in cancel_at.iter().enumerate() {
                if let Some(at) = at {
                    schedule[*at as usize].push(i);
                }
            }

            println!(
                "\n{total} timers of 5 s over {TICKS} ticks ({arms_per_tick}/tick), 95% cancelled"
            );

            // --- the wheel ---
            let mut wheel = TimeWheel::new();
            let start = Instant::now();
            let wall = SystemTime::now();
            let mut ids = vec![0usize; total];
            let mut arm = Duration::ZERO;
            let mut cancel = Duration::ZERO;
            let mut tick = Duration::ZERO;
            let mut peak = 0usize;
            let mut cursor = 0usize;
            for t in 0..horizon as u64 {
                let t0 = Instant::now();
                if t < TICKS {
                    for _ in 0..arms_per_tick {
                        ids[cursor] = wheel
                            .init_timer(Duration::from_secs(5), Waker::noop())
                            .unwrap();
                        cursor += 1;
                    }
                }
                arm += t0.elapsed();

                let t0 = Instant::now();
                for &i in &schedule[t as usize] {
                    wheel.drop(ids[i]);
                }
                cancel += t0.elapsed();

                let t0 = Instant::now();
                wheel.tick_at(start + Duration::from_millis(t * 10), wall);
                tick += t0.elapsed();
                peak = peak.max(wheel.active_timers());
            }
            let n = total as f64;
            println!(
                "TimeWheel   arm {:>6.1} ns  cancel {:>6.1} ns  tick {:>6.1} ns  peak {peak:>8}",
                arm.as_nanos() as f64 / n,
                cancel.as_nanos() as f64 / n,
                tick.as_nanos() as f64 / n,
            );

            // --- the queues ---
            let mut timers = Timers::<D>::new();
            let mut tokens = vec![Token(0); total];
            let mut arm = Duration::ZERO;
            let mut cancel = Duration::ZERO;
            let mut tick = Duration::ZERO;
            let mut peak = 0usize;
            let mut cursor = 0usize;
            for t in 0..horizon as u64 {
                let t0 = Instant::now();
                if t < TICKS {
                    for _ in 0..arms_per_tick {
                        tokens[cursor] = timers.push(Waker::noop().clone(), t);
                        cursor += 1;
                    }
                }
                arm += t0.elapsed();

                let t0 = Instant::now();
                for &i in &schedule[t as usize] {
                    timers.cancel(tokens[i]);
                }
                cancel += t0.elapsed();

                let t0 = Instant::now();
                timers.tick(t);
                tick += t0.elapsed();
                peak = peak.max(timers.wakers.len());
            }
            println!(
                "Timers      arm {:>6.1} ns  cancel {:>6.1} ns  tick {:>6.1} ns  peak {peak:>8}",
                arm.as_nanos() as f64 / n,
                cancel.as_nanos() as f64 / n,
                tick.as_nanos() as f64 / n,
            );
        }
    }
}
