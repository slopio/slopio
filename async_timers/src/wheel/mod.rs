use self::nodes::{BUCKET, NIL, Nodes, State};
use std::{
    task::Waker,
    time::{Duration, Instant},
};

mod nodes;

// Two levels, ms → s. The seconds period is a whole number of ms ticks, so a
// seconds slot cascades exactly when its window opens.
const MS_TICK: u64 = 10;
// No spare slot: the ms level only ever holds deadlines under a second out.
const MS_BUCKETS: usize = 100;
// 61 and not 60: a minute-out deadline must not alias the seconds slot the
// clock stands on, which has already cascaded.
const S_BUCKETS: usize = 61;
const TICKS_PER_S: u64 = MS_BUCKETS as u64;
const MAX_TICKS: u64 = TICKS_PER_S * (S_BUCKETS as u64 - 1);
/// The longest timer [`TimeWheel::init_timer`] accepts, inclusive.
pub const MAX_DURATION: Duration = Duration::from_millis(MAX_TICKS * MS_TICK);
// One list head per slot, ms slots first: bucket `b` is ms slot `b` below
// `MS_BUCKETS`, seconds slot `b - MS_BUCKETS` above.
const BUCKETS: usize = MS_BUCKETS + S_BUCKETS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationTooLong;

/// Each slot is an intrusive doubly linked list threaded through the timers'
/// nodes, kernel style: arming links at the head, cancelling unlinks in O(1)
/// and frees the node at once, and a cascade relinks without copying.
pub struct TimeWheel {
    nodes: Nodes,
    buckets: BucketLevels,
    last_tick: Instant,
    // Whole ticks processed since creation; every level index derives from it.
    ticks: u64,
}

struct Bitset<T>(T);

macro_rules! impl_bitset {
    ($($t:ty),*) => {$(
        impl Bitset<$t> {
            #[inline]
            fn set(&mut self, idx: usize) {
                self.0 |= 1 << idx;
            }

            #[inline]
            fn clear(&mut self, idx: usize) {
                self.0 &= !(1 << idx);
            }

            #[inline]
            fn is_set(&self, idx: usize) -> bool {
                (self.0 & (1 << idx)) != 0
            }

            // Distance from `from` to the next set bit on a ring of `len` bits.
            #[inline]
            fn next_set(&self, from: usize, len: usize) -> Option<usize> {
                let mask: $t = (1 << len) - 1;
                let rotated = ((self.0 >> from) | (self.0 << (len - from))) & mask;
                (rotated != 0).then(|| rotated.trailing_zeros() as usize)
            }
        }
    )*};
}

impl_bitset!(u64, u128);

struct BucketLevels {
    /// First node of each slot's list, or `NIL`.
    heads: [u32; BUCKETS],
    /// Exact: a bit is set iff its list is non-empty.
    ms_occupied: Bitset<u128>,
    s_occupied: Bitset<u64>,
}

impl BucketLevels {
    fn new() -> Self {
        Self {
            heads: [NIL; BUCKETS],
            ms_occupied: Bitset(0),
            s_occupied: Bitset(0),
        }
    }

    #[inline]
    fn mark(&mut self, bucket: usize) {
        match bucket.checked_sub(MS_BUCKETS) {
            None => self.ms_occupied.set(bucket),
            Some(s) => self.s_occupied.set(s),
        }
    }

    #[inline]
    fn unmark(&mut self, bucket: usize) {
        match bucket.checked_sub(MS_BUCKETS) {
            None => self.ms_occupied.clear(bucket),
            Some(s) => self.s_occupied.clear(s),
        }
    }

    /// Detach a whole slot: its list, now owned by the caller, and an empty slot.
    #[inline]
    fn take(&mut self, bucket: usize) -> u32 {
        self.unmark(bucket);
        std::mem::replace(&mut self.heads[bucket], NIL)
    }
}

impl TimeWheel {
    #[must_use]
    pub fn new() -> Self {
        Self::new_at(Instant::now())
    }

    /// A wheel whose time starts at a caller-supplied instant, for a clock
    /// that [`tick_at`](Self::tick_at) will keep driving.
    #[must_use]
    pub fn new_at(now: Instant) -> Self {
        Self {
            nodes: Nodes::default(),
            buckets: BucketLevels::new(),
            last_tick: now,
            ticks: 0,
        }
    }

    /// Advance the wheel to the current time.
    #[inline]
    pub fn tick(&mut self) {
        self.tick_at(Instant::now());
    }

    /// Advance the wheel to a caller-supplied instant.
    ///
    /// The wheel does not decide where time comes from. That lets a simulation
    /// drive it from a clock it controls, without this crate learning what a
    /// simulation is — and leaves [`tick`](Self::tick) as the only path
    /// production takes.
    pub fn tick_at(&mut self, now: Instant) {
        let elapsed = now.duration_since(self.last_tick);
        let ticks_to_process = (elapsed.as_millis() / MS_TICK as u128) as u64;

        for _ in 0..ticks_to_process {
            self.process_single_tick();
        }

        // Advance by whole ticks only, keeping the sub-tick remainder.
        // Resetting to `now` unconditionally would discard up to MS_TICK of
        // elapsed time per call — called faster than once per MS_TICK (a busy
        // event loop), the wheel would never accumulate a full tick and never
        // advance.
        self.last_tick += Duration::from_millis(ticks_to_process * MS_TICK);
    }

    /// Number of timers holding a node: waiting, or fired but not yet polled.
    /// A cancelled timer's node is freed by the cancel itself.
    pub fn active_timers(&self) -> usize {
        self.nodes.live()
    }

    /// Whole ticks processed since creation. The reference clock for anything
    /// else the driver advances alongside the wheel.
    pub fn ticks(&self) -> u64 {
        self.ticks
    }

    #[inline]
    fn ms_idx(&self) -> usize {
        (self.ticks % TICKS_PER_S) as usize
    }

    #[inline]
    fn s_idx(&self) -> usize {
        ((self.ticks / TICKS_PER_S) % S_BUCKETS as u64) as usize
    }

    fn process_single_tick(&mut self) {
        let idx = self.ms_idx();
        if self.buckets.ms_occupied.is_set(idx) {
            let mut id = self.buckets.take(idx);
            while id != NIL {
                // Read before firing: `next` is dead once the node is `Done`.
                let next = self.nodes.get(id).next;
                if let Some(waker) = self.nodes.fire(id) {
                    waker.wake();
                }
                id = next;
            }
        }

        self.ticks += 1;

        // The seconds slot cascades as its own window opens, into ms slots that
        // have not fired yet — the one just fired is a tick behind.
        if self.ticks.is_multiple_of(TICKS_PER_S) {
            let idx = self.s_idx();
            if self.buckets.s_occupied.is_set(idx) {
                let head = self.buckets.take(MS_BUCKETS + idx);
                self.cascade(head);
            }
        }
    }

    // Relink each timer by its remaining time; nothing is copied or freed.
    fn cascade(&mut self, mut id: u32) {
        while id != NIL {
            let node = self.nodes.get(id);
            let (next, deadline) = (node.next, node.deadline);
            self.place(id, deadline);
            id = next;
        }
    }

    // Finest level whose window still contains `deadline`. A deadline a second
    // or more out is a whole seconds-slot ahead of the current one, which has
    // already cascaded, so that slot is never targeted.
    fn place(&mut self, id: u32, deadline: u64) {
        let now = self.ticks;
        let deadline = deadline.max(now);
        let bucket = if deadline - now < TICKS_PER_S {
            (deadline % TICKS_PER_S) as usize
        } else {
            MS_BUCKETS + ((deadline / TICKS_PER_S) % S_BUCKETS as u64) as usize
        };
        self.link(id, bucket);
    }

    // At the head, like the kernel: no tail to keep, so a slot fires newest first.
    #[inline]
    fn link(&mut self, id: u32, bucket: usize) {
        let head = self.buckets.heads[bucket];
        let node = self.nodes.get_mut(id);
        node.next = head;
        node.prev = BUCKET | bucket as u32;
        if head != NIL {
            self.nodes.get_mut(head).prev = id;
        }
        self.buckets.heads[bucket] = id;
        self.buckets.mark(bucket);
    }

    // O(1) wherever the node sits: `prev` names its predecessor or its bucket.
    #[inline]
    fn unlink(&mut self, id: u32) {
        let node = self.nodes.get(id);
        let (prev, next) = (node.prev, node.next);
        if prev & BUCKET != 0 {
            let bucket = (prev & !BUCKET) as usize;
            self.buckets.heads[bucket] = next;
            if next == NIL {
                self.buckets.unmark(bucket);
            }
        } else {
            self.nodes.get_mut(prev).next = next;
        }
        if next != NIL {
            self.nodes.get_mut(next).prev = prev;
        }
    }

    /// `Ready` once the timer has fired, which also frees it: the id must not
    /// be used again after `Ready` or after [`drop`](Self::drop).
    pub fn poll(&mut self, id: usize, waker: &Waker) -> std::task::Poll<()> {
        let id = id as u32;
        match self.nodes.state(id) {
            State::Waiting => {
                match &mut self.nodes.get_mut(id).waker {
                    Some(held) if held.will_wake(waker) => {}
                    slot => *slot = Some(waker.clone()),
                }
                std::task::Poll::Pending
            }
            State::Done => {
                self.nodes.release(id);
                std::task::Poll::Ready(())
            }
            State::Free => unreachable!("timer {id} polled after it was freed"),
        }
    }

    /// Arm a timer, rounded down to the enclosing 10ms tick.
    ///
    /// Anything past [`MAX_DURATION`] is rejected rather than truncated: the
    /// seconds level reaches exactly one minute and has nowhere to put it.
    ///
    /// A slot is drained by the tick that *reaches* it, so a deadline `n` ticks
    /// out is served by the n+1-th tick from here — the duration plus whatever
    /// is left of the current tick. The slot is picked one short to cancel
    /// that, which trades a wait of up to a tick too long for one up to a tick
    /// too short. Neither is reachable without knowing how far into the current
    /// tick we are, and the wheel is the only clock: nobody can tell it.
    pub fn init_timer(
        &mut self,
        duration: Duration,
        waker: &Waker,
    ) -> Result<usize, DurationTooLong> {
        // Compared as a Duration, not in milliseconds: `as_millis() as u64`
        // truncates a large enough duration into a small one that would pass.
        if duration > MAX_DURATION {
            return Err(DurationTooLong);
        }

        // Saturating, and `place` clamps to the current slot in turn: under one
        // tick asks for the slot behind us, which is the one about to drain.
        let ticks = (duration.as_millis() as u64 / MS_TICK).saturating_sub(1);
        let deadline = self.ticks + ticks;
        let id = self.nodes.alloc(waker, deadline);
        self.place(id, deadline);
        Ok(id as usize)
    }

    /// Cancel a timer, or release one that fired unpolled. Either way its node
    /// is freed now, not when its slot comes around.
    pub fn drop(&mut self, id: usize) {
        let id = id as u32;
        match self.nodes.state(id) {
            State::Waiting => self.unlink(id),
            State::Done => {}
            State::Free => unreachable!("timer {id} dropped twice"),
        }
        self.nodes.release(id);
    }

    // Ticks until the slot `i` ahead on a level of period `period` opens.
    #[inline]
    fn ticks_until(&self, i: usize, period: u64) -> u64 {
        (i as u64 * period).saturating_sub(self.ticks % period)
    }

    /// Lower bound on the time until the next timer fires, or None if no timers
    /// are registered. Exact for the ms level; a seconds slot answers with when
    /// it cascades, which is never past the earliest deadline it holds.
    ///
    /// The `+ 1` is the same offset [`init_timer`](Self::init_timer) takes off:
    /// a slot `i` ahead is drained by the i+1-th tick, so answering `i` sends
    /// the loop back to sleep one tick short of the firing, every time.
    pub fn next_deadline(&self) -> Option<Duration> {
        let b = &self.buckets;
        let ms = b
            .ms_occupied
            .next_set(self.ms_idx(), MS_BUCKETS)
            .map(|i| i as u64);
        let s = b
            .s_occupied
            .next_set(self.s_idx(), S_BUCKETS)
            .map(|i| self.ticks_until(i, TICKS_PER_S));
        // Both: a seconds slot can cascade into an ms slot ahead of the occupied one.
        let ticks = match (ms, s) {
            (Some(ms), Some(s)) => ms.min(s),
            (ms, s) => ms.or(s)?,
        };
        Some(Duration::from_millis((ticks + 1) * MS_TICK))
    }
}

impl Default for TimeWheel {
    fn default() -> Self {
        Self::new()
    }
}
