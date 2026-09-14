use self::slab::TimerStorage;
use smallvec::SmallVec;
use std::{
    task::Waker,
    time::{Duration, Instant, SystemTime},
};

mod slab;

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
const SMALLVEC_SIZE: usize = 8;

type TimerId = usize;
type Bucket = SmallVec<[TimerId; SMALLVEC_SIZE]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationTooLong;

pub struct TimeWheel {
    storage: TimerStorage,
    buckets: BucketLevels,
    last_tick: Instant,
    last_tick_wall: SystemTime,
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
    ms_level: [Bucket; MS_BUCKETS],
    s_level: [Bucket; S_BUCKETS],
    ms_occupied: Bitset<u128>,
    s_occupied: Bitset<u64>,
}

impl BucketLevels {
    fn new() -> Self {
        Self {
            ms_level: std::array::from_fn(|_| SmallVec::new()),
            s_level: std::array::from_fn(|_| SmallVec::new()),
            ms_occupied: Bitset(0),
            s_occupied: Bitset(0),
        }
    }
}

impl TimeWheel {
    #[must_use]
    pub fn new() -> Self {
        Self {
            storage: TimerStorage::default(),
            buckets: BucketLevels::new(),
            last_tick: Instant::now(),
            last_tick_wall: SystemTime::now(),
            ticks: 0,
        }
    }

    /// Advance the wheel to the current time.
    #[inline]
    pub fn tick(&mut self) {
        self.tick_at(Instant::now(), SystemTime::now());
    }

    /// Advance the wheel to a caller-supplied instant.
    ///
    /// The wheel does not decide where time comes from. That lets a simulation
    /// drive it from a clock it controls, without this crate learning what a
    /// simulation is — and leaves [`tick`](Self::tick) as the only path
    /// production takes.
    pub fn tick_at(&mut self, now: Instant, now_wall: SystemTime) {
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
        self.last_tick_wall = now_wall;
    }

    /// Number of timers currently occupying slab slots: waiting, fired but
    /// not yet polled, or cancelled but not yet swept by their bucket.
    pub fn active_timers(&self) -> usize {
        self.storage.len()
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
            self.buckets.ms_occupied.clear(idx);
            for timer_id in self.buckets.ms_level[idx].drain(..) {
                self.storage.wake(timer_id);
            }
        }

        self.ticks += 1;

        // The seconds slot cascades as its own window opens, into ms slots that
        // have not fired yet — the one just fired is a tick behind.
        if self.ticks.is_multiple_of(TICKS_PER_S) {
            let idx = self.s_idx();
            if self.buckets.s_occupied.is_set(idx) {
                self.buckets.s_occupied.clear(idx);
                let bucket = std::mem::take(&mut self.buckets.s_level[idx]);
                self.cascade(bucket);
            }
        }
    }

    // Re-place each timer by its remaining time; cancelled ids are freed here.
    fn cascade(&mut self, bucket: Bucket) {
        for timer_id in bucket {
            if let Some(deadline) = self.storage.deadline(timer_id) {
                self.place(timer_id, deadline);
            }
        }
    }

    // Finest level whose window still contains `deadline`. A deadline a second
    // or more out is a whole seconds-slot ahead of the current one, which has
    // already cascaded, so that slot is never targeted.
    fn place(&mut self, timer_id: TimerId, deadline: u64) {
        let now = self.ticks;
        let deadline = deadline.max(now);
        let b = &mut self.buckets;

        if deadline - now < TICKS_PER_S {
            let idx = (deadline % TICKS_PER_S) as usize;
            b.ms_occupied.set(idx);
            b.ms_level[idx].push(timer_id);
        } else {
            let idx = ((deadline / TICKS_PER_S) % S_BUCKETS as u64) as usize;
            b.s_occupied.set(idx);
            b.s_level[idx].push(timer_id);
        }
    }

    pub fn poll(&mut self, id: usize, waker: &Waker) -> std::task::Poll<()> {
        self.storage.poll(id, waker)
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
        let timer_id = self.storage.create(waker, deadline);
        self.place(timer_id, deadline);
        Ok(timer_id)
    }

    pub fn drop(&mut self, id: usize) {
        self.storage.drop(id);
    }

    pub fn now(&self) -> SystemTime {
        self.last_tick_wall
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
        let ticks = if let Some(i) = b.ms_occupied.next_set(self.ms_idx(), MS_BUCKETS) {
            i as u64
        } else if let Some(i) = b.s_occupied.next_set(self.s_idx(), S_BUCKETS) {
            self.ticks_until(i, TICKS_PER_S)
        } else {
            return None;
        };
        Some(Duration::from_millis((ticks + 1) * MS_TICK))
    }
}

impl Default for TimeWheel {
    fn default() -> Self {
        Self::new()
    }
}
