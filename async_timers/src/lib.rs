use crate::slab::TimerStorage;
use smallvec::SmallVec;
use std::{
    task::Waker,
    time::{Duration, Instant, SystemTime},
};

mod slab;

// Hierarchical wheel: ms → s → min → h. Every level's period is a whole
// number of the level below, so a slot cascades exactly when its window opens.
const MS_TICK: u64 = 10;
const MS_BUCKETS: usize = 100;
const S_BUCKETS: usize = 60;
const M_BUCKETS: usize = 60;
// 25, not 24: a deadline up to 24h out can land 24 hour-slots ahead of `ticks`.
const H_BUCKETS: usize = 25;
const MAX_DURATION_HOURS: u64 = 24;
const SMALLVEC_SIZE: usize = 8;

const TICKS_PER_S: u64 = MS_BUCKETS as u64;
const TICKS_PER_M: u64 = TICKS_PER_S * S_BUCKETS as u64;
const TICKS_PER_H: u64 = TICKS_PER_M * M_BUCKETS as u64;

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

impl_bitset!(u32, u64, u128);

struct BucketLevels {
    ms_level: [Bucket; MS_BUCKETS],
    s_level: [Bucket; S_BUCKETS],
    m_level: [Bucket; M_BUCKETS],
    h_level: [Bucket; H_BUCKETS],
    ms_occupied: Bitset<u128>,
    s_occupied: Bitset<u64>,
    m_occupied: Bitset<u64>,
    h_occupied: Bitset<u32>,
}

impl BucketLevels {
    fn new() -> Self {
        Self {
            ms_level: std::array::from_fn(|_| SmallVec::new()),
            s_level: std::array::from_fn(|_| SmallVec::new()),
            m_level: std::array::from_fn(|_| SmallVec::new()),
            h_level: std::array::from_fn(|_| SmallVec::new()),
            ms_occupied: Bitset(0),
            s_occupied: Bitset(0),
            m_occupied: Bitset(0),
            h_occupied: Bitset(0),
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

    #[inline]
    fn ms_idx(&self) -> usize {
        (self.ticks % TICKS_PER_S) as usize
    }

    #[inline]
    fn s_idx(&self) -> usize {
        ((self.ticks / TICKS_PER_S) % S_BUCKETS as u64) as usize
    }

    #[inline]
    fn m_idx(&self) -> usize {
        ((self.ticks / TICKS_PER_M) % M_BUCKETS as u64) as usize
    }

    #[inline]
    fn h_idx(&self) -> usize {
        ((self.ticks / TICKS_PER_H) % H_BUCKETS as u64) as usize
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

        // Coarse levels first: a cascaded timer may land in a finer slot that
        // is itself opening on this same tick.
        if self.ticks.is_multiple_of(TICKS_PER_S) {
            if self.ticks.is_multiple_of(TICKS_PER_M) {
                if self.ticks.is_multiple_of(TICKS_PER_H) {
                    let idx = self.h_idx();
                    if self.buckets.h_occupied.is_set(idx) {
                        self.buckets.h_occupied.clear(idx);
                        let bucket = std::mem::take(&mut self.buckets.h_level[idx]);
                        self.cascade(bucket);
                    }
                }
                let idx = self.m_idx();
                if self.buckets.m_occupied.is_set(idx) {
                    self.buckets.m_occupied.clear(idx);
                    let bucket = std::mem::take(&mut self.buckets.m_level[idx]);
                    self.cascade(bucket);
                }
            }
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

    // Finest level whose window still contains `deadline`; the current slot of
    // a coarser level has already cascaded, so it is never targeted.
    fn place(&mut self, timer_id: TimerId, deadline: u64) {
        let now = self.ticks;
        let deadline = deadline.max(now);
        let b = &mut self.buckets;

        if deadline - now < TICKS_PER_S {
            let idx = (deadline % TICKS_PER_S) as usize;
            b.ms_occupied.set(idx);
            b.ms_level[idx].push(timer_id);
        } else if deadline / TICKS_PER_S - now / TICKS_PER_S < S_BUCKETS as u64 {
            let idx = ((deadline / TICKS_PER_S) % S_BUCKETS as u64) as usize;
            b.s_occupied.set(idx);
            b.s_level[idx].push(timer_id);
        } else if deadline / TICKS_PER_M - now / TICKS_PER_M < M_BUCKETS as u64 {
            let idx = ((deadline / TICKS_PER_M) % M_BUCKETS as u64) as usize;
            b.m_occupied.set(idx);
            b.m_level[idx].push(timer_id);
        } else {
            let idx = ((deadline / TICKS_PER_H) % H_BUCKETS as u64) as usize;
            b.h_occupied.set(idx);
            b.h_level[idx].push(timer_id);
        }
    }

    pub fn poll(&mut self, id: usize, waker: &Waker) -> std::task::Poll<()> {
        self.storage.poll(id, waker)
    }

    pub fn init_timer(
        &mut self,
        duration: Duration,
        waker: &Waker,
    ) -> Result<usize, DurationTooLong> {
        let total_ms = duration.as_millis() as u64;
        if total_ms >= MAX_DURATION_HOURS * 3_600_000 {
            return Err(DurationTooLong);
        }

        let deadline = self.ticks + total_ms / MS_TICK;
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

    /// Lower bound on the time until the next timer fires, or None if no
    /// timers are registered. Exact for the ms level; for coarser levels it is
    /// when the slot cascades, which never overshoots the earliest deadline.
    pub fn next_deadline(&self) -> Option<Duration> {
        let b = &self.buckets;
        let ticks = if let Some(i) = b.ms_occupied.next_set(self.ms_idx(), MS_BUCKETS) {
            i as u64
        } else if let Some(i) = b.s_occupied.next_set(self.s_idx(), S_BUCKETS) {
            self.ticks_until(i, TICKS_PER_S)
        } else if let Some(i) = b.m_occupied.next_set(self.m_idx(), M_BUCKETS) {
            self.ticks_until(i, TICKS_PER_M)
        } else if let Some(i) = b.h_occupied.next_set(self.h_idx(), H_BUCKETS) {
            self.ticks_until(i, TICKS_PER_H)
        } else {
            return None;
        };
        Some(Duration::from_millis(ticks * MS_TICK))
    }
}

impl Default for TimeWheel {
    fn default() -> Self {
        Self::new()
    }
}
