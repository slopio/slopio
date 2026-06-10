use crate::slab::TimerStorage;
use smallvec::SmallVec;
use std::{
    task::Waker,
    time::{Duration, Instant, SystemTime},
};

mod slab;

const MS_TICK: u64 = 10; //10ms
const MS_BUCKETS: usize = 20; //200ms
const S_BUCKETS: usize = 60;
const H_BUCKETS: usize = 24;
const MAX_DURATION_HOURS: u64 = 24;
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
    current_ms_idx: usize,
    current_s_idx: usize,
    current_h_idx: usize,
}

struct Bitset<T>(T);

impl Bitset<u16> {
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
}

impl Bitset<u32> {
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
}

impl Bitset<u64> {
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
}

struct BucketLevels {
    ms_level: [Bucket; MS_BUCKETS],
    s_level: [Bucket; S_BUCKETS],
    h_level: [Bucket; H_BUCKETS],
    ms_occupied: Bitset<u32>,
    s_occupied: Bitset<u64>,
    h_occupied: Bitset<u32>,
}

impl BucketLevels {
    fn new() -> Self {
        Self {
            ms_level: std::array::from_fn(|_| SmallVec::new()),
            s_level: std::array::from_fn(|_| SmallVec::new()),
            h_level: std::array::from_fn(|_| SmallVec::new()),
            ms_occupied: Bitset(0),
            s_occupied: Bitset(0),
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
            current_ms_idx: 0,
            current_s_idx: 0,
            current_h_idx: 0,
        }
    }

    pub fn tick(&mut self) {
        let now = Instant::now();
        let now_wall = SystemTime::now();
        let elapsed = now.duration_since(self.last_tick);
        let ticks_to_process = (elapsed.as_millis() / MS_TICK as u128) as usize;

        for _ in 0..ticks_to_process {
            self.process_single_tick();
        }

        // Advance by whole ticks only, keeping the sub-tick remainder.
        // Resetting to `now` unconditionally would discard up to MS_TICK of
        // elapsed time per call — called faster than once per MS_TICK (a busy
        // event loop), the wheel would never accumulate a full tick and never
        // advance.
        self.last_tick += Duration::from_millis(ticks_to_process as u64 * MS_TICK);
        self.last_tick_wall = now_wall;
    }

    /// Number of timers currently occupying slab slots: waiting, fired but
    /// not yet polled, or cancelled but not yet swept by their bucket.
    pub fn active_timers(&self) -> usize {
        self.storage.len()
    }

    fn process_single_tick(&mut self) {
        if self.buckets.ms_occupied.is_set(self.current_ms_idx) {
            self.buckets.ms_occupied.clear(self.current_ms_idx);

            for timer_id in self.buckets.ms_level[self.current_ms_idx].drain(..) {
                self.storage.wake(timer_id);
            }
        }

        self.current_ms_idx = (self.current_ms_idx + 1) % MS_BUCKETS;

        if self.current_ms_idx == 0 {
            self.cascade_from_seconds();
            self.current_s_idx = (self.current_s_idx + 1) % S_BUCKETS;

            if self.current_s_idx == 0 {
                self.cascade_from_hours();
                self.current_h_idx = (self.current_h_idx + 1) % H_BUCKETS;
            }
        }
    }

    fn cascade_from_seconds(&mut self) {
        if !self.buckets.s_occupied.is_set(self.current_s_idx) {
            return;
        }

        self.buckets.s_occupied.clear(self.current_s_idx);

        let bucket = std::mem::take(&mut self.buckets.s_level[self.current_s_idx]);
        self.buckets.ms_occupied.set(self.current_ms_idx);
        self.buckets.ms_level[self.current_ms_idx].extend(bucket);
    }

    fn cascade_from_hours(&mut self) {
        if !self.buckets.h_occupied.is_set(self.current_h_idx) {
            return;
        }

        self.buckets.h_occupied.clear(self.current_h_idx);

        let bucket = std::mem::take(&mut self.buckets.h_level[self.current_h_idx]);
        self.buckets.s_occupied.set(self.current_s_idx);
        self.buckets.s_level[self.current_s_idx].extend(bucket);
    }

    fn compute_ms_bucket_from_ms(&self, ms: u64) -> usize {
        let bucket_offset = (ms / MS_TICK) as usize;
        (self.current_ms_idx + bucket_offset.min(MS_BUCKETS - 1)) % MS_BUCKETS
    }

    fn compute_s_bucket_from_ms(&self, ms: u64) -> usize {
        let secs = (ms / 1000) as usize;
        (self.current_s_idx + secs.min(S_BUCKETS - 1)) % S_BUCKETS
    }

    fn compute_h_bucket_from_ms(&self, ms: u64) -> usize {
        let hours = (ms / 3_600_000) as usize;
        (self.current_h_idx + hours.min(H_BUCKETS - 1)) % H_BUCKETS
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

        let timer_id = self.storage.create(waker);

        let ms_threshold = (MS_BUCKETS as u64) * MS_TICK;
        let s_threshold = (S_BUCKETS as u64) * 1000;

        if total_ms < ms_threshold {
            let idx = self.compute_ms_bucket_from_ms(total_ms);
            self.buckets.ms_occupied.set(idx);
            self.buckets.ms_level[idx].push(timer_id);
        } else if total_ms < s_threshold {
            let idx = self.compute_s_bucket_from_ms(total_ms);
            self.buckets.s_occupied.set(idx);
            self.buckets.s_level[idx].push(timer_id);
        } else {
            let idx = self.compute_h_bucket_from_ms(total_ms);
            self.buckets.h_occupied.set(idx);
            self.buckets.h_level[idx].push(timer_id);
        }

        Ok(timer_id)
    }

    pub fn drop(&mut self, id: usize) {
        self.storage.drop(id);
    }

    pub fn now(&self) -> SystemTime {
        self.last_tick_wall
    }

    /// returns the duration until the next timer is triggered, or None if no timers are registered.
    pub fn next_deadline(&self) -> Option<Duration> {
        for i in 0..MS_BUCKETS {
            let idx = (self.current_ms_idx + i) % MS_BUCKETS;
            if self.buckets.ms_occupied.is_set(idx) {
                return Some(Duration::from_millis(i as u64 * MS_TICK));
            }
        }

        for i in 0..S_BUCKETS {
            let idx = (self.current_s_idx + i) % S_BUCKETS;
            if self.buckets.s_occupied.is_set(idx) {
                let ms_remaining = (MS_BUCKETS - self.current_ms_idx) * MS_TICK as usize;
                let s_remaining = i * 1000;
                return Some(Duration::from_millis((ms_remaining + s_remaining) as u64));
            }
        }

        for i in 0..H_BUCKETS {
            let idx = (self.current_h_idx + i) % H_BUCKETS;
            if self.buckets.h_occupied.is_set(idx) {
                let ms_remaining = (MS_BUCKETS - self.current_ms_idx) * MS_TICK as usize;
                let s_remaining = (S_BUCKETS - self.current_s_idx - 1) * 1000;
                let h_remaining = i * 3600 * 1000;
                return Some(Duration::from_millis(
                    (ms_remaining + s_remaining + h_remaining) as u64,
                ));
            }
        }

        None
    }
}

impl Default for TimeWheel {
    fn default() -> Self {
        Self::new()
    }
}
