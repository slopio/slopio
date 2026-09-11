use self::slab::TimerStorage;
use smallvec::SmallVec;
use std::{
    task::Waker,
    time::{Duration, Instant, SystemTime},
};

mod slab;

// One ring of 10ms slots covering a second, the longest timer the wheel takes.
// Nothing cascades: a deadline always lands in the only level there is.
const MS_TICK: u64 = 10;
const MAX_TICKS: u64 = 100;
/// The longest timer [`TimeWheel::init_timer`] accepts, inclusive.
pub const MAX_DURATION: Duration = Duration::from_millis(MAX_TICKS * MS_TICK);
// One slot per tick of the window, plus one: a deadline a full second out must
// not alias the slot the clock is standing on, which has already fired.
const BUCKETS: usize = MAX_TICKS as usize + 1;
const SMALLVEC_SIZE: usize = 8;

type TimerId = usize;
type Bucket = SmallVec<[TimerId; SMALLVEC_SIZE]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DurationTooLong;

pub struct TimeWheel {
    storage: TimerStorage,
    buckets: [Bucket; BUCKETS],
    occupied: Bitset,
    last_tick: Instant,
    last_tick_wall: SystemTime,
    // Whole ticks processed since creation; the slot index derives from it.
    ticks: u64,
}

struct Bitset(u128);

impl Bitset {
    // Only the low BUCKETS bits are live; the rest stay zero.
    const MASK: u128 = (1 << BUCKETS) - 1;

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

    // Distance from `from` to the next set bit, wrapping around the ring.
    #[inline]
    fn next_set(&self, from: usize) -> Option<usize> {
        let rotated = ((self.0 >> from) | (self.0 << (BUCKETS - from))) & Self::MASK;
        (rotated != 0).then(|| rotated.trailing_zeros() as usize)
    }
}

impl TimeWheel {
    #[must_use]
    pub fn new() -> Self {
        Self {
            storage: TimerStorage::default(),
            buckets: std::array::from_fn(|_| SmallVec::new()),
            occupied: Bitset(0),
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
    fn idx(&self) -> usize {
        (self.ticks % BUCKETS as u64) as usize
    }

    fn process_single_tick(&mut self) {
        let idx = self.idx();
        if self.occupied.is_set(idx) {
            self.occupied.clear(idx);
            for timer_id in self.buckets[idx].drain(..) {
                self.storage.wake(timer_id);
            }
        }
        self.ticks += 1;
    }

    // A deadline is at most MAX_TICKS out, so its slot is always free: the ring
    // is one longer than the window it holds.
    fn place(&mut self, timer_id: TimerId, deadline: u64) {
        let idx = (deadline % BUCKETS as u64) as usize;
        self.occupied.set(idx);
        self.buckets[idx].push(timer_id);
    }

    pub fn poll(&mut self, id: usize, waker: &Waker) -> std::task::Poll<()> {
        self.storage.poll(id, waker)
    }

    /// Arm a timer, rounded down to the enclosing 10ms tick.
    ///
    /// Anything past [`MAX_DURATION`] is rejected rather than truncated: the
    /// ring holds exactly one second and has nowhere to put it.
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

        let deadline = self.ticks + duration.as_millis() as u64 / MS_TICK;
        let timer_id = self.storage.create(waker);
        self.place(timer_id, deadline);
        Ok(timer_id)
    }

    pub fn drop(&mut self, id: usize) {
        self.storage.drop(id);
    }

    pub fn now(&self) -> SystemTime {
        self.last_tick_wall
    }

    /// Time until the next occupied slot opens, or None if no timers are
    /// registered. Exact, up to the 10ms tick: with a single level there is
    /// nothing to cascade, so the slot that opens is the one that fires.
    pub fn next_deadline(&self) -> Option<Duration> {
        let i = self.occupied.next_set(self.idx())?;
        Some(Duration::from_millis(i as u64 * MS_TICK))
    }
}

impl Default for TimeWheel {
    fn default() -> Self {
        Self::new()
    }
}
