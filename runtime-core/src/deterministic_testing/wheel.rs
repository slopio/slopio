//! A timing wheel that decides when submitted ops complete.
//!
//! Each op is assigned a random *bucket* on submission, and the bucket is the
//! whole schedule: bucket `i` fires `i + 1` ticks later, a tick being the
//! configured step. Nothing carries a timestamp, nothing is sorted, and nothing
//! is compared — an op's position in the wheel *is* its due time.
//!
//! The randomness is seeded, which is the point of the design. A seed
//! reproduces an interleaving exactly, and sweeping seeds explores orderings
//! that are rare and unrepeatable against a real kernel.

use std::collections::VecDeque;

use super::clock::{self, Nanos};
use super::sqe::Sqe;

/// How many latency classes an op can be assigned.
pub const BUCKETS: usize = 6;

/// One more slot than there are buckets.
///
/// Offsets run `1..=BUCKETS` from the cursor. With only `BUCKETS` slots the
/// longest offset would wrap onto the cursor itself — the slot being drained —
/// so the slowest bucket would fire immediately instead of last.
const SLOTS: usize = BUCKETS + 1;

/// What a completion will say.
#[derive(Clone, Copy, Debug)]
pub struct Outcome {
    pub result: i32,
    pub flags: u32,
}

/// A submitted op waiting to complete.
pub struct Pending {
    pub sqe: Sqe,
    /// Submission order. The wheel never consults it — slot position and FIFO
    /// order settle everything — but tests report ops in this order.
    pub seq: u64,
    pub outcome: Outcome,
}

/// A scheduled tick: which slot fires, and when.
pub struct Tick {
    slot: usize,
    pub deadline: Nanos,
}

/// SplitMix64. Four lines, good statistical quality, no dependency — and
/// entirely determined by its seed, which is the only property that matters
/// here.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn bucket(&mut self) -> usize {
        (self.next_u64() % BUCKETS as u64) as usize
    }
}

pub struct Wheel {
    /// Ops due at each tick, relative to `cursor`. FIFO within a slot, which is
    /// submission order, which is the order they complete in.
    slots: [VecDeque<Pending>; SLOTS],
    cursor: usize,
    /// Absolute deadline of the slot one tick ahead of `cursor`.
    next_tick_at: Nanos,
    step: Nanos,
    rng: SplitMix64,
}

impl Wheel {
    pub fn new(step: Nanos, seed: u64) -> Self {
        Self {
            slots: std::array::from_fn(|_| VecDeque::new()),
            cursor: 0,
            next_tick_at: clock::now() + step,
            step,
            rng: SplitMix64(seed),
        }
    }

    /// Assign a bucket and enqueue. O(1) — a modular index and a push.
    pub fn schedule(&mut self, pending: Pending) {
        let offset = self.rng.bucket() + 1; // 1..=BUCKETS, never 0
        let slot = (self.cursor + offset) % SLOTS;
        self.slots[slot].push_back(pending);
    }

    /// The next tick carrying work, or `None` if the wheel is empty.
    ///
    /// Scanning forward past empty slots is what keeps this O(BUCKETS) instead
    /// of O(ticks): an op six ticks out costs one sleep, not six.
    pub fn next_tick(&mut self) -> Option<Tick> {
        // If the wheel sat idle past its own deadline, restart the cadence from
        // now. Persisting the clamp rather than recomputing it per call is what
        // stops scheduled ops from starving: a wheel woken repeatedly for other
        // reasons would otherwise push its own work further out every time.
        let now = clock::now();
        if self.next_tick_at < now {
            self.next_tick_at = now + self.step;
        }

        (1..=BUCKETS).find_map(|d| {
            let slot = (self.cursor + d) % SLOTS;
            (!self.slots[slot].is_empty()).then(|| Tick {
                slot,
                deadline: self.next_tick_at + self.step * (d as Nanos - 1),
            })
        })
    }

    /// Advance to `tick` and take everything on it. Call once its deadline has
    /// actually passed.
    pub fn fire(&mut self, tick: Tick) -> VecDeque<Pending> {
        self.cursor = tick.slot;
        self.next_tick_at = tick.deadline + self.step;
        std::mem::take(&mut self.slots[tick.slot])
    }

    pub fn iter(&self) -> impl Iterator<Item = &Pending> {
        self.slots.iter().flatten()
    }

    pub fn contains(&self, user_data: u64) -> bool {
        self.iter().any(|p| p.sqe.user_data == user_data)
    }

    /// Remove a scheduled op, for a test completing it by hand ahead of its
    /// tick.
    pub fn take_by_user_data(&mut self, user_data: u64) -> Option<Pending> {
        for slot in &mut self.slots {
            if let Some(i) = slot.iter().position(|p| p.sqe.user_data == user_data) {
                return slot.remove(i);
            }
        }
        None
    }
}
