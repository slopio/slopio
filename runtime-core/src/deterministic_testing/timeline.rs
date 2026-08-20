//! When submitted ops complete.
//!
//! Ops are held in one list ordered by due time, so the front is always the
//! next event. An op that a scenario timed gets exactly the delay it asked for;
//! anything else draws one of a few latency classes from the seed, which is
//! what makes completion order vary from seed to seed.
//!
//! Those two are deliberately separate concerns — the ordering *structure* and
//! the latency *policy*. Merging them, as a fixed-slot wheel does, caps the
//! delays a scenario can express at the number of slots.

use std::time::Duration;

use super::clock::Nanos;
use super::fds::FdKind;
use super::sqe::Sqe;

/// How many latencies an unscripted op can draw. Purely a policy knob: the
/// list below holds any due time, whatever this is set to.
pub const LATENCY_CLASSES: u64 = 6;

/// What an op completes with.
#[derive(Clone, Debug)]
pub enum Outcome {
    /// Complete with this result and these flags, touching no memory.
    Result { result: i32, flags: u32 },
    /// Write these bytes into the op's own buffer, then complete with the
    /// number written — what the kernel does for a recv into a caller buffer.
    Data(Vec<u8>),
    /// Complete with a freshly minted descriptor, for ops that hand one back.
    NewFd(FdKind),
}

impl Outcome {
    /// A plain result, no flags.
    pub fn result(result: i32) -> Self {
        Self::Result { result, flags: 0 }
    }
}

pub struct Pending {
    pub sqe: Sqe,
    /// Submission order. Breaks ties between ops due at the same instant, and
    /// is the order tests see in `inflight()`.
    pub seq: u64,
    pub outcome: Outcome,
}

struct Scheduled {
    pending: Pending,
    due: Nanos,
}

/// SplitMix64: short, good quality, no dependency, and entirely determined by
/// its seed — the only property that matters here.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A latency class, `1..=LATENCY_CLASSES`.
    fn latency_class(&mut self) -> u64 {
        self.next_u64() % LATENCY_CLASSES + 1
    }
}

pub struct Timeline {
    /// Ordered by `(due, seq)`, so the front is the next event. Kept sorted on
    /// insert rather than searched: the front is read on every blocking wait.
    scheduled: Vec<Scheduled>,
    step: Nanos,
    rng: SplitMix64,
}

impl Timeline {
    pub fn new(step: Nanos, seed: u64) -> Self {
        Self {
            scheduled: Vec::new(),
            step,
            rng: SplitMix64(seed),
        }
    }

    /// Enqueue an op due `now` plus its latency — `delay` if the scenario timed
    /// one, a drawn class otherwise.
    ///
    /// `now` is passed in rather than read here so a whole submission batch
    /// shares one instant. Reading the clock per op would give two ops of the
    /// same latency due times a few nanoseconds apart, making their order an
    /// artefact of how long the submit loop took instead of the `seq` tie-break
    /// below.
    ///
    /// A latency is drawn either way. Skipping the draw when a delay is given
    /// would take a different number of values from the stream, so adding one
    /// `after(..)` to a test would reshuffle every later op.
    pub fn schedule(&mut self, pending: Pending, delay: Option<Duration>, now: Nanos) {
        let drawn = self.step * self.rng.latency_class();
        let after = delay.map_or(drawn, |d| {
            Nanos::try_from(d.as_nanos()).unwrap_or(Nanos::MAX)
        });
        let due = now.saturating_add(after);

        let key = (due, pending.seq);
        let at = self
            .scheduled
            .partition_point(|s| (s.due, s.pending.seq) < key);
        self.scheduled.insert(at, Scheduled { pending, due });
    }

    /// When the next op completes, or `None` if nothing is scheduled.
    pub fn next_due(&self) -> Option<Nanos> {
        self.scheduled.first().map(|s| s.due)
    }

    /// Take everything due at or before `now`, in completion order.
    ///
    /// Not just the earliest: a sleep overshoots, and anything that came due
    /// during the overshoot belongs in the same batch rather than stranded a
    /// full cycle behind.
    pub fn take_due(&mut self, now: Nanos) -> Vec<Pending> {
        let cut = self.scheduled.partition_point(|s| s.due <= now);
        self.scheduled.drain(..cut).map(|s| s.pending).collect()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Pending> {
        self.scheduled.iter().map(|s| &s.pending)
    }

    pub fn contains(&self, user_data: u64) -> bool {
        self.iter().any(|p| p.sqe.user_data == user_data)
    }

    pub fn take_by_user_data(&mut self, user_data: u64) -> Option<Pending> {
        let i = self
            .scheduled
            .iter()
            .position(|s| s.pending.sqe.user_data == user_data)?;
        Some(self.scheduled.remove(i).pending)
    }
}
