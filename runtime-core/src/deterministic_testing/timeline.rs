//! When submitted ops complete.
//!
//! Ops are held in one list ordered by due time, so the front is always the
//! next event. An op the scenario timed gets exactly the delay it asked for.
//! Everything else is due at once, so submission order is completion order
//! unless a test says otherwise — nothing here invents a latency.

use std::time::Duration;

use super::clock::Nanos;
use super::fds::FdKind;
use super::sqe::Sqe;

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

/// Where a pending op's outcome came from.
pub enum Source {
    /// The op's own default. Nothing to give back if it is cancelled.
    Default,
    /// A scenario entry, which asked for this delay. Kept so a cancel can put
    /// the entry back exactly as it was.
    Scenario(Option<Duration>),
}

pub struct Pending {
    pub sqe: Sqe,
    /// Submission order. Breaks ties between ops due at the same instant, and
    /// is the order tests see in `inflight()`.
    pub seq: u64,
    pub outcome: Outcome,
    pub source: Source,
}

struct Scheduled {
    pending: Pending,
    due: Nanos,
}

pub struct Timeline {
    /// Ordered by `(due, seq)`, so the front is the next event. Kept sorted on
    /// insert rather than searched: the front is read on every blocking wait.
    scheduled: Vec<Scheduled>,
}

impl Default for Timeline {
    fn default() -> Self {
        Self {
            scheduled: Vec::new(),
        }
    }
}

impl Timeline {
    /// Enqueue an op, due `now` plus whatever delay the scenario asked for.
    ///
    /// No delay means due immediately: an op nobody timed completes as soon as
    /// the ring is next waited on, so a batch comes back in submission order.
    /// Drawing a latency here would invent a schedule the test did not ask for.
    ///
    /// `now` is passed in rather than read here so a whole submission batch
    /// shares one instant, and `seq` decides the order within it rather than
    /// however long the submit loop took.
    pub fn schedule(&mut self, pending: Pending, delay: Option<Duration>, now: Nanos) {
        let after = delay.map_or(0, |d| Nanos::try_from(d.as_nanos()).unwrap_or(Nanos::MAX));
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
