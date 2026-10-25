//! When submitted ops complete.
//!
//! Ops are held in one list ordered by due time, so the front is always the
//! next event. An op the scenario timed gets exactly the delay it asked for.
//! Everything else is due at once, so submission order is completion order
//! unless a test says otherwise — nothing here invents a latency.

use std::os::fd::RawFd;
use std::time::Duration;

use std::collections::VecDeque;

use super::clock::Nanos;
use super::fds::FdKind;
use super::scenario::{Batch, Flow, Scripted};
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
    /// Take the bytes the op offers — all of them, or the first `n` — record
    /// them against the descriptor, and complete with the count. The mirror of
    /// `Data`, for the write side.
    Accept(Option<u32>),
    /// Write the next chunk of these bytes into the op's buffer, and keep the
    /// rest for the completions after it.
    Flow(Flow),
    /// Asked of the descriptor's actor when the completion is posted.
    Actor,
    /// Already done by the actor, waiting only to be reported: past the point
    /// a cancel can undo.
    Committed(i32),
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
    /// A single scenario entry, which asked for this delay. Kept so a cancel
    /// can put the entry back exactly as it was.
    Scenario(Option<Duration>),
    /// A multishot's declared life, travelling with the op rather than sitting
    /// on the trigger — so two ops reading the same descriptor cannot consume
    /// each other's script.
    Stream {
        /// The rest of the batch the pending completion belongs to — declared
        /// to land in the same drain as it.
        also: Vec<Scripted>,
        /// Batches after the current one.
        rest: VecDeque<Batch>,
        /// The delay the current batch asked for, for putting it back on
        /// cancel.
        delay: Option<Duration>,
    },
}

pub struct Pending {
    pub sqe: Sqe,
    /// Submission order. Breaks ties between ops due at the same instant, and
    /// is the order tests see in `inflight()`.
    pub seq: u64,
    pub outcome: Outcome,
    pub source: Source,
}

/// Something the timeline holds until it falls due.
// Ops are nearly every entry; boxing them would allocate one per op.
#[allow(clippy::large_enum_variant)]
pub enum Item {
    /// An op, completing as its outcome says.
    Op(Pending),
    /// An actor's timer, still current only while its generation is.
    Timer {
        fd: RawFd,
        tag: u64,
        generation: u64,
    },
    /// Ask an actor again for the ops waiting on it.
    Wake { fd: RawFd },
}

struct Scheduled {
    item: Item,
    due: Nanos,
    seq: u64,
}

pub struct Timeline {
    /// Ordered by `(due, seq)`, so the front is the next event. Kept sorted on
    /// insert rather than searched: the front is read on every blocking wait.
    /// Ops and actor events share one order, so nothing at the same instant is
    /// left for chance to decide.
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
        let seq = pending.seq;
        self.insert(Item::Op(pending), now.saturating_add(after), seq);
    }

    /// Enqueue an actor event at `due`, ordered among ops by `seq`.
    pub fn schedule_at(&mut self, item: Item, due: Nanos, seq: u64) {
        self.insert(item, due, seq);
    }

    fn insert(&mut self, item: Item, due: Nanos, seq: u64) {
        let key = (due, seq);
        let at = self.scheduled.partition_point(|s| (s.due, s.seq) < key);
        self.scheduled.insert(at, Scheduled { item, due, seq });
    }

    /// When the next item falls due, or `None` if nothing is scheduled.
    pub fn next_due(&self) -> Option<Nanos> {
        self.scheduled.first().map(|s| s.due)
    }

    /// Take everything due at or before `now`, in completion order.
    ///
    /// Not just the earliest: a sleep overshoots, and anything that came due
    /// during the overshoot belongs in the same batch rather than stranded a
    /// full cycle behind.
    pub fn take_due(&mut self, now: Nanos) -> Vec<Item> {
        let cut = self.scheduled.partition_point(|s| s.due <= now);
        self.scheduled.drain(..cut).map(|s| s.item).collect()
    }

    /// The ops scheduled, in order; actor events are not ops.
    pub fn iter(&self) -> impl Iterator<Item = &Pending> {
        self.scheduled.iter().filter_map(|s| match &s.item {
            Item::Op(pending) => Some(pending),
            _ => None,
        })
    }

    pub fn contains(&self, user_data: u64) -> bool {
        self.iter().any(|p| p.sqe.user_data == user_data)
    }

    pub fn take_by_user_data(&mut self, user_data: u64) -> Option<Pending> {
        let i = self
            .scheduled
            .iter()
            .position(|s| matches!(&s.item, Item::Op(p) if p.sqe.user_data == user_data))?;
        match self.scheduled.remove(i).item {
            Item::Op(pending) => Some(pending),
            _ => unreachable!("matched an op above"),
        }
    }
}
