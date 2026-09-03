//! What the simulated world does, declared before the runtime starts.
//!
//! An op that waits on something external — a recv, an accept — cannot be set
//! up in advance by token, because the runtime picks the token when it submits.
//! So a scenario is keyed by what the op *is*: a read on this descriptor, an
//! accept on that listener. Responses queue per trigger and are consumed in
//! order, so a socket that yields two chunks and then EOF is three entries.
//!
//! Declaring it up front rather than driving the ring live is what keeps a test
//! reproducible. A task that calls into the ring while the runtime runs makes
//! the result depend on when that task happened to be polled, which is exactly
//! the nondeterminism a simulator exists to remove.

use std::collections::{HashMap, VecDeque};
use std::os::fd::RawFd;
use std::time::Duration;

use super::timeline::Outcome;

/// What a scripted response is attached to.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Trigger {
    /// Any read-side op on this descriptor — `RECV`, `READ` or `RECVMSG`. The
    /// test does not have to know which one the runtime chose.
    Read(RawFd),
    /// An accept on this listener.
    Accept(RawFd),
    /// A connect on this socket.
    Connect(RawFd),
    /// Any write-side op on this descriptor — `SEND`, `SEND_ZC`, `SENDMSG`,
    /// `WRITE`, `WRITEV` or `WRITE_FIXED`.
    Write(RawFd),
}

/// What a scripted read does.
///
/// The variants exist because a bare `i32` made one integer carry three
/// unrelated meanings — a byte count, an end of stream, and a negated errno —
/// and left the sign convention to the caller.
pub enum ReadEvent {
    /// These bytes arrive, written into the op's own buffer.
    Data(Vec<u8>),
    /// The peer closed. Reported as `0`, which also ends a multishot.
    Eof,
    /// This many bytes arrived, contents unspecified — zeros.
    ///
    /// For when a test cares about the count and not the payload. The bytes are
    /// still written, so the reader never sees stale contents behind a length
    /// that claims otherwise.
    Bytes(u32),
    /// The read fails with this errno, given positive.
    Error(i32),
}

impl ReadEvent {
    /// [`Data`](Self::Data) without spelling out the conversion.
    pub fn data(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Data(bytes.into())
    }
}

/// What a scripted accept does.
pub enum AcceptEvent {
    /// A connection, on a socket the simulator creates. Its descriptor is only
    /// known once the accept completes.
    Connection,
    /// A connection on a descriptor the test made in advance — the way to
    /// script what that connection then goes on to do.
    On(RawFd),
    /// The accept fails with this errno, given positive. Ends a multishot.
    Error(i32),
}

/// What a scripted connect does.
pub enum ConnectEvent {
    Ok,
    /// Fails with this errno, given positive.
    Error(i32),
}

/// What a scripted write does.
///
/// Nothing declared means [`All`](Self::All), now — a socket with room in its
/// buffer — so a test that never scripts a write sees no change.
pub enum WriteEvent {
    /// Everything the op offers is accepted.
    All,
    /// Only the first `n` bytes are accepted — a short write. Capped at what
    /// the op offered, since a kernel cannot take more than it was given.
    Short(u32),
    /// The write fails with this errno, given positive. Nothing is accepted.
    Error(i32),
}

/// How long an entry is held back.
#[derive(Clone, Copy, Debug)]
pub enum Delay {
    /// Exactly this long.
    Exact(Duration),
    /// Drawn from this range, inclusive, when the op is armed.
    Between(Duration, Duration),
}

/// A scripted response, and optionally when it lands.
pub struct Scripted {
    pub outcome: Outcome,
    /// When it completes, or `None` for as soon as the ring is next waited on.
    pub delay: Option<Delay>,
}

/// SplitMix64: short, good quality, no dependency, and entirely determined by
/// its seed.
///
/// The scenario is the only thing that draws. Nothing else in the simulator
/// decides a schedule, so a test that never asks for a range never meets a
/// random number — and one that does knows exactly where it came from.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

/// Errnos are stored negated in a CQE; taking them positive means a caller
/// cannot forget the sign, and cannot accidentally pass a "successful" error.
fn as_errno(errno: i32) -> i32 {
    -errno.abs()
}

fn scheduled(outcome: Outcome, delay: Delay) -> Scripted {
    Scripted {
        outcome,
        delay: Some(delay),
    }
}

macro_rules! timed {
    ($event:ty) => {
        impl $event {
            /// Hold the completion back by `delay` instead of letting it land
            /// as soon as the ring is next waited on.
            ///
            /// This is how a test states an interleaving — "the read lands
            /// after the write", "this one times out". Nothing else invents a
            /// latency, so an op nobody delays completes in submission order.
            ///
            /// Any duration: the timeline is ordered by due time, so nothing
            /// constrains it to a grid.
            pub fn after(self, delay: Duration) -> Scripted {
                scheduled(self.into(), Delay::Exact(delay))
            }

            /// Hold the completion back by a duration drawn from `min..=max`,
            /// inclusive, when the op is armed.
            ///
            /// The only randomness in the simulator, and only because a test
            /// asked for it here. Which value comes out is settled by the
            /// ring's seed and the order entries are armed in, so a run
            /// repeats.
            pub fn after_between(self, min: Duration, max: Duration) -> Scripted {
                assert!(
                    min <= max,
                    "after_between({min:?}, {max:?}) is an empty range"
                );
                scheduled(self.into(), Delay::Between(min, max))
            }
        }

        impl From<$event> for Scripted {
            fn from(event: $event) -> Self {
                Scripted {
                    outcome: event.into(),
                    delay: None,
                }
            }
        }
    };
}

timed!(ReadEvent);
timed!(AcceptEvent);
timed!(ConnectEvent);
timed!(WriteEvent);

impl From<ReadEvent> for Outcome {
    fn from(event: ReadEvent) -> Self {
        match event {
            ReadEvent::Data(bytes) => Outcome::Data(bytes),
            ReadEvent::Eof => Outcome::result(0),
            ReadEvent::Bytes(n) => Outcome::Data(vec![0; n as usize]),
            ReadEvent::Error(errno) => Outcome::result(as_errno(errno)),
        }
    }
}

impl From<AcceptEvent> for Outcome {
    fn from(event: AcceptEvent) -> Self {
        match event {
            AcceptEvent::Connection => Outcome::NewFd(super::fds::FdKind::Socket),
            AcceptEvent::On(fd) => Outcome::result(fd),
            AcceptEvent::Error(errno) => Outcome::result(as_errno(errno)),
        }
    }
}

impl From<ConnectEvent> for Outcome {
    fn from(event: ConnectEvent) -> Self {
        match event {
            ConnectEvent::Ok => Outcome::result(0),
            ConnectEvent::Error(errno) => Outcome::result(as_errno(errno)),
        }
    }
}

impl From<WriteEvent> for Outcome {
    fn from(event: WriteEvent) -> Self {
        match event {
            WriteEvent::All => Outcome::Accept(None),
            WriteEvent::Short(n) => Outcome::Accept(Some(n)),
            WriteEvent::Error(errno) => Outcome::result(as_errno(errno)),
        }
    }
}

/// Completions declared to land together, as one drain of the completion
/// queue.
///
/// How many arrive at once is not cosmetic: two in one drain is one round of
/// task polling, two in separate drains is two, with the program making
/// progress in between.
pub struct Batch(pub Vec<Scripted>);

impl Batch {
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl<T: Into<Scripted>> From<Vec<T>> for Batch {
    fn from(events: Vec<T>) -> Self {
        Batch(events.into_iter().map(Into::into).collect())
    }
}

impl<T: Into<Scripted>, const N: usize> From<[T; N]> for Batch {
    fn from(events: [T; N]) -> Self {
        Batch(events.into_iter().map(Into::into).collect())
    }
}

impl From<Scripted> for Batch {
    fn from(scripted: Scripted) -> Self {
        Batch(vec![scripted])
    }
}

macro_rules! batch_of_one {
    ($event:ty) => {
        impl From<$event> for Batch {
            fn from(event: $event) -> Self {
                Batch(vec![event.into()])
            }
        }
    };
}

batch_of_one!(ReadEvent);
batch_of_one!(AcceptEvent);
batch_of_one!(ConnectEvent);
batch_of_one!(WriteEvent);

/// One declaration in a scenario.
///
/// A multishot is one op with a lifetime, not a run of unrelated completions,
/// so it is declared as one thing. Kept apart from single completions because
/// the two are told apart at arming time: a stream handed to a single-shot op,
/// or the reverse, is a test describing something other than what it runs.
pub enum Entry {
    /// One completion, for one op.
    Once(Scripted),
    /// Everything one multishot op produces, in order, grouped by what lands
    /// together. A sequence ending on a terminal outcome closes the op; an open
    /// one leaves it armed.
    Stream(VecDeque<Batch>),
}

impl Entry {
    /// Completions still to be delivered.
    pub fn len(&self) -> usize {
        match self {
            Self::Once(_) => 1,
            Self::Stream(rest) => rest.iter().map(Batch::len).sum(),
        }
    }
}

pub struct Scenario {
    queued: HashMap<Trigger, VecDeque<Entry>>,
    rng: SplitMix64,
}

impl Scenario {
    pub fn new(seed: u64) -> Self {
        Self {
            queued: HashMap::new(),
            rng: SplitMix64(seed),
        }
    }

    /// A range becomes a duration here, once per entry.
    ///
    /// Resolved at the single point entries leave the scenario, so the draw
    /// order is the order ops were armed in and nothing else consumes the
    /// stream.
    pub fn resolve(&mut self, delay: Delay) -> Duration {
        match delay {
            Delay::Exact(exact) => exact,
            Delay::Between(min, max) => {
                let span = max.saturating_sub(min).as_nanos() as u64;
                if span == 0 {
                    return min;
                }
                min + Duration::from_nanos(self.rng.next_u64() % (span + 1))
            }
        }
    }
    pub fn push(&mut self, trigger: Trigger, entry: Entry) {
        self.queued.entry(trigger).or_default().push_back(entry);
    }

    /// A range becomes a duration, once per element.
    pub fn resolve_delay(&mut self, delay: Option<Delay>) -> Option<Duration> {
        delay.map(|d| self.resolve(d))
    }

    /// Put an entry back at the head, so it is the next one taken.
    ///
    /// A cancelled op has to give back what it claimed: a kernel does not
    /// discard buffered data because a read was cancelled, and the next read
    /// would see it.
    pub fn unshift(&mut self, trigger: Trigger, entry: Entry) {
        self.queued.entry(trigger).or_default().push_front(entry);
    }

    /// Take the next declaration for `trigger`.
    pub fn take(&mut self, trigger: Trigger) -> Option<Entry> {
        self.queued.get_mut(&trigger)?.pop_front()
    }

    /// Completions declared but not yet claimed by any op. A stream already
    /// taken lives on the op that took it, and is counted there.
    pub fn remaining(&self) -> usize {
        self.queued
            .values()
            .flat_map(|entries| entries.iter())
            .map(Entry::len)
            .sum()
    }
}
