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
    /// These bytes arrive in order, split across as many completions as the
    /// simulator draws, each at most one buffer. Only in a multishot stream.
    Flow {
        bytes: Vec<u8>,
        /// How long each later drain of the flow is held back; `None` lands it
        /// as soon as the ring is next waited on.
        gap: Option<Delay>,
        /// How the connection dies partway through, if it does.
        cut: Option<Cut>,
    },
}

/// How a connection dies in the middle of a [`ReadEvent::Flow`]: the bytes up
/// to a drawn offset arrive, the read after them fails, and the rest is never
/// sent. Either way the connection is reset from then on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cut {
    /// The peer sent RST: `ECONNRESET`.
    Reset,
    /// The peer vanished and retransmission gave up: `ETIMEDOUT`.
    Timeout,
}

impl Cut {
    fn errno(self) -> i32 {
        match self {
            Self::Reset => libc::ECONNRESET,
            Self::Timeout => libc::ETIMEDOUT,
        }
    }
}

impl ReadEvent {
    /// [`Data`](Self::Data) without spelling out the conversion.
    pub fn data(bytes: impl Into<Vec<u8>>) -> Self {
        Self::Data(bytes.into())
    }

    /// [`Flow`](Self::Flow), its later drains landing back to back.
    pub fn flow(bytes: impl Into<Vec<u8>>) -> Self {
        let bytes = bytes.into();
        assert!(!bytes.is_empty(), "an empty flow would read as EOF");
        Self::Flow {
            bytes,
            gap: None,
            cut: None,
        }
    }

    /// Hold each later drain of this flow back by a delay drawn from
    /// `min..=max`. Chunks drawn into the same drain still land together.
    pub fn spaced(self, min: Duration, max: Duration) -> Self {
        assert!(min <= max, "spaced({min:?}, {max:?}) is an empty range");
        match self {
            Self::Flow { bytes, cut, .. } => Self::Flow {
                bytes,
                gap: Some(Delay::Between(min, max)),
                cut,
            },
            _ => panic!("spaced applies to a flow"),
        }
    }

    /// Kill the connection partway through this flow, at an offset drawn from
    /// `0..=len`: zero cuts before the first byte, `len` after the last.
    pub fn cut(self, cut: Cut) -> Self {
        match self {
            Self::Flow { bytes, gap, .. } => Self::Flow {
                bytes,
                gap,
                cut: Some(cut),
            },
            _ => panic!("cut applies to a flow"),
        }
    }
}

/// The unread part of a [`ReadEvent::Flow`], handing out one chunk per
/// completion.
#[derive(Clone, Debug)]
pub struct Flow {
    bytes: Vec<u8>,
    at: usize,
    gap: Option<Delay>,
    cut: Option<Cut>,
    /// Where the cut falls, drawn by the first completion.
    cut_at: Option<usize>,
}

impl Flow {
    fn new(bytes: Vec<u8>, gap: Option<Delay>, cut: Option<Cut>) -> Self {
        Self {
            bytes,
            at: 0,
            gap,
            cut,
            cut_at: None,
        }
    }

    /// The errno the next completion reports instead of data, once the flow
    /// has reached its cut. Draws the cut's offset the first time.
    pub(crate) fn cut_now(&mut self, scenario: &mut Scenario) -> Option<i32> {
        let cut = self.cut?;
        let at = *self
            .cut_at
            .get_or_insert_with(|| scenario.offset(self.bytes.len()));
        (self.at == at).then(|| {
            self.cut = None;
            -cut.errno()
        })
    }

    /// Where delivery stops: the cut, or the end of the bytes.
    fn end(&self) -> usize {
        match (self.cut, self.cut_at) {
            (Some(_), Some(at)) => at,
            _ => self.bytes.len(),
        }
    }

    /// The delay before each later drain, still to be drawn.
    pub(crate) fn gap(&self) -> Option<Delay> {
        self.gap
    }

    /// Nothing left to complete with: every byte delivered and no cut to report.
    pub fn is_empty(&self) -> bool {
        self.cut.is_none() && self.at == self.bytes.len()
    }

    /// The next chunk, at most `max` bytes and never past the cut, its length
    /// drawn from `scenario`.
    pub(crate) fn next_chunk(&mut self, max: usize, scenario: &mut Scenario) -> Vec<u8> {
        let n = scenario.chunk_len(max.min(self.end() - self.at));
        let chunk = self.bytes[self.at..self.at + n].to_vec();
        self.at += n;
        chunk
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
/// The scenario is the only thing that draws, and only for what a test asked to
/// have drawn — a delay range, a flow's split — so a test that asks for
/// neither never meets a random number.
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
            ReadEvent::Flow { bytes, gap, cut } => Outcome::Flow(Flow::new(bytes, gap, cut)),
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

    /// How many bytes a flow's next completion carries, out of at most `max`.
    /// Weighted toward a full buffer and a single byte, where readers slip.
    pub(crate) fn chunk_len(&mut self, max: usize) -> usize {
        match self.rng.next_u64() % 4 {
            0 => max,
            1 => 1,
            _ => 1 + (self.rng.next_u64() % max as u64) as usize,
        }
    }

    /// An offset into `len` bytes, `0..=len` inclusive.
    pub(crate) fn offset(&mut self, len: usize) -> usize {
        (self.rng.next_u64() % (len as u64 + 1)) as usize
    }

    /// Whether the rest of a flow lands in the same drain as its last chunk.
    pub(crate) fn same_drain(&mut self) -> bool {
        self.rng.next_u64() & 1 == 0
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

    /// Drop everything still declared for `trigger`, and say how many
    /// completions that was.
    pub fn void(&mut self, trigger: Trigger) -> usize {
        self.queued
            .remove(&trigger)
            .map_or(0, |entries| entries.iter().map(Entry::len).sum())
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
