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
use std::time::{Duration, SystemTime};

use super::files::FileMeta;
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
    /// An open. It names no descriptor yet, so opens answer in the order they
    /// are submitted, whatever their paths.
    Open,
    /// A `statx` on this descriptor.
    Stat(RawFd),
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
        /// Stumbles that end the multishot reading the flow, but not the
        /// connection.
        hiccups: Vec<Hiccup>,
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

/// A stumble at a drawn offset inside a [`ReadEvent::Flow`]. It ends the
/// multishot reading the flow but not the connection, so the rest of the flow
/// arrives on the read that re-arms.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hiccup {
    /// The read fails with `EINTR`.
    Interrupted,
    /// A chunk arrives without `IORING_CQE_F_MORE`: the kernel ended the
    /// multishot on its own, as it may when the completion queue overflows.
    Ended,
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
            hiccups: Vec::new(),
        }
    }

    /// The flow this event declares, for the builders below to refine.
    fn as_flow(
        &mut self,
        builder: &str,
    ) -> (&mut Option<Delay>, &mut Option<Cut>, &mut Vec<Hiccup>) {
        match self {
            Self::Flow {
                gap, cut, hiccups, ..
            } => (gap, cut, hiccups),
            _ => panic!("{builder} applies to a flow"),
        }
    }

    /// Hold each later drain of this flow back by a delay drawn from
    /// `min..=max`. Chunks drawn into the same drain still land together.
    pub fn spaced(mut self, min: Duration, max: Duration) -> Self {
        assert!(min <= max, "spaced({min:?}, {max:?}) is an empty range");
        *self.as_flow("spaced").0 = Some(Delay::Between(min, max));
        self
    }

    /// Kill the connection partway through this flow, at an offset drawn from
    /// `0..=len`: zero cuts before the first byte, `len` after the last.
    pub fn cut(mut self, cut: Cut) -> Self {
        *self.as_flow("cut").1 = Some(cut);
        self
    }

    /// Stumble once at an offset drawn from `0..len`; call again for more.
    /// A hiccup past a cut never happens.
    pub fn hiccup(mut self, hiccup: Hiccup) -> Self {
        self.as_flow("hiccup").2.push(hiccup);
        self
    }
}

/// What a flow's next completion is.
pub(crate) enum Step {
    /// The next chunk, carrying `F_MORE` as usual.
    Chunk,
    /// The next chunk, ending the multishot.
    Ended,
    /// This errno instead of data, ending the multishot.
    Fails(i32),
    /// This errno, and the connection is gone: nothing follows.
    Cut(i32),
}

/// The unread part of a [`ReadEvent::Flow`], handing out one chunk per
/// completion.
#[derive(Clone, Debug)]
pub struct Flow {
    bytes: Vec<u8>,
    at: usize,
    gap: Option<Delay>,
    cut: Option<Cut>,
    /// Hiccups whose offsets are still to be drawn.
    hiccups: Vec<Hiccup>,
    /// Drawn by the first completion: where the cut falls, and the hiccups by
    /// offset, nearest first.
    cut_at: Option<usize>,
    stumbles: VecDeque<(usize, Hiccup)>,
    drawn: bool,
}

impl Flow {
    fn new(bytes: Vec<u8>, gap: Option<Delay>, cut: Option<Cut>, hiccups: Vec<Hiccup>) -> Self {
        Self {
            bytes,
            at: 0,
            gap,
            cut,
            hiccups,
            cut_at: None,
            stumbles: VecDeque::new(),
            drawn: false,
        }
    }

    /// Settle where the cut and every hiccup fall, once.
    fn draw(&mut self, scenario: &mut Scenario) {
        if std::mem::replace(&mut self.drawn, true) {
            return;
        }
        let len = self.bytes.len();
        if self.cut.is_some() {
            self.cut_at = Some(scenario.offset(len));
        }
        let mut stumbles: Vec<_> = std::mem::take(&mut self.hiccups)
            .into_iter()
            .map(|hiccup| (scenario.offset(len - 1), hiccup))
            .collect();
        stumbles.sort_by_key(|&(at, _)| at);
        self.stumbles = stumbles.into();
    }

    /// What the next completion is: a stumble due here, the cut, or a chunk.
    pub(crate) fn step(&mut self, scenario: &mut Scenario) -> Step {
        self.draw(scenario);
        // A stumble a chunk ran past, behind another at the same offset.
        while self.stumbles.front().is_some_and(|&(at, _)| at < self.at) {
            self.stumbles.pop_front();
        }
        while let Some(&(at, hiccup)) = self.stumbles.front() {
            if at != self.at {
                break;
            }
            self.stumbles.pop_front();
            match hiccup {
                Hiccup::Interrupted => return Step::Fails(-libc::EINTR),
                // Needs a chunk to end on; at the cut there is none.
                Hiccup::Ended if self.at < self.end() => return Step::Ended,
                Hiccup::Ended => {}
            }
        }
        match self.cut {
            Some(cut) if self.cut_at == Some(self.at) => {
                self.cut = None;
                Step::Cut(-cut.errno())
            }
            _ => Step::Chunk,
        }
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

    /// The next chunk, at most `max` bytes, never past the cut or the next
    /// stumble, its length drawn from `scenario`.
    pub(crate) fn next_chunk(&mut self, max: usize, scenario: &mut Scenario) -> Vec<u8> {
        let stop = match self.stumbles.front() {
            Some(&(at, _)) if at > self.at => at.min(self.end()),
            _ => self.end(),
        };
        let n = scenario.chunk_len(max.min(stop - self.at));
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

/// What a scripted open does.
///
/// Nothing declared means the open succeeds, onto a file the simulator makes
/// up: created empty if the open may create, found empty otherwise.
pub enum OpenEvent {
    /// The file was already there, and is this. The open's own flags still
    /// apply: `O_TRUNC` empties it, `O_APPEND` writes at its end.
    Existing(FileMeta),
    /// The open fails with this errno, given positive.
    Error(i32),
}

impl OpenEvent {
    /// [`Existing`](Self::Existing), for a regular file of `size` bytes.
    pub fn existing(size: u64) -> Self {
        Self::Existing(FileMeta::file(size))
    }
}

/// What a scripted `statx` does.
///
/// Nothing declared means the simulator answers from what it knows of the
/// file: what it was opened as, grown by the writes accepted since.
pub enum StatEvent {
    /// The file is this, whatever the simulator knew of it.
    Meta(FileMeta),
    /// The stat fails with this errno, given positive.
    Error(i32),
}

impl StatEvent {
    /// A regular file of `size` bytes, `0o644`.
    pub fn file(size: u64) -> Self {
        Self::Meta(FileMeta::file(size))
    }

    /// A directory, `0o755`.
    pub fn dir() -> Self {
        Self::Meta(FileMeta::dir())
    }

    fn refine(self, builder: &str, refine: impl FnOnce(FileMeta) -> FileMeta) -> Self {
        match self {
            Self::Meta(meta) => Self::Meta(refine(meta)),
            Self::Error(_) => panic!("{builder} applies to a file"),
        }
    }

    /// The permission bits, `0o600` and the like.
    pub fn permissions(self, bits: u32) -> Self {
        self.refine("permissions", |meta| meta.permissions(bits))
    }

    /// When the content last changed.
    pub fn modified(self, at: SystemTime) -> Self {
        self.refine("modified", |meta| meta.modified(at))
    }
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
pub(crate) struct SplitMix64(u64);

impl SplitMix64 {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
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
timed!(OpenEvent);
timed!(StatEvent);

impl From<ReadEvent> for Outcome {
    fn from(event: ReadEvent) -> Self {
        match event {
            ReadEvent::Data(bytes) => Outcome::Data(bytes),
            ReadEvent::Eof => Outcome::result(0),
            ReadEvent::Bytes(n) => Outcome::Data(vec![0; n as usize]),
            ReadEvent::Error(errno) => Outcome::result(as_errno(errno)),
            ReadEvent::Flow {
                bytes,
                gap,
                cut,
                hiccups,
            } => Outcome::Flow(Flow::new(bytes, gap, cut, hiccups)),
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

impl From<OpenEvent> for Outcome {
    fn from(event: OpenEvent) -> Self {
        match event {
            OpenEvent::Existing(meta) => Outcome::Open(meta),
            OpenEvent::Error(errno) => Outcome::result(as_errno(errno)),
        }
    }
}

impl From<StatEvent> for Outcome {
    fn from(event: StatEvent) -> Self {
        match event {
            StatEvent::Meta(meta) => Outcome::Stat(Some(meta)),
            StatEvent::Error(errno) => Outcome::result(as_errno(errno)),
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

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
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

    /// True with probability `odds`, drawing nothing when it is zero.
    pub(crate) fn chance(&mut self, odds: f64) -> bool {
        if odds <= 0.0 {
            return false;
        }
        let unit = (self.rng.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        unit < odds
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

    /// Whether anything is still declared for `trigger`.
    pub fn declares(&self, trigger: Trigger) -> bool {
        self.queued.get(&trigger).is_some_and(|q| !q.is_empty())
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
