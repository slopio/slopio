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
//! the nondeterminism the seed exists to remove.

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

/// A scripted response, and optionally when it lands.
pub struct Scripted {
    pub outcome: Outcome,
    /// When it completes, or `None` to draw a latency from the seed.
    pub delay: Option<Duration>,
}

/// Errnos are stored negated in a CQE; taking them positive means a caller
/// cannot forget the sign, and cannot accidentally pass a "successful" error.
fn as_errno(errno: i32) -> i32 {
    -errno.abs()
}

fn scheduled(outcome: Outcome, delay: Duration) -> Scripted {
    Scripted {
        outcome,
        delay: Some(delay),
    }
}

macro_rules! timed {
    ($event:ty) => {
        impl $event {
            /// Pin the completion to `delay` after submission, instead of
            /// a latency drawn from the seed.
            ///
            /// Use it to reproduce one specific interleaving — "the read lands
            /// before the write" — rather than to search for one. A pinned op
            /// no longer varies with the seed, so pinning everything turns the
            /// fuzzing off.
            ///
            /// Any duration: the timeline is ordered by due time, so nothing
            /// constrains it to a grid.
            pub fn after(self, delay: Duration) -> Scripted {
                scheduled(self.into(), delay)
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

#[derive(Default)]
pub struct Scenario {
    queued: HashMap<Trigger, VecDeque<Scripted>>,
}

impl Scenario {
    pub fn push(&mut self, trigger: Trigger, scripted: Scripted) {
        self.queued.entry(trigger).or_default().push_back(scripted);
    }

    /// Take the next response for `trigger`, if the scenario has one left.
    pub fn take(&mut self, trigger: Trigger) -> Option<Scripted> {
        self.queued.get_mut(&trigger)?.pop_front()
    }

    /// Responses declared but never claimed. A test that scripted three reads
    /// against code that only performed two should hear about it.
    pub fn remaining(&self) -> usize {
        self.queued.values().map(VecDeque::len).sum()
    }
}
