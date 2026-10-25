//! The far end of a simulated connection, written as code.
//!
//! A script says in advance what every read returns. An [`Actor`] decides as
//! the run goes: the ring asks it what each read receives and what each write
//! does, at the moment that completion is posted, and calls it back when its
//! timers fire. Its state — what it has to send, whether it closed — is its
//! own; the ring only routes events to it and applies its answers.
//!
//! Nothing here runs as a task of the runtime under test. The ring calls an
//! actor at fixed points of its own schedule, so a run stays a function of the
//! seed and the program alone.

use std::time::Duration;

use super::scenario::SplitMix64;

/// The program's peer on one simulated descriptor.
///
/// Every method gets an [`ActorCx`] to arm timers, draw from the seed and read
/// the virtual clock. Whatever it asks for is applied once the call returns.
pub trait Actor {
    /// The connection exists: the accept that hands this descriptor out has
    /// completed, or the program first used it.
    fn on_open(&mut self, cx: &mut ActorCx) {
        let _ = cx;
    }

    /// A read of at most `max` bytes is completing now: what does it get?
    /// [`ReadReply::Pending`] holds it until a later call has something.
    fn read(&mut self, max: usize, cx: &mut ActorCx) -> ReadReply;

    /// The program writes `bytes`: how many does the peer take?
    /// [`WriteReply::Pending`] holds the write, as a full send buffer would.
    fn write(&mut self, bytes: &[u8], cx: &mut ActorCx) -> WriteReply;

    /// The program closed the descriptor. Nothing reaches it after this.
    fn on_closed(&mut self, cx: &mut ActorCx) {
        let _ = cx;
    }

    /// The timer armed under `tag` fired.
    fn on_timer(&mut self, tag: u64, cx: &mut ActorCx) {
        let _ = (tag, cx);
    }
}

/// What a read receives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadReply {
    /// These bytes, at least one and at most the `max` the read was offered.
    Data(Vec<u8>),
    /// The peer closed its side: the read returns zero.
    Eof,
    /// The read fails with this errno, given positive.
    Error(i32),
    /// Nothing yet. The read waits, and is asked again after the actor's next
    /// timer, write or wake-up.
    Pending,
}

/// What a write does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteReply {
    /// The first `n` bytes are taken; fewer than offered is a short write.
    Accepted(usize),
    /// The first `n` bytes are taken now, but the program learns so only after
    /// this long — a send buffer slow to drain. Sends on one descriptor still
    /// complete in order, and one already taken can no longer be cancelled.
    AcceptedAfter(usize, Duration),
    /// The write fails with this errno, given positive.
    Error(i32),
    /// Nothing taken yet. The write waits, and is offered again after the
    /// actor's next timer or wake-up.
    Pending,
}

/// What an actor asked for during one call, applied once it returns.
pub(crate) enum Effect {
    Timer { after: Duration, tag: u64 },
    Cancel { tag: u64 },
    WakeAt(Duration),
}

/// An actor's view of the world for the length of one call.
pub struct ActorCx<'a> {
    pub(crate) now: Duration,
    pub(crate) rng: &'a mut SplitMix64,
    pub(crate) effects: Vec<Effect>,
}

impl ActorCx<'_> {
    /// Virtual time since the ring was built.
    pub fn now(&self) -> Duration {
        self.now
    }

    /// A number from this actor's own stream, settled by the ring's seed.
    pub fn random(&mut self) -> u64 {
        self.rng.next_u64()
    }

    /// Uniform in `lo..=hi`.
    pub fn between(&mut self, lo: u64, hi: u64) -> u64 {
        assert!(lo <= hi, "between({lo}, {hi}) is an empty range");
        lo + self.random() % (hi - lo + 1)
    }

    /// Call [`Actor::on_timer`] with `tag` once `after` has passed. Arming a
    /// tag again replaces the timer it had.
    pub fn timer(&mut self, after: Duration, tag: u64) {
        self.effects.push(Effect::Timer { after, tag });
    }

    /// Disarm the timer under `tag`, if any.
    pub fn cancel(&mut self, tag: u64) {
        self.effects.push(Effect::Cancel { tag });
    }

    /// Ask again, at virtual time `at`, for the reads and writes waiting on
    /// this actor — no callback, for state that becomes ready on its own, such
    /// as bytes in flight on a simulated network.
    pub fn wake_at(&mut self, at: Duration) {
        self.effects.push(Effect::WakeAt(at));
    }
}
