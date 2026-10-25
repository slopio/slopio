//! A submission/completion ring that never reaches the kernel.
//!
//! Submissions are decoded into [`Sqe`] and scheduled on a [`Timeline`]. The
//! ring invents nothing: an op completes as soon as the ring is next waited on,
//! unless a scenario entry held it back. Orderings that are rare and
//! unreproducible against a real kernel are therefore things a test states.

use std::cell::{RefCell, RefMut};
use std::collections::{HashMap, VecDeque};
use std::os::fd::RawFd;
use std::rc::Rc;

use io_uring::{opcode, squeue};

use crate::io_driver::BufRingTake;

use super::actor::{Actor, ActorCx, Effect, ReadReply, WriteReply};
use super::clock::{self, Nanos};
use super::fds::{self, FdKind};
use super::scenario::{Batch, Delay, Entry, Flow, Scenario, Scripted, SplitMix64, Step, Trigger};
use super::sqe::{
    IORING_ACCEPT_MULTISHOT, IORING_CQE_BUFFER_SHIFT, IORING_CQE_F_BUFFER, IORING_CQE_F_MORE,
    IORING_RECV_MULTISHOT, Sqe,
};
use super::timeline::{Item, Outcome, Pending, Source, Timeline};
use super::virtual_clock::VirtualClock;

/// Where the simulated ring gets time, and what it does to wait.
///
/// Under `Real` it sleeps for as long as it says, which is what lets the
/// runtime's own timewheel keep working on the same OS clock. Under `Virtual`
/// nothing sleeps: the clock is shared with the timewheel and simply jumped, so
/// a test spanning an hour runs in microseconds.
#[derive(Clone)]
enum Pace {
    Real,
    Virtual(Rc<VirtualClock>),
}

impl Pace {
    fn now(&self) -> Nanos {
        match self {
            Self::Real => clock::now(),
            Self::Virtual(clock) => clock.now_nanos(),
        }
    }

    fn wait_until(&self, deadline: Nanos) {
        match self {
            Self::Real => clock::sleep_until(deadline),
            Self::Virtual(clock) => clock.advance_to_nanos(deadline),
        }
    }
}

/// The `ring_fd` a simulated ring reports. Deliberately invalid, so a stray
/// syscall on it fails instead of touching an unrelated fd.
pub const SIMULATED_RING_FD: RawFd = -1;

/// One completion, as the three fields a CQE carries.
#[derive(Clone, Copy, Debug)]
pub struct Completion {
    pub user_data: u64,
    pub result: i32,
    pub flags: u32,
}

// Matched as constants because the builder types collide: `Recv`/`RecvMulti`/
// `RecvBundle` are all `IORING_OP_RECV`, as are the recvmsg and accept pairs.
const OP_TIMEOUT: u8 = opcode::Timeout::CODE;
const OP_RECV: u8 = opcode::Recv::CODE;
const OP_READ: u8 = opcode::Read::CODE;
const OP_RECVMSG: u8 = opcode::RecvMsg::CODE;
const OP_ACCEPT: u8 = opcode::Accept::CODE;
const OP_CONNECT: u8 = opcode::Connect::CODE;
const OP_WAITID: u8 = opcode::WaitId::CODE;
const OP_SEND: u8 = opcode::Send::CODE;
const OP_SEND_ZC: u8 = opcode::SendZc::CODE;
const OP_SENDMSG: u8 = opcode::SendMsg::CODE;
const OP_SENDMSG_ZC: u8 = opcode::SendMsgZc::CODE;
const OP_WRITE: u8 = opcode::Write::CODE;
const OP_WRITEV: u8 = opcode::Writev::CODE;
const OP_WRITE_FIXED: u8 = opcode::WriteFixed::CODE;
const OP_OPENAT: u8 = opcode::OpenAt::CODE;
const OP_ASYNC_CANCEL: u8 = opcode::AsyncCancel::CODE;
const OP_CLOSE: u8 = opcode::Close::CODE;

enum Class {
    /// The kernel finishes these alone: sends, writes, opens, cancels. A
    /// scenario may still say how — a short write, an `EPIPE` — but silence
    /// means success, not waiting.
    Scheduled,
    /// Blocked on something external. A recv on a socket nobody wrote to must
    /// never complete by itself, so it parks until a test says otherwise.
    Waiting,
    /// The `TIMEOUT` the runtime arms before blocking. Not IO — it bounds how
    /// long the ring may wait, so it is never scheduled.
    Wakeup,
}

fn classify(sqe: &Sqe) -> Class {
    match sqe.opcode {
        OP_TIMEOUT => Class::Wakeup,
        OP_RECV | OP_READ | OP_RECVMSG | OP_ACCEPT | OP_CONNECT | OP_WAITID => Class::Waiting,
        _ => Class::Scheduled,
    }
}

/// Whether this op yields many completions rather than one.
///
/// The multishot builders are invisible from the opcode — `AcceptMulti` and
/// `RecvMulti` share theirs with the single-shot versions and differ only by a
/// bit in `ioprio`.
fn is_multishot(sqe: &Sqe) -> bool {
    match sqe.opcode {
        OP_ACCEPT => sqe.ioprio & IORING_ACCEPT_MULTISHOT != 0,
        OP_RECV | OP_RECVMSG => sqe.ioprio & IORING_RECV_MULTISHOT != 0,
        _ => false,
    }
}

/// A recv that reads without consuming: what it sees stays queued for the
/// next read on the descriptor, as `MSG_PEEK` leaves it in the kernel's queue.
fn is_peek(sqe: &Sqe) -> bool {
    sqe.opcode == OP_RECV && sqe.op_flags & libc::MSG_PEEK as u32 != 0
}

/// Whether this completion is the last one a multishot will produce.
///
/// The rule differs by op, which is why it cannot just be "non-negative keeps
/// going": a zero-length read is EOF and ends the stream, whereas a zero from
/// an accept is a perfectly good descriptor.
fn ends_multishot(sqe: &Sqe, result: i32) -> bool {
    match sqe.opcode {
        OP_RECV | OP_RECVMSG => result <= 0,
        OP_ACCEPT => result < 0,
        _ => true,
    }
}

/// What a scenario entry would have to name to answer this op.
///
/// Public because a test holding an [`Sqe`] from [`FakeRingHandle::inflight`]
/// otherwise has a `pub` opcode byte it cannot interpret. Going through the
/// simulator's own classification is also what keeps a test's idea of "this is
/// a read" from drifting away from the one [`arm`] dispatches on.
pub fn trigger_for(sqe: &Sqe) -> Option<Trigger> {
    match sqe.opcode {
        OP_RECV | OP_READ | OP_RECVMSG => Some(Trigger::Read(sqe.fd)),
        OP_ACCEPT => Some(Trigger::Accept(sqe.fd)),
        OP_CONNECT => Some(Trigger::Connect(sqe.fd)),
        OP_SEND | OP_SEND_ZC | OP_SENDMSG | OP_SENDMSG_ZC | OP_WRITE | OP_WRITEV
        | OP_WRITE_FIXED => Some(Trigger::Write(sqe.fd)),
        _ => None,
    }
}

/// What the far end of a simulated connection has done, as the reads and
/// writes on it see it. A connection with no entry is open.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Peer {
    /// Sent FIN: reads see EOF from here on; writes still go through.
    Closed,
    /// Sent RST, reported by the completion that brought it. From then on
    /// reads see EOF and writes `EPIPE`, as Linux answers a reset socket.
    Reset,
}

/// The state a completion leaves its connection in, if it changes it. Only a
/// simulated socket has a far end; a file read of zero bytes is just its end.
fn peer_after(sqe: &Sqe, result: i32) -> Option<Peer> {
    if fds::kind(sqe.fd) != Some(FdKind::Socket) {
        return None;
    }
    match (trigger_for(sqe)?, -result) {
        (Trigger::Read(_), 0) => Some(Peer::Closed),
        (
            Trigger::Read(_) | Trigger::Write(_),
            libc::ECONNRESET | libc::EPIPE | libc::ETIMEDOUT,
        ) => Some(Peer::Reset),
        _ => None,
    }
}

/// How an op on a closed or reset connection completes, whatever was scripted.
fn peer_outcome(state: &FakeRingState, sqe: &Sqe) -> Option<Outcome> {
    let peer = state.peers.get(&sqe.fd)?;
    match (trigger_for(sqe)?, peer) {
        (Trigger::Read(_), _) => Some(Outcome::result(0)),
        (Trigger::Write(_), Peer::Reset) => Some(Outcome::result(-libc::EPIPE)),
        _ => None,
    }
}

/// Whether the side of a connection `trigger` names has ended, so nothing
/// scripted for it can happen any more.
fn dead(state: &FakeRingState, trigger: Trigger) -> bool {
    match trigger {
        Trigger::Read(fd) => state.peers.contains_key(&fd),
        Trigger::Write(fd) => state.peers.get(&fd) == Some(&Peer::Reset),
        Trigger::Accept(_) | Trigger::Connect(_) => false,
    }
}

/// Scripted completions an op still carries, delivered or not.
fn owed(source: &Source) -> usize {
    match source {
        Source::Default => 0,
        Source::Scenario(_) => 1,
        Source::Stream { also, rest, .. } => {
            1 + also.len() + rest.iter().map(Batch::len).sum::<usize>()
        }
    }
}

/// Move a connection to `peer`. What the script still says about the sides
/// that are now dead is voided, and ops parked on them complete from the new
/// state. Never goes back: a reset connection stays reset.
fn enter(state: &mut FakeRingState, fd: RawFd, peer: Peer, now: Nanos) {
    match (state.peers.get(&fd), peer) {
        (Some(Peer::Reset), _) | (Some(Peer::Closed), Peer::Closed) => return,
        _ => {}
    }
    state.peers.insert(fd, peer);

    state.voided += state.scenario.void(Trigger::Read(fd));
    if peer == Peer::Reset {
        state.voided += state.scenario.void(Trigger::Write(fd));
    }
    let (woken, still): (Vec<Pending>, Vec<Pending>) = std::mem::take(&mut state.waiting)
        .into_iter()
        .partition(|p| p.sqe.fd == fd && peer_outcome(state, &p.sqe).is_some());
    state.waiting = still;
    for pending in woken {
        arm(state, pending, now);
    }
}

/// An actor and what the ring keeps for it.
struct ActorSlot {
    actor: Box<dyn Actor>,
    rng: SplitMix64,
    /// The generation each timer tag is at; a fired timer of an older one was
    /// replaced or cancelled, and does nothing.
    generations: HashMap<u64, u64>,
    opened: bool,
}

impl ActorSlot {
    fn bump(&mut self, tag: u64) -> u64 {
        let generation = self.generations.entry(tag).or_default();
        *generation += 1;
        *generation
    }
}

fn nanos(d: std::time::Duration) -> Nanos {
    Nanos::try_from(d.as_nanos()).unwrap_or(Nanos::MAX)
}

/// Call `fd`'s actor, then apply what it asked for. `None` if `fd` has none.
fn call_actor<R>(
    state: &mut FakeRingState,
    fd: RawFd,
    now: Nanos,
    f: impl FnOnce(&mut dyn Actor, &mut ActorCx) -> R,
) -> Option<R> {
    let since = std::time::Duration::from_nanos(now.saturating_sub(state.start));
    let slot = state.actors.get_mut(&fd)?;
    let mut cx = ActorCx {
        now: since,
        rng: &mut slot.rng,
        effects: Vec::new(),
    };
    let reply = f(slot.actor.as_mut(), &mut cx);
    let effects = cx.effects;
    for effect in effects {
        apply(state, fd, effect, now);
    }
    Some(reply)
}

fn apply(state: &mut FakeRingState, fd: RawFd, effect: Effect, now: Nanos) {
    let Some(slot) = state.actors.get_mut(&fd) else {
        return;
    };
    let (item, due) = match effect {
        Effect::Timer { after, tag } => {
            let generation = slot.bump(tag);
            let item = Item::Timer {
                fd,
                tag,
                generation,
            };
            (item, now.saturating_add(nanos(after)))
        }
        Effect::Cancel { tag } => {
            slot.bump(tag);
            return;
        }
        Effect::WakeAt(at) => (
            Item::Wake { fd },
            state.start.saturating_add(nanos(at)).max(now),
        ),
    };
    let seq = state.next_seq;
    state.next_seq += 1;
    state.timeline.schedule_at(item, due, seq);
}

/// Ask `fd`'s actor again for every op waiting on it.
fn repull(state: &mut FakeRingState, fd: RawFd, now: Nanos) {
    let (woken, still): (Vec<Pending>, Vec<Pending>) = std::mem::take(&mut state.waiting)
        .into_iter()
        .partition(|p| p.sqe.fd == fd && matches!(p.outcome, Outcome::Actor));
    state.waiting = still;
    for pending in woken {
        state.timeline.schedule(pending, None, now);
    }
}

/// Tell `fd`'s actor its connection exists, the first time only.
fn open_actor(state: &mut FakeRingState, fd: RawFd, now: Nanos) {
    let Some(slot) = state.actors.get_mut(&fd) else {
        return;
    };
    if std::mem::replace(&mut slot.opened, true) {
        return;
    }
    call_actor(state, fd, now, |actor, cx| actor.on_open(cx));
    repull(state, fd, now);
}

/// Complete a read or a write on a descriptor with an actor, as the actor
/// answers now.
fn serve_actor(
    state: &mut FakeRingState,
    pending: Pending,
    group: Option<(BufRingTake, u32)>,
    now: Nanos,
) {
    open_actor(state, pending.sqe.fd, now);
    match trigger_for(&pending.sqe) {
        Some(Trigger::Write(_)) => serve_write(state, pending, now),
        Some(Trigger::Read(_)) => serve_read(state, pending, group, now),
        _ => unreachable!("only reads and writes are routed to an actor"),
    }
}

fn serve_write(state: &mut FakeRingState, pending: Pending, now: Nanos) {
    let sqe = pending.sqe;
    let bytes = offered(&sqe);
    let reply = call_actor(state, sqe.fd, now, |actor, cx| actor.write(&bytes, cx))
        .expect("routed here because the fd has an actor");
    let (result, after) = match reply {
        WriteReply::Pending => {
            state.waiting.push(pending);
            return;
        }
        WriteReply::Accepted(n) | WriteReply::AcceptedAfter(n, _) => {
            assert!(
                n <= bytes.len(),
                "the actor on fd {} took {n} of {} bytes offered",
                sqe.fd,
                bytes.len(),
            );
            state
                .written
                .entry(sqe.fd)
                .or_default()
                .push(bytes[..n].to_vec());
            let after = match reply {
                WriteReply::AcceptedAfter(_, after) => nanos(after),
                _ => 0,
            };
            (n as i32, after)
        }
        WriteReply::Error(errno) => (-errno.abs(), 0),
    };
    // Sends on one socket complete in order: none overtakes one still held.
    let due = now
        .saturating_add(after)
        .max(state.writes_due.get(&sqe.fd).copied().unwrap_or(0));
    if due > now {
        state.writes_due.insert(sqe.fd, due);
        let seq = pending.seq;
        let committed = Pending {
            outcome: Outcome::Committed(result),
            source: Source::Default,
            ..pending
        };
        state.timeline.schedule_at(Item::Op(committed), due, seq);
    } else {
        state.cq.push_back(Completion {
            user_data: sqe.user_data,
            result,
            flags: 0,
        });
    }
    // What the program wrote may be what the actor waited for to answer.
    repull(state, sqe.fd, now);
}

fn serve_read(
    state: &mut FakeRingState,
    pending: Pending,
    group: Option<(BufRingTake, u32)>,
    now: Nanos,
) {
    let sqe = pending.sqe;
    let fd = sqe.fd;
    let multishot = is_multishot(&sqe);
    let peek = is_peek(&sqe) && !multishot;
    let max = match group {
        Some((_, capacity)) if sqe.uses_buffer_select() => capacity as usize,
        _ => sqe.len as usize,
    };
    loop {
        // Bytes the actor handed over that no buffer could take come first.
        let reply = match state.unread.get_mut(&fd).filter(|u| !u.is_empty()) {
            Some(unread) => ReadReply::Data(unread.drain(..max.min(unread.len())).collect()),
            None => call_actor(state, fd, now, |actor, cx| actor.read(max, cx))
                .expect("routed here because the fd has an actor"),
        };
        let completion = match reply {
            ReadReply::Pending => {
                state.waiting.push(pending);
                return;
            }
            ReadReply::Eof => Completion {
                user_data: sqe.user_data,
                result: 0,
                flags: 0,
            },
            ReadReply::Error(errno) => Completion {
                user_data: sqe.user_data,
                result: -errno.abs(),
                flags: 0,
            },
            ReadReply::Data(bytes) => {
                assert!(
                    !bytes.is_empty() && bytes.len() <= max,
                    "the actor on fd {fd} answered a read of at most {max} bytes with {}",
                    bytes.len(),
                );
                // A peek leaves the bytes where they were: not delivered, so not recorded.
                let mut seen = HashMap::new();
                let record = if peek { &mut seen } else { &mut state.read };
                let provider = group.map(|(take, _)| take);
                match deliver(sqe, provider, record, |_| bytes.clone()) {
                    None => {
                        // The socket keeps what no buffer could take.
                        state.unread.entry(fd).or_default().splice(0..0, bytes);
                        dry(sqe)
                    }
                    Some(mut completion) => {
                        if peek {
                            state.unread.entry(fd).or_default().splice(0..0, bytes);
                        }
                        if multishot {
                            completion.flags |= IORING_CQE_F_MORE;
                            state.cq.push_back(completion);
                            if state.scenario.same_drain() {
                                continue;
                            }
                            state.timeline.schedule(pending, None, now);
                            return;
                        }
                        completion
                    }
                }
            }
        };
        state.cq.push_back(completion);
        return;
    }
}

/// Put an op where it belongs: on the timeline if the scenario already says
/// what happens to it, parked otherwise — unless it is one the kernel finishes
/// alone, which then completes with its default.
fn arm(state: &mut FakeRingState, mut pending: Pending, now: Nanos) {
    // A descriptor with an actor answers through it, when the op completes.
    if let Some(Trigger::Read(fd) | Trigger::Write(fd)) = trigger_for(&pending.sqe)
        && state.actors.contains_key(&fd)
    {
        pending.outcome = Outcome::Actor;
        state.timeline.schedule(pending, None, now);
        return;
    }
    if let Some(outcome) = peer_outcome(state, &pending.sqe) {
        pending.outcome = outcome;
        state.timeline.schedule(pending, None, now);
        return;
    }
    let entry = trigger_for(&pending.sqe).and_then(|t| state.scenario.take(t));
    let multishot = is_multishot(&pending.sqe);

    let (outcome, delay, source) = match entry {
        None => {
            match classify(&pending.sqe) {
                Class::Waiting => state.waiting.push(pending),
                _ => state.timeline.schedule(pending, None, now),
            }
            return;
        }
        Some(Entry::Once(scripted)) => {
            assert!(
                !multishot,
                "user_data {:#x} is a multishot, but the scenario declared a single \
                 completion for it — use on_multishot_* to describe its whole life",
                pending.sqe.user_data,
            );
            assert!(
                !matches!(scripted.outcome, Outcome::Flow(_)),
                "user_data {:#x}: a flow is split across a multishot's completions — \
                 declare it with on_multishot_read",
                pending.sqe.user_data,
            );
            let delay = state.scenario.resolve_delay(scripted.delay);
            (scripted.outcome, delay, Source::Scenario(delay))
        }
        Some(Entry::Stream(mut rest)) => {
            assert!(
                multishot,
                "user_data {:#x} is a single-shot op, but the scenario declared a stream \
                 for it — a stream describes one multishot's whole life",
                pending.sqe.user_data,
            );
            let Some(first) = next_batch(&mut rest) else {
                // An empty stream declares an op that stays armed and yields
                // nothing, which is the same as saying nothing.
                state.waiting.push(pending);
                return;
            };
            let (head, also) = split_batch(first);
            let delay = state.scenario.resolve_delay(head.delay);
            (head.outcome, delay, Source::Stream { also, rest, delay })
        }
    };

    pending.outcome = outcome;
    pending.source = source;
    state.timeline.schedule(pending, delay, now);
}

/// Skip past any batch a test left empty.
fn next_batch(rest: &mut VecDeque<Batch>) -> Option<Batch> {
    while let Some(batch) = rest.pop_front() {
        if !batch.0.is_empty() {
            return Some(batch);
        }
    }
    None
}

/// A batch as its first completion and whatever lands alongside it.
fn split_batch(batch: Batch) -> (Scripted, Vec<Scripted>) {
    let mut elements = batch.0;
    let head = elements.remove(0);
    (head, elements)
}

/// Continue a multishot from its own declared sequence, or park it once the
/// sequence runs out.
fn advance_stream(
    state: &mut FakeRingState,
    sqe: Sqe,
    seq: u64,
    mut rest: VecDeque<Batch>,
    now: Nanos,
) {
    let Some(batch) = next_batch(&mut rest) else {
        state.waiting.push(Pending {
            sqe,
            seq,
            outcome: default_outcome(&sqe),
            source: Source::Default,
        });
        return;
    };
    let (head, also) = split_batch(batch);
    let delay = state.scenario.resolve_delay(head.delay);
    state.timeline.schedule(
        Pending {
            sqe,
            seq,
            outcome: head.outcome,
            source: Source::Stream { also, rest, delay },
        },
        delay,
        now,
    );
}

fn default_outcome(sqe: &Sqe) -> Outcome {
    match sqe.opcode {
        OP_OPENAT => Outcome::NewFd(FdKind::File),
        OP_SEND | OP_SEND_ZC | OP_SENDMSG | OP_SENDMSG_ZC | OP_WRITE | OP_WRITEV
        | OP_WRITE_FIXED => Outcome::Accept(None),
        _ => Outcome::result(0),
    }
}

/// The bytes a write-side op offers, gathered from wherever its opcode keeps
/// them: a flat buffer, an iovec array, or the iovecs behind a `msghdr`.
fn offered(sqe: &Sqe) -> Vec<u8> {
    // SAFETY: `addr`/`len` describe memory the submitting future keeps alive
    // until its op completes — the contract the kernel reads it under. The op
    // is being retired now, so nothing has freed it yet.
    unsafe {
        match sqe.opcode {
            OP_SEND | OP_SEND_ZC | OP_WRITE | OP_WRITE_FIXED => {
                std::slice::from_raw_parts(sqe.addr as *const u8, sqe.len as usize).to_vec()
            }
            OP_WRITEV => gather(sqe.addr as *const libc::iovec, sqe.len as usize),
            OP_SENDMSG | OP_SENDMSG_ZC => {
                let hdr = &*(sqe.addr as *const libc::msghdr);
                gather(hdr.msg_iov, hdr.msg_iovlen)
            }
            other => unreachable!("opcode {other} offers no bytes"),
        }
    }
}

/// Concatenate an iovec array, as a `writev` would send it.
unsafe fn gather(iov: *const libc::iovec, count: usize) -> Vec<u8> {
    // SAFETY: the array and every buffer it names are kept alive by the
    // submitting future until completion, same as a flat buffer.
    unsafe {
        std::slice::from_raw_parts(iov, count)
            .iter()
            .flat_map(|v| std::slice::from_raw_parts(v.iov_base as *const u8, v.iov_len))
            .copied()
            .collect()
    }
}

/// Retire an op: deliver its outcome and build the CQE. A CQE carries no
/// payload, so data goes into the op's buffer and only the count is reported.
/// Data each way is recorded, in `read` and `written`.
///
/// Also returns what a flow has left unread, for the completions after this
/// one, and whether the flow ends the op here whatever the result.
fn complete_outcome(
    sqe: Sqe,
    outcome: Outcome,
    provider: Option<BufRingTake>,
    read: &mut HashMap<RawFd, Vec<Vec<u8>>>,
    written: &mut HashMap<RawFd, Vec<Vec<u8>>>,
    scenario: &mut Scenario,
) -> (Completion, Option<Flow>, bool) {
    let completion = match outcome {
        Outcome::Result { result, flags } => Completion {
            user_data: sqe.user_data,
            result,
            flags,
        },
        Outcome::Accept(cap) => {
            let mut bytes = offered(&sqe);
            if let Some(n) = cap {
                bytes.truncate(n as usize);
            }
            let n = bytes.len() as i32;
            written.entry(sqe.fd).or_default().push(bytes);
            Completion {
                user_data: sqe.user_data,
                result: n,
                flags: 0,
            }
        }
        Outcome::NewFd(kind) => Completion {
            user_data: sqe.user_data,
            result: fds::mint(kind),
            flags: 0,
        },
        Outcome::Data(bytes) => deliver(sqe, provider, read, |_| bytes).unwrap_or_else(|| dry(sqe)),
        Outcome::Actor => unreachable!("an actor's op is served by serve_actor"),
        Outcome::Committed(result) => Completion {
            user_data: sqe.user_data,
            result,
            flags: 0,
        },
        Outcome::Flow(mut flow) => {
            let fails = |result| Completion {
                user_data: sqe.user_data,
                result,
                flags: 0,
            };
            let (completion, ends) = match flow.step(scenario) {
                // The connection dies here: no buffer is taken, nothing more follows.
                Step::Cut(result) => return (fails(result), None, false),
                Step::Fails(result) => (fails(result), false),
                // A dry ring takes nothing, so the whole flow is still unread.
                step @ (Step::Chunk | Step::Ended) => (
                    deliver(sqe, provider, read, |max| flow.next_chunk(max, scenario))
                        .unwrap_or_else(|| dry(sqe)),
                    matches!(step, Step::Ended),
                ),
            };
            return (completion, (!flow.is_empty()).then_some(flow), ends);
        }
    };
    (completion, None, false)
}

/// The `-ENOBUFS` a ring with every buffer handed out reports.
fn dry(sqe: Sqe) -> Completion {
    Completion {
        user_data: sqe.user_data,
        result: -libc::ENOBUFS,
        flags: 0,
    }
}

/// Write the bytes `chunk` picks into the op's buffer, record them against the
/// descriptor, and complete with their count. `chunk` is told the buffer's
/// capacity; `None` if the op draws from a ring with no buffer left.
fn deliver(
    sqe: Sqe,
    provider: Option<BufRingTake>,
    read: &mut HashMap<RawFd, Vec<Vec<u8>>>,
    chunk: impl FnOnce(usize) -> Vec<u8>,
) -> Option<Completion> {
    if sqe.uses_buffer_select() {
        let take = provider.unwrap_or_else(|| {
            panic!(
                "user_data {:#x} draws from buffer group {}, which no registered ring \
                 provides — was the spec passed to the entry point?",
                sqe.user_data, sqe.buf_group,
            )
        });
        // Every buffer handed out and none recycled. A real ring says the same,
        // and a leaked `BorrowedBuffer` is how a test gets here.
        let (bid, ptr, capacity) = take()?;
        let bytes = chunk(capacity as usize);

        assert!(
            bytes.len() <= capacity as usize,
            "user_data {:#x}: {} bytes declared for one completion, into a {}-byte \
             buffer from group {}. A completion cannot exceed its buffer — declare \
             the split, since how the payload divides is what the reader sees.",
            sqe.user_data,
            bytes.len(),
            capacity,
            sqe.buf_group,
        );

        let n = bytes.len();
        if n > 0 {
            read.entry(sqe.fd).or_default().push(bytes.clone());
            // SAFETY: the pointer comes from the ring's own entry for
            // `bid`, so it addresses that buffer, and the write is capped
            // at the buffer size the ring was built with.
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.as_ptr(), n);
            }
        }
        Some(Completion {
            user_data: sqe.user_data,
            result: n as i32,
            // Which buffer the data landed in. The driver reads it back
            // with `cqueue::buffer_select`.
            flags: IORING_CQE_F_BUFFER | ((bid as u32) << IORING_CQE_BUFFER_SHIFT),
        })
    } else {
        assert!(
            sqe.addr != 0,
            "delivering data to user_data {:#x}, which has no buffer",
            sqe.user_data,
        );
        let bytes = chunk(sqe.len as usize);

        assert!(
            bytes.len() <= sqe.len as usize,
            "user_data {:#x}: {} bytes declared for one completion, into a {}-byte \
             buffer. A completion cannot exceed its buffer — declare the split, \
             since how the payload divides is what the reader sees.",
            sqe.user_data,
            bytes.len(),
            sqe.len,
        );

        // Fewer bytes than the buffer holds is an ordinary short read.
        let n = bytes.len();
        if n > 0 {
            read.entry(sqe.fd).or_default().push(bytes.clone());
            // SAFETY: `addr`/`len` describe a buffer the submitting future
            // keeps alive until its op completes — the same contract that
            // lets the kernel write there. The op is being retired now, the
            // write is capped at the declared length, and source and
            // destination are distinct allocations.
            unsafe {
                std::ptr::copy_nonoverlapping(bytes.as_ptr(), sqe.addr as *mut u8, n);
            }
        }
        Some(Completion {
            user_data: sqe.user_data,
            result: n as i32,
            flags: 0,
        })
    }
}

/// A flow's unread bytes and whatever was declared after them in the same
/// drain, as the batch that now lands next, `delay` after this one.
fn carry(flow: Option<Flow>, batch: &mut VecDeque<Outcome>, delay: Option<Delay>) -> Batch {
    let mut carried: Vec<Scripted> = flow
        .map(Outcome::Flow)
        .into_iter()
        .chain(batch.drain(..))
        .map(|outcome| Scripted {
            outcome,
            delay: None,
        })
        .collect();
    if let Some(head) = carried.first_mut() {
        head.delay = delay;
    }
    Batch(carried)
}

/// `io_uring::types::Timespec` is `repr(transparent)` over this but exposes no
/// accessors, so reading a `TIMEOUT`'s duration means restating the layout.

struct Wakeup {
    deadline: Nanos,
    user_data: u64,
}

/// What a cancelled op owes the scenario back.
///
/// Delays come back already drawn — one element, one draw — so a retry after a
/// cancel lands where the first attempt would have.
fn give_back(source: Source, outcome: Outcome) -> Option<Entry> {
    match source {
        Source::Default => None,
        Source::Scenario(delay) => Some(Entry::Once(Scripted {
            outcome,
            delay: delay.map(Delay::Exact),
        })),
        Source::Stream {
            mut also,
            mut rest,
            delay,
        } => {
            // Nothing in this batch was delivered, so the whole of it heads the
            // sequence again.
            also.insert(
                0,
                Scripted {
                    outcome,
                    delay: delay.map(Delay::Exact),
                },
            );
            rest.push_front(Batch(also));
            Some(Entry::Stream(rest))
        }
    }
}

/// Retire the op an `ASYNC_CANCEL` names, and report what the cancel itself
/// should complete with.
///
/// The target's CQE carries no `F_MORE`, which is what tells the driver a
/// multishot is finally done. Both are queued here, target first, matching the
/// order a real ring reports them in.
fn apply_cancel(state: &mut FakeRingState, target: u64, now: Nanos) -> i32 {
    // Done already, only not reported yet: it completes as it would have.
    if state
        .timeline
        .iter()
        .any(|p| p.sqe.user_data == target && matches!(p.outcome, Outcome::Committed(_)))
    {
        return -libc::EALREADY;
    }
    let cancelled = state.timeline.take_by_user_data(target).or_else(|| {
        let i = state
            .waiting
            .iter()
            .position(|p| p.sqe.user_data == target)?;
        Some(state.waiting.remove(i))
    });
    let Some(pending) = cancelled else {
        // Already gone. A cancel racing its target's own completion gets this
        // from a real kernel too — including when both land in one batch here.
        return -libc::ENOENT;
    };

    // Give the scenario entry back. A kernel does not discard buffered data
    // because a read was cancelled — the next read on that socket still sees
    // it — so neither does this. Without it a cancel silently eats what was
    // scripted, and `scenario_remaining` reports it as delivered.
    if let Some(trigger) = trigger_for(&pending.sqe)
        && let Some(entry) = give_back(pending.source, pending.outcome)
    {
        restore(state, trigger, entry, now);
    }

    state.cq.push_back(Completion {
        user_data: target,
        result: -libc::ECANCELED,
        flags: 0,
    });
    0
}

/// Return an entry to the head of its trigger's queue and serve it to any op
/// already parked on that trigger.
///
/// Nothing else revisits `waiting`, so without the second step the entry would
/// sit there while a read that wants it waits forever — which is exactly the
/// shape of a read that timed out and was retried.
fn restore(state: &mut FakeRingState, trigger: Trigger, entry: Entry, now: Nanos) {
    // A cancel after the connection ended gives back what can no longer arrive.
    if dead(state, trigger) {
        state.voided += entry.len();
        return;
    }
    state.scenario.unshift(trigger, entry);
    if let Some(i) = state
        .waiting
        .iter()
        .position(|p| trigger_for(&p.sqe) == Some(trigger))
    {
        let parked = state.waiting.remove(i);
        arm(state, parked, now);
    }
}

struct FakeRingState {
    /// Pushed by the driver, not yet flushed by a `submit`.
    staged: Vec<Sqe>,
    timeline: Timeline,
    /// Ops blocked on an event the simulation hasn't produced.
    waiting: Vec<Pending>,
    wakeup: Option<Wakeup>,
    /// Declared before the run; consulted as ops arrive.
    scenario: Scenario,
    cq: VecDeque<Completion>,
    next_seq: u64,
    /// Descriptors handed to the runtime, in the order they were minted.
    opened: Vec<RawFd>,
    /// Connections whose far end closed or reset.
    peers: HashMap<RawFd, Peer>,
    /// Scripted completions dropped because their connection ended first.
    voided: usize,
    /// What the simulation delivered to reads, per descriptor, one entry per
    /// completion in completion order.
    read: HashMap<RawFd, Vec<Vec<u8>>>,
    /// What the runtime wrote and the simulation accepted, per descriptor, one
    /// entry per completed op in completion order.
    written: HashMap<RawFd, Vec<Vec<u8>>>,
    /// Registered buffer rings, by group id, with the size of their buffers.
    buf_rings: Vec<(u16, BufRingTake, u32)>,
    /// Descriptors whose far end is an actor.
    actors: HashMap<RawFd, ActorSlot>,
    /// Bytes an actor handed over for a read that no buffer could take, served
    /// before asking it again.
    unread: HashMap<RawFd, Vec<u8>>,
    /// When the last held send on each actor descriptor completes, so the next
    /// one does not overtake it.
    writes_due: HashMap<RawFd, Nanos>,
    /// The builder's seed, from which each actor's stream is drawn.
    seed: u64,
    /// When the ring was built, which actors count time from.
    start: Nanos,
    pace: Pace,
    /// When the ring was last waited on, and how many waits in a row found
    /// time standing there. Virtual time only moves when the program blocks.
    stalled_at: Nanos,
    stalled_waits: u32,
    /// How long the next `TIMEOUT` sqe is for, handed over by the driver.
    ///
    /// A real ring reads that from the `Timespec` the sqe points at, and this
    /// used to do the same — which is sound across a syscall boundary and is
    /// undefined behaviour inside one process. The sqe carries the timespec as
    /// an integer, so reading it back is an integer-to-pointer round trip whose
    /// provenance was never exposed; and it could not be exposed usefully,
    /// because every path from `push_wakeup_timeout` to `submit` takes
    /// `&mut Ring`, which invalidates any shared tag on the field the timespec
    /// lives in. Miri says so at the first armed timer.
    ///
    /// Taking the value instead costs the simulation nothing: one producer
    /// arms these and one consumer reads them, and the address was only ever
    /// standing in for a boundary that is not there.
    wakeup_nanos: Nanos,
}

/// A test's handle on a [`FakeRing`]: what the runtime submitted, and what
/// comes back. Cloneable and shared — the test keeps one, the driver the other.
#[derive(Clone)]
pub struct FakeRingHandle(Rc<RefCell<FakeRingState>>);

impl FakeRingHandle {
    /// Every op submitted and not yet completed, in submission order.
    pub fn inflight(&self) -> Vec<Sqe> {
        let state = self.0.borrow();
        let mut all: Vec<&Pending> = state.timeline.iter().chain(state.waiting.iter()).collect();
        all.sort_unstable_by_key(|p| p.seq);
        all.into_iter().map(|p| p.sqe).collect()
    }

    /// The oldest in-flight op, if any.
    pub fn next_inflight(&self) -> Option<Sqe> {
        self.inflight().first().copied()
    }

    /// In-flight ops matching `pred`, oldest first.
    pub fn inflight_matching(&self, pred: impl Fn(&Sqe) -> bool) -> Vec<Sqe> {
        self.inflight().into_iter().filter(|s| pred(s)).collect()
    }

    /// Complete an op now, ahead of the tick it was scheduled for.
    ///
    /// Panics if it is not in flight: completing an unknown or already-retired
    /// token would hand the driver a slab index it dereferences unchecked, so
    /// the mistake is caught here rather than as a crash somewhere unrelated.
    pub fn complete(&self, user_data: u64, result: i32, flags: u32) {
        let mut state = self.0.borrow_mut();
        let found = state.timeline.take_by_user_data(user_data).is_some() || {
            match state
                .waiting
                .iter()
                .position(|p| p.sqe.user_data == user_data)
            {
                Some(i) => {
                    state.waiting.remove(i);
                    true
                }
                None => false,
            }
        };
        assert!(
            found,
            "completing user_data {user_data:#x}, which is not in flight",
        );
        state.cq.push_back(Completion {
            user_data,
            result,
            flags,
        });
    }

    /// Complete an op that stays in flight — a multishot yielding one of many
    /// results, or a zero-copy send still awaiting its notification. `flags`
    /// should carry `IORING_CQE_F_MORE`.
    pub fn complete_keeping(&self, user_data: u64, result: i32, flags: u32) {
        let mut state = self.0.borrow_mut();
        assert!(
            state.timeline.contains(user_data)
                || state.waiting.iter().any(|p| p.sqe.user_data == user_data),
            "completing user_data {user_data:#x}, which is not in flight",
        );
        state.cq.push_back(Completion {
            user_data,
            result,
            flags,
        });
    }

    /// Schedule a parked op, as though the event it waited for just happened.
    /// It still serves a latency rather than completing on the spot.
    pub fn wake(&self, user_data: u64, result: i32) {
        self.schedule_parked(user_data, Outcome::Result { result, flags: 0 });
    }

    /// Schedule a parked `ACCEPT`, as though a connection arrived. It completes
    /// with a simulated, unconnected TCP socket.
    pub fn accept(&self, user_data: u64) {
        self.schedule_parked(user_data, Outcome::NewFd(FdKind::Socket));
    }

    /// Deliver bytes to a parked read-side op. They are copied into its buffer
    /// when the completion is posted, not now — that is where the kernel would
    /// have written them. A delivery longer than the buffer is a short read.
    ///
    /// Panics if the op draws from a provided-buffer ring.
    pub fn deliver(&self, user_data: u64, data: &[u8]) {
        self.schedule_parked(user_data, Outcome::Data(data.to_vec()));
    }

    fn schedule_parked(&self, user_data: u64, outcome: Outcome) {
        let mut state = self.0.borrow_mut();
        let i = state
            .waiting
            .iter()
            .position(|p| p.sqe.user_data == user_data)
            .unwrap_or_else(|| panic!("waking user_data {user_data:#x}, which is not parked"));
        let mut pending = state.waiting.remove(i);
        pending.outcome = outcome;
        let now = state.pace.now();
        state.timeline.schedule(pending, None, now);
    }

    /// Completions queued but not yet drained by the driver.
    pub fn queued_completions(&self) -> usize {
        self.0.borrow().cq.len()
    }

    /// Ops parked with no event to complete them.
    pub fn parked(&self) -> usize {
        self.0.borrow().waiting.len()
    }

    /// Queue what the next read on `fd` does. Call once per read the code under
    /// test will perform; they are consumed in order.
    ///
    /// Declared before the runtime starts. The op is scheduled the moment it is
    /// submitted, so nothing has to touch the ring while it runs — which is
    /// what keeps the interleaving a property of the scenario alone.
    pub fn on_read(&self, fd: RawFd, event: impl Into<Scripted>) {
        self.script(Trigger::Read(fd), event.into());
    }

    /// Queue what the next accept on `fd` does.
    pub fn on_accept(&self, fd: RawFd, event: impl Into<Scripted>) {
        self.script(Trigger::Accept(fd), event.into());
    }

    /// Queue what the next connect on `fd` does.
    pub fn on_connect(&self, fd: RawFd, event: impl Into<Scripted>) {
        self.script(Trigger::Connect(fd), event.into());
    }

    /// Queue what the next write-side op on `fd` does — `SEND`, `WRITE`,
    /// `WRITEV`, … alike. A write nobody scripted is accepted whole.
    pub fn on_write(&self, fd: RawFd, event: impl Into<Scripted>) {
        self.script(Trigger::Write(fd), event.into());
    }

    /// Queue everything one multishot read on `fd` produces, in order.
    ///
    /// A stream describes one op's whole life rather than a run of unrelated
    /// completions, so two readers of the same descriptor cannot consume each
    /// other's script. Ending on a terminal outcome — `Eof`, an error — closes
    /// the op; an open sequence leaves it armed and waiting.
    pub fn on_multishot_read<B: Into<Batch>>(
        &self,
        fd: RawFd,
        batches: impl IntoIterator<Item = B>,
    ) {
        self.stream(Trigger::Read(fd), batches);
    }

    /// Queue everything one multishot accept on `fd` produces, in order.
    pub fn on_multishot_accept<B: Into<Batch>>(
        &self,
        fd: RawFd,
        batches: impl IntoIterator<Item = B>,
    ) {
        self.stream(Trigger::Accept(fd), batches);
    }

    fn script(&self, trigger: Trigger, scripted: Scripted) {
        self.refuse_actor(trigger);
        self.0
            .borrow_mut()
            .scenario
            .push(trigger, Entry::Once(scripted));
    }

    fn stream<B: Into<Batch>>(&self, trigger: Trigger, batches: impl IntoIterator<Item = B>) {
        self.refuse_actor(trigger);
        let sequence = batches.into_iter().map(Into::into).collect();
        self.0
            .borrow_mut()
            .scenario
            .push(trigger, Entry::Stream(sequence));
    }

    fn refuse_actor(&self, trigger: Trigger) {
        if let Trigger::Read(fd) | Trigger::Write(fd) = trigger {
            assert!(
                !self.0.borrow().actors.contains_key(&fd),
                "fd {fd} has an actor, so its reads and writes are the actor's to answer",
            );
        }
    }

    /// Make `actor` the far end of `fd`: every read and write on it is asked
    /// of the actor as it completes, instead of the script.
    ///
    /// Its [`on_open`](Actor::on_open) runs when an accept hands `fd` out, or
    /// at the first op on it otherwise.
    pub fn attach(&self, fd: RawFd, actor: impl Actor + 'static) {
        let mut state = self.0.borrow_mut();
        assert!(
            !state.scenario.declares(Trigger::Read(fd))
                && !state.scenario.declares(Trigger::Write(fd)),
            "fd {fd} already has reads or writes scripted; an actor answers them all",
        );
        // Each actor its own stream, so one drawing more leaves the others alone.
        let rng = SplitMix64::new(
            SplitMix64::new(state.seed ^ (fd as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
                .next_u64(),
        );
        let slot = ActorSlot {
            actor: Box::new(actor),
            rng,
            generations: HashMap::new(),
            opened: false,
        };
        let replaced = state.actors.insert(fd, slot);
        assert!(replaced.is_none(), "fd {fd} already has an actor");
    }

    /// Scripted completions never delivered — three reads declared against code
    /// that performed two.
    ///
    /// Counts what a stream still owes as well: once taken, its remainder
    /// travels with the op rather than sitting in the scenario.
    pub fn scenario_remaining(&self) -> usize {
        let state = self.0.borrow();
        let undelivered: usize = state
            .timeline
            .iter()
            .chain(state.waiting.iter())
            .map(|p| owed(&p.source))
            .sum();
        state.scenario.remaining() + undelivered
    }

    /// Install a provided-buffer ring under `bgid`.
    ///
    /// Called from a spec's `init` when the runtime starts on a simulated ring.
    /// Without it the simulator has no way to reach the buffers a
    /// `BUFFER_SELECT` op draws from.
    pub fn register_buf_ring(&self, bgid: u16, take: BufRingTake, capacity: u32) {
        self.0.borrow_mut().buf_rings.push((bgid, take, capacity));
    }

    /// The clock this ring advances, if it was built with
    /// [`virtual_time`](FakeRingBuilder::virtual_time).
    ///
    /// The handle outlives the ring — which disappears into the driver — so
    /// this is how a test inspects how far simulated time actually moved.
    pub fn clock(&self) -> Option<Rc<VirtualClock>> {
        match &self.0.borrow().pace {
            Pace::Virtual(clock) => Some(clock.clone()),
            Pace::Real => None,
        }
    }

    /// Descriptors this ring minted and handed to the runtime, oldest first.
    /// Pair with [`fds::is_open`] to check each was closed.
    pub fn opened_fds(&self) -> Vec<RawFd> {
        self.0.borrow().opened.clone()
    }

    /// What the far end of `fd` has done, or `None` while it is open.
    pub fn peer(&self, fd: RawFd) -> Option<Peer> {
        self.0.borrow().peers.get(&fd).copied()
    }

    /// Scripted completions dropped because their connection closed or reset
    /// first. Not counted by [`scenario_remaining`](Self::scenario_remaining).
    pub fn voided(&self) -> usize {
        self.0.borrow().voided
    }

    /// Every chunk delivered to a read on `fd`, one entry per completion,
    /// oldest first — what the runtime was handed, whether or not the program
    /// went on to use it. The mirror of [`writes`](Self::writes).
    pub fn reads(&self, fd: RawFd) -> Vec<Vec<u8>> {
        self.0.borrow().read.get(&fd).cloned().unwrap_or_default()
    }

    /// Every write accepted on `fd`, one entry per completed op, oldest first
    /// — what a peer reading the socket would have seen, with the op
    /// boundaries kept. A short write records only what was accepted; a failed
    /// one records nothing.
    pub fn writes(&self, fd: RawFd) -> Vec<Vec<u8>> {
        self.0
            .borrow()
            .written
            .get(&fd)
            .cloned()
            .unwrap_or_default()
    }
}

/// Builds a [`FakeRing`] and its [`FakeRingHandle`].
///
/// As a pair on purpose: the two share one state, and a way to mint a second
/// ring against the same handle would give the driver two views of one timeline.
#[derive(Default)]
pub struct FakeRingBuilder {
    virtual_time: bool,
    seed: u64,
}

impl FakeRingBuilder {
    /// Seed for the ranges a scenario asks to have drawn.
    ///
    /// Nothing else in the simulator draws, so this changes only entries
    /// written with `after_between`.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Run on a clock the ring advances instead of the OS one, so nothing ever
    /// sleeps.
    ///
    /// The runtime's timewheel is handed the same clock by
    /// [`RuntimeInner::simulated`], so its timers expire when the ring says
    /// time passed. Without this the ring sleeps for real, which is what keeps
    /// an unmodified timewheel honest — at the cost of a test taking as long as
    /// the delays it declares.
    ///
    /// [`RuntimeInner::simulated`]: crate::runtime::RuntimeInner::simulated
    pub fn virtual_time(mut self) -> Self {
        self.virtual_time = true;
        self
    }

    /// Give the ring to the runtime, keep the handle.
    ///
    /// Restarts the simulated fd numbering; panics if a descriptor from an
    /// earlier run on this thread is still open.
    pub fn build(self) -> (FakeRing, FakeRingHandle) {
        fds::reset();
        let pace = if self.virtual_time {
            Pace::Virtual(Rc::new(VirtualClock::new()))
        } else {
            Pace::Real
        };
        let handle = FakeRingHandle(Rc::new(RefCell::new(FakeRingState {
            staged: Vec::new(),
            timeline: Timeline::default(),
            waiting: Vec::new(),
            wakeup: None,
            scenario: Scenario::new(self.seed),
            cq: VecDeque::new(),
            next_seq: 0,
            opened: Vec::new(),
            peers: HashMap::new(),
            voided: 0,
            read: HashMap::new(),
            written: HashMap::new(),
            buf_rings: Vec::new(),
            actors: HashMap::new(),
            unread: HashMap::new(),
            writes_due: HashMap::new(),
            seed: self.seed,
            start: pace.now(),
            pace,
            stalled_at: 0,
            stalled_waits: 0,
            wakeup_nanos: 0,
        })));
        (FakeRing(handle.clone()), handle)
    }
}

/// The ring itself. Held by the driver; driven through [`FakeRingHandle`].
pub struct FakeRing(FakeRingHandle);

impl FakeRing {
    /// The duration the next `TIMEOUT` sqe is for.
    ///
    /// Told rather than read: see `FakeRingState::wakeup_nanos`. Called by
    /// `push_wakeup_timeout` immediately before the sqe it describes, and only
    /// one wakeup is ever armed at a time, so a single slot is enough.
    pub fn note_wakeup_timeout(&self, duration: std::time::Duration) {
        self.0.0.borrow_mut().wakeup_nanos = duration.as_nanos() as Nanos;
    }
}

/// Waits in a row at one instant before a run counts as spinning. Far above
/// what any burst of completions due together needs.
const STALL_LIMIT: u32 = 100_000;

enum Wait {
    /// Sleep to the next op's due time, then complete everything due.
    Due(Nanos),
    /// Sleep to the runtime's own deadline. Nothing completes.
    Wakeup(Nanos),
}

impl FakeRing {
    pub fn builder() -> FakeRingBuilder {
        FakeRingBuilder::default()
    }

    fn state(&self) -> RefMut<'_, FakeRingState> {
        self.0.0.borrow_mut()
    }

    pub fn add(&mut self, entry: &squeue::Entry) {
        self.state().staged.push(Sqe::decode(entry));
    }

    pub fn submit(&mut self) {
        let mut guard = self.state();
        let state = &mut *guard;

        // Lifted out because the loop calls helpers that need all of `state`,
        // which a live borrow of one field would block. Put back afterwards so
        // the allocation is reused instead of rebuilt on every submit.
        let mut staged = std::mem::take(&mut state.staged);

        // One instant for the whole batch: ops of the same latency then share a
        // due time exactly, and `seq` decides their order rather than however
        // long this loop happened to take.
        let now = state.pace.now();

        for sqe in staged.drain(..) {
            let seq = state.next_seq;
            state.next_seq += 1;

            // A simulated fd is checked on every op naming it: the driver must
            // never submit on a closed one.
            assert!(
                !fds::is_simulated(sqe.fd) || fds::is_open(sqe.fd),
                "user_data {:#x} (opcode {}) names simulated fd {}, which is closed",
                sqe.user_data,
                sqe.opcode,
                sqe.fd,
            );
            // The kernel runs a close inline at submit and posts its CQE there.
            if sqe.opcode == OP_CLOSE {
                if fds::is_simulated(sqe.fd) {
                    fds::close(sqe.fd).expect("checked open just above");
                }
                // Its last word; nothing it asks for can reach the program now.
                call_actor(state, sqe.fd, now, |actor, cx| actor.on_closed(cx));
                state.actors.remove(&sqe.fd);
                state.unread.remove(&sqe.fd);
                state.writes_due.remove(&sqe.fd);
                state.cq.push_back(Completion {
                    user_data: sqe.user_data,
                    result: 0,
                    flags: 0,
                });
                continue;
            }
            let outcome = default_outcome(&sqe);

            match classify(&sqe) {
                Class::Wakeup => {
                    let deadline = now + state.wakeup_nanos;
                    state.wakeup = Some(Wakeup {
                        deadline,
                        user_data: sqe.user_data,
                    });
                }
                Class::Waiting | Class::Scheduled => arm(
                    state,
                    Pending {
                        sqe,
                        seq,
                        outcome,
                        source: Source::Default,
                    },
                    now,
                ),
            }
        }

        state.staged = staged;
    }

    /// Flush, then block until whichever comes first: the next op falling due,
    /// or the deadline the runtime armed. Neither existing means the runtime
    /// would sleep forever, which a test should see as a failure rather than a
    /// hang.
    pub fn submit_and_wait(&mut self) {
        self.submit();
        // An actor's timer can fire and post nothing; keep waiting until
        // something does, or the runtime's own deadline comes.
        while self.state().cq.is_empty() {
            self.wait_once();
        }
    }

    fn wait_once(&mut self) {
        let (wait, pace) = {
            let mut state = self.state();
            let now = state.pace.now();
            if now == state.stalled_at {
                state.stalled_waits += 1;
            } else {
                (state.stalled_at, state.stalled_waits) = (now, 1);
            }
            assert!(
                state.stalled_waits <= STALL_LIMIT,
                "simulated ring: waited on {STALL_LIMIT} times without time moving — \
                 the program is spinning rather than blocking, so nothing due later \
                 can ever land",
            );
            if !state.cq.is_empty() {
                return;
            }
            let pace = state.pace.clone();
            let due = state.timeline.next_due();
            let wakeup = state.wakeup.as_ref().map(|w| w.deadline);
            let wait = match (due, wakeup) {
                (Some(d), Some(w)) if w < d => Wait::Wakeup(w),
                (Some(d), _) => Wait::Due(d),
                (None, Some(w)) => Wait::Wakeup(w),
                (None, None) => panic!(
                    "simulated ring: {} op(s) parked with no event to complete them \
                     and nothing scheduled — the runtime would never wake up",
                    state.waiting.len(),
                ),
            };
            (wait, pace)
        };

        match wait {
            Wait::Wakeup(deadline) => {
                pace.wait_until(deadline);
                let mut state = self.state();
                if let Some(w) = state.wakeup.take() {
                    state.cq.push_back(Completion {
                        user_data: w.user_data,
                        result: -libc::ETIME,
                        flags: 0,
                    });
                }
            }
            Wait::Due(deadline) => {
                pace.wait_until(deadline);
                let mut state = self.state();
                let fired_at = pace.now();
                let fired = state.timeline.take_due(fired_at);
                for item in fired {
                    let mut pending = match item {
                        Item::Op(pending) => pending,
                        Item::Timer {
                            fd,
                            tag,
                            generation,
                        } => {
                            let current = state.actors.get(&fd).is_some_and(|slot| {
                                slot.generations.get(&tag) == Some(&generation)
                            });
                            if current {
                                call_actor(&mut state, fd, fired_at, |actor, cx| {
                                    actor.on_timer(tag, cx)
                                });
                                repull(&mut state, fd, fired_at);
                            }
                            continue;
                        }
                        Item::Wake { fd } => {
                            repull(&mut state, fd, fired_at);
                            continue;
                        }
                    };
                    let sqe = pending.sqe;

                    // A cancel reports nothing of its own — it acts on another
                    // op — so it is resolved against live state rather than
                    // through an outcome.
                    if sqe.opcode == OP_ASYNC_CANCEL {
                        let result = apply_cancel(&mut state, sqe.addr, fired_at);
                        state.cq.push_back(Completion {
                            user_data: sqe.user_data,
                            result,
                            flags: 0,
                        });
                        continue;
                    }

                    // Resolved before the calls below so no borrow of `state` is
                    // held across them.
                    let group = sqe
                        .uses_buffer_select()
                        .then(|| {
                            state
                                .buf_rings
                                .iter()
                                .find(|(bgid, ..)| *bgid == sqe.buf_group)
                                .map(|&(_, take, capacity)| (take, capacity))
                        })
                        .flatten();
                    if matches!(pending.outcome, Outcome::Actor) {
                        serve_actor(&mut state, pending, group, fired_at);
                        continue;
                    }

                    // Armed before its connection closed or reset: the state answers now.
                    if let Some(outcome) = peer_outcome(&state, &sqe) {
                        state.voided += owed(&pending.source);
                        pending.outcome = outcome;
                        pending.source = Source::Default;
                    }

                    let seq = pending.seq;
                    // Lifted out before the outcome is consumed, so a stream can
                    // carry on from where it left off.
                    let source = std::mem::replace(&mut pending.source, Source::Default);
                    // Resolved before the calls below so no borrow of `state` is
                    // held across them.
                    let provider = group.map(|(take, _)| take);

                    // Everything the scenario declared to land in this drain,
                    // and what the op has left afterwards.
                    let (also, mut rest) = match source {
                        Source::Stream { also, rest, .. } => (also, Some(rest)),
                        _ => (Vec::new(), None),
                    };
                    let multishot = is_multishot(&sqe);
                    // A peek delivers the scripted read but does not consume
                    // it. The kernel would still hold those bytes, so they go
                    // back for the next read — with no delay, since they have
                    // by now arrived. An error is consumed even by a peek.
                    let mut peeked = (is_peek(&sqe) && !multishot).then(|| pending.outcome.clone());
                    let mut batch: VecDeque<Outcome> = std::iter::once(pending.outcome)
                        .chain(also.into_iter().map(|s| s.outcome))
                        .collect();

                    let mut ended = false;
                    while let Some(outcome) = batch.pop_front() {
                        let minted = matches!(outcome, Outcome::NewFd(_));
                        let st = &mut *state;
                        let (mut completion, unread, ends) = complete_outcome(
                            sqe,
                            outcome,
                            provider,
                            &mut st.read,
                            &mut st.written,
                            &mut st.scenario,
                        );
                        if minted && completion.result >= 0 {
                            state.opened.push(completion.result);
                        }
                        if sqe.opcode == OP_ACCEPT && completion.result >= 0 {
                            open_actor(&mut state, completion.result, fired_at);
                        }
                        // `F_MORE` is per completion: the last of a batch that
                        // closes the op does not carry it, the others do.
                        if multishot && !ends && !ends_multishot(&sqe, completion.result) {
                            completion.flags |= IORING_CQE_F_MORE;
                        } else if multishot {
                            ended = true;
                        }
                        let peer = peer_after(&sqe, completion.result);
                        if let Some(peer) = peer {
                            enter(&mut state, sqe.fd, peer, fired_at);
                            // Declared past the end of the connection, so never deliverable.
                            if ended {
                                state.voided += batch.len()
                                    + rest.take().map_or(0, |r| r.iter().map(Batch::len).sum());
                                batch.clear();
                            }
                        }
                        // A peeked EOF stays with the connection's state, not the script.
                        let keep = completion.result >= 0 && peer.is_none();
                        if let Some(outcome) = peeked.take().filter(|_| keep) {
                            let entry = Entry::Once(Scripted {
                                outcome,
                                delay: None,
                            });
                            restore(&mut state, Trigger::Read(sqe.fd), entry, fired_at);
                        }
                        state.cq.push_back(completion);

                        // The unread bytes go to this drain, the next one, or — once the
                        // op is over with the connection alive — the op that re-arms,
                        // with everything declared after them, as a socket keeps them.
                        if ended && (unread.is_some() || ends) {
                            let mut stream = rest.take().unwrap_or_default();
                            // Already in the socket, so no gap for the re-armed op.
                            let carried = carry(unread, &mut batch, None);
                            if !carried.is_empty() {
                                stream.push_front(carried);
                            }
                            if !stream.is_empty() {
                                let entry = Entry::Stream(stream);
                                restore(&mut state, Trigger::Read(sqe.fd), entry, fired_at);
                            }
                        } else if let Some(flow) = unread {
                            if state.scenario.same_drain() {
                                batch.push_front(Outcome::Flow(flow));
                            } else {
                                let gap = flow.gap();
                                rest.get_or_insert_default().push_front(carry(
                                    Some(flow),
                                    &mut batch,
                                    gap,
                                ));
                            }
                        }
                    }

                    if multishot && !ended {
                        match rest {
                            // Declared as one op's life: take the next of its
                            // own batches rather than reconsulting the trigger,
                            // which another reader may share.
                            Some(rest) => advance_stream(&mut state, sqe, seq, rest, fired_at),
                            None => arm(
                                &mut state,
                                Pending {
                                    sqe,
                                    seq,
                                    outcome: default_outcome(&sqe),
                                    source: Source::Default,
                                },
                                fired_at,
                            ),
                        }
                    }
                }
                // Armed with `.count(1)`, so it completes as soon as one
                // completion event lands — not only on elapse. An actor's
                // timer that posted nothing lands none.
                if !state.cq.is_empty()
                    && let Some(w) = state.wakeup.take()
                {
                    state.cq.push_back(Completion {
                        user_data: w.user_data,
                        result: 0,
                        flags: 0,
                    });
                }
            }
        }
    }

    /// Take the next queued completion, oldest first.
    pub fn pop_completion(&mut self) -> Option<Completion> {
        self.state().cq.pop_front()
    }

    pub fn ring_fd(&self) -> RawFd {
        SIMULATED_RING_FD
    }

    /// The handle onto this ring's state, for installing buffer rings.
    pub fn handle(&self) -> &FakeRingHandle {
        &self.0
    }

    /// The clock this ring advances, if it was built with
    /// [`virtual_time`](FakeRingBuilder::virtual_time).
    ///
    /// The runtime's timewheel needs the same one, or the two would disagree
    /// about what time it is.
    pub fn clock(&self) -> Option<Rc<VirtualClock>> {
        match &self.state().pace {
            Pace::Virtual(clock) => Some(clock.clone()),
            Pace::Real => None,
        }
    }
}
