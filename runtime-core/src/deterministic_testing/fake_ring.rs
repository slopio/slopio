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

use super::clock::{self, Nanos};
use super::fds::{self, FdKind};
use super::scenario::{Batch, Delay, Entry, Scenario, Scripted, Trigger};
use super::sqe::{
    IORING_ACCEPT_MULTISHOT, IORING_CQE_BUFFER_SHIFT, IORING_CQE_F_BUFFER, IORING_CQE_F_MORE,
    IORING_RECV_MULTISHOT, Sqe,
};
use super::timeline::{Outcome, Pending, Source, Timeline};
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

/// Put an op where it belongs: on the timeline if the scenario already says
/// what happens to it, parked otherwise — unless it is one the kernel finishes
/// alone, which then completes with its default.
fn arm(state: &mut FakeRingState, mut pending: Pending, now: Nanos) {
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
/// payload, so data goes into the op's buffer and only the count is reported;
/// data going the other way is recorded in `written`.
fn complete_outcome(
    sqe: Sqe,
    outcome: Outcome,
    provider: Option<BufRingTake>,
    written: &mut HashMap<RawFd, Vec<Vec<u8>>>,
) -> Completion {
    match outcome {
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
        Outcome::Data(bytes) if sqe.uses_buffer_select() => {
            let take = provider.unwrap_or_else(|| {
                panic!(
                    "user_data {:#x} draws from buffer group {}, which no registered ring \
                     provides — was the spec passed to the entry point?",
                    sqe.user_data, sqe.buf_group,
                )
            });
            let Some((bid, ptr, capacity)) = take() else {
                // Every buffer handed out and none recycled. A real ring says
                // the same, and a leaked `BorrowedBuffer` is how a test gets
                // here.
                return Completion {
                    user_data: sqe.user_data,
                    result: -libc::ENOBUFS,
                    flags: 0,
                };
            };

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
                // SAFETY: the pointer comes from the ring's own entry for
                // `bid`, so it addresses that buffer, and the write is capped
                // at the buffer size the ring was built with.
                unsafe {
                    std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr.as_ptr(), n);
                }
            }
            Completion {
                user_data: sqe.user_data,
                result: n as i32,
                // Which buffer the data landed in. The driver reads it back
                // with `cqueue::buffer_select`.
                flags: IORING_CQE_F_BUFFER | ((bid as u32) << IORING_CQE_BUFFER_SHIFT),
            }
        }
        Outcome::Data(bytes) => {
            assert!(
                sqe.addr != 0,
                "delivering data to user_data {:#x}, which has no buffer",
                sqe.user_data,
            );

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
                // SAFETY: `addr`/`len` describe a buffer the submitting future
                // keeps alive until its op completes — the same contract that
                // lets the kernel write there. The op is being retired now, the
                // write is capped at the declared length, and source and
                // destination are distinct allocations.
                unsafe {
                    std::ptr::copy_nonoverlapping(bytes.as_ptr(), sqe.addr as *mut u8, n);
                }
            }
            Completion {
                user_data: sqe.user_data,
                result: n as i32,
                flags: 0,
            }
        }
    }
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
    /// What the runtime wrote and the simulation accepted, per descriptor, one
    /// entry per completed op in completion order.
    written: HashMap<RawFd, Vec<Vec<u8>>>,
    /// Registered buffer rings, by group id.
    buf_rings: Vec<(u16, BufRingTake)>,
    pace: Pace,
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
        self.0
            .borrow_mut()
            .scenario
            .push(trigger, Entry::Once(scripted));
    }

    fn stream<B: Into<Batch>>(&self, trigger: Trigger, batches: impl IntoIterator<Item = B>) {
        let sequence = batches.into_iter().map(Into::into).collect();
        self.0
            .borrow_mut()
            .scenario
            .push(trigger, Entry::Stream(sequence));
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
            .map(|p| match &p.source {
                Source::Default => 0,
                // Taken from the scenario but not yet posted, so still owed.
                Source::Scenario(_) => 1,
                // The pending completion, the rest of its batch, and every
                // batch after it.
                Source::Stream { also, rest, .. } => {
                    1 + also.len() + rest.iter().map(Batch::len).sum::<usize>()
                }
            })
            .sum();
        state.scenario.remaining() + undelivered
    }

    /// Install a provided-buffer ring under `bgid`.
    ///
    /// Called from a spec's `init` when the runtime starts on a simulated ring.
    /// Without it the simulator has no way to reach the buffers a
    /// `BUFFER_SELECT` op draws from.
    pub fn register_buf_ring(&self, bgid: u16, take: BufRingTake) {
        self.0.borrow_mut().buf_rings.push((bgid, take));
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
    pub fn build(self) -> (FakeRing, FakeRingHandle) {
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
            written: HashMap::new(),
            buf_rings: Vec::new(),
            pace,
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

        let (wait, pace) = {
            let state = self.state();
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
                for mut pending in fired {
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

                    let seq = pending.seq;
                    // Lifted out before the outcome is consumed, so a stream can
                    // carry on from where it left off.
                    let source = std::mem::replace(&mut pending.source, Source::Default);
                    // Resolved before the calls below so no borrow of `state` is
                    // held across them.
                    let provider = sqe
                        .uses_buffer_select()
                        .then(|| {
                            state
                                .buf_rings
                                .iter()
                                .find(|(bgid, _)| *bgid == sqe.buf_group)
                                .map(|(_, take)| *take)
                        })
                        .flatten();

                    // Everything the scenario declared to land in this drain,
                    // and what the op has left afterwards.
                    let (also, rest) = match source {
                        Source::Stream { also, rest, .. } => (also, Some(rest)),
                        _ => (Vec::new(), None),
                    };
                    let multishot = is_multishot(&sqe);
                    // A peek delivers the scripted read but does not consume
                    // it. The kernel would still hold those bytes, so they go
                    // back for the next read — with no delay, since they have
                    // by now arrived. An error is consumed even by a peek.
                    let mut peeked = (is_peek(&sqe) && !multishot).then(|| pending.outcome.clone());
                    let batch =
                        std::iter::once(pending.outcome).chain(also.into_iter().map(|s| s.outcome));

                    let mut ended = false;
                    for outcome in batch {
                        let minted = matches!(outcome, Outcome::NewFd(_));
                        let mut completion =
                            complete_outcome(sqe, outcome, provider, &mut state.written);
                        if minted && completion.result >= 0 {
                            state.opened.push(completion.result);
                        }
                        // `F_MORE` is per completion: the last of a batch that
                        // closes the op does not carry it, the others do.
                        if multishot && !ends_multishot(&sqe, completion.result) {
                            completion.flags |= IORING_CQE_F_MORE;
                        } else if multishot {
                            ended = true;
                        }
                        if let Some(outcome) = peeked.take().filter(|_| completion.result >= 0) {
                            let entry = Entry::Once(Scripted {
                                outcome,
                                delay: None,
                            });
                            restore(&mut state, Trigger::Read(sqe.fd), entry, fired_at);
                        }
                        state.cq.push_back(completion);
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
                // completion event lands — not only on elapse.
                if let Some(w) = state.wakeup.take() {
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
