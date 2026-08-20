//! A submission/completion ring that never reaches the kernel.
//!
//! Submissions are decoded into [`Sqe`] and scheduled on a seeded [`Timeline`],
//! so
//! a seed fixes the completion order exactly — orderings that are rare and
//! unreproducible against a real kernel become things a test asks for.

use std::cell::{RefCell, RefMut};
use std::collections::VecDeque;
use std::os::fd::RawFd;
use std::rc::Rc;
use std::time::Duration;

use io_uring::{opcode, squeue};

use super::clock::{self, Nanos};
use super::fds::{self, FdKind};
use super::scenario::{Scenario, Scripted, Trigger};
use super::sqe::{IORING_ACCEPT_MULTISHOT, IORING_CQE_F_MORE, IORING_RECV_MULTISHOT, Sqe};
use super::timeline::{Outcome, Pending, Timeline};
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
const OP_WRITE: u8 = opcode::Write::CODE;
const OP_WRITEV: u8 = opcode::Writev::CODE;
const OP_WRITE_FIXED: u8 = opcode::WriteFixed::CODE;
const OP_OPENAT: u8 = opcode::OpenAt::CODE;
const OP_ASYNC_CANCEL: u8 = opcode::AsyncCancel::CODE;

enum Class {
    /// The kernel finishes these alone: sends, writes, opens, cancels.
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
fn trigger_for(sqe: &Sqe) -> Option<Trigger> {
    match sqe.opcode {
        OP_RECV | OP_READ | OP_RECVMSG => Some(Trigger::Read(sqe.fd)),
        OP_ACCEPT => Some(Trigger::Accept(sqe.fd)),
        OP_CONNECT => Some(Trigger::Connect(sqe.fd)),
        _ => None,
    }
}

/// Put an op where it belongs: on the timeline if the scenario already says
/// what happens to it, parked otherwise.
fn arm(state: &mut FakeRingState, mut pending: Pending, now: Nanos) {
    let scripted = trigger_for(&pending.sqe).and_then(|t| state.scenario.take(t));
    match scripted {
        Some(scripted) => {
            pending.outcome = scripted.outcome;
            state.timeline.schedule(pending, scripted.delay, now);
        }
        None => state.waiting.push(pending),
    }
}

fn default_outcome(sqe: &Sqe) -> Outcome {
    let result = match sqe.opcode {
        OP_OPENAT => return Outcome::NewFd(FdKind::File),
        // `WRITEV`'s `len` is an iovec count rather than a byte count — close
        // enough until something needs the real figure.
        OP_SEND | OP_SEND_ZC | OP_SENDMSG | OP_WRITE | OP_WRITEV | OP_WRITE_FIXED => sqe.len as i32,
        _ => 0,
    };
    Outcome::Result { result, flags: 0 }
}

/// Retire an op: deliver its outcome and build the CQE. A CQE carries no
/// payload, so data goes into the op's buffer and only the count is reported.
fn complete_pending(pending: Pending) -> Completion {
    let Pending { sqe, outcome, .. } = pending;
    match outcome {
        Outcome::Result { result, flags } => Completion {
            user_data: sqe.user_data,
            result,
            flags,
        },
        Outcome::NewFd(kind) => Completion {
            user_data: sqe.user_data,
            result: match fds::mint(kind) {
                Ok(fd) => fd,
                // Report exhaustion the way the kernel would, rather than
                // panicking inside what is meant to be a completion.
                Err(e) => -e.raw_os_error().unwrap_or(libc::ENFILE),
            },
            flags: 0,
        },
        Outcome::Data(bytes) => {
            assert!(
                !sqe.uses_buffer_select(),
                "delivering data to user_data {:#x}, a provided-buffer op: its \
                 `addr` names no buffer, so the bytes would land on unrelated \
                 memory. Managed recv needs a buffer group and a `bid`, which \
                 the simulator does not model yet.",
                sqe.user_data,
            );
            assert!(
                sqe.addr != 0,
                "delivering data to user_data {:#x}, which has no buffer",
                sqe.user_data,
            );

            // Truncate rather than reject: that is a short read.
            let n = bytes.len().min(sqe.len as usize);
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
#[repr(C)]
struct KernelTimespec {
    tv_sec: i64,
    tv_nsec: i64,
}

fn timeout_duration(sqe: &Sqe) -> Nanos {
    if sqe.addr == 0 {
        return 0;
    }
    // SAFETY: a `TIMEOUT` sqe points at a `Timespec` the submitter keeps alive
    // while the op is in flight — the contract the kernel itself reads it under.
    let ts = unsafe { &*(sqe.addr as *const KernelTimespec) };
    (ts.tv_sec.max(0) as Nanos) * 1_000_000_000 + ts.tv_nsec.max(0) as Nanos
}

struct Wakeup {
    deadline: Nanos,
    user_data: u64,
}

/// Retire the op an `ASYNC_CANCEL` names, and report what the cancel itself
/// should complete with.
///
/// The target's CQE carries no `F_MORE`, which is what tells the driver a
/// multishot is finally done. Both are queued here, target first, matching the
/// order a real ring reports them in.
fn apply_cancel(state: &mut FakeRingState, target: u64) -> i32 {
    let found = state.timeline.take_by_user_data(target).is_some() || {
        match state.waiting.iter().position(|p| p.sqe.user_data == target) {
            Some(i) => {
                state.waiting.remove(i);
                true
            }
            None => false,
        }
    };
    if !found {
        // Already gone. A cancel racing its target's own completion gets this
        // from a real kernel too — including when both land in one batch here.
        return -libc::ENOENT;
    }
    state.cq.push_back(Completion {
        user_data: target,
        result: -libc::ECANCELED,
        flags: 0,
    });
    0
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
    pace: Pace,
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
    /// with a real, unconnected TCP socket the runtime can set options on and
    /// close like any other.
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
    /// what keeps the interleaving a function of the seed alone.
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

    fn script(&self, trigger: Trigger, scripted: Scripted) {
        self.0.borrow_mut().scenario.push(trigger, scripted);
    }

    /// Scripted responses that were never claimed — three reads declared
    /// against code that performed two.
    pub fn scenario_remaining(&self) -> usize {
        self.0.borrow().scenario.remaining()
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

    /// Descriptors the simulator minted and handed to the runtime, oldest
    /// first. They are real and owned by whatever received them, so a test can
    /// check that each was closed — an fd still open at the end is a leak.
    pub fn opened_fds(&self) -> Vec<RawFd> {
        self.0.borrow().opened.clone()
    }
}

/// Builds a [`FakeRing`] and its [`FakeRingHandle`].
///
/// As a pair on purpose: the two share one state, and a way to mint a second
/// ring against the same handle would give the driver two views of one timeline.
pub struct FakeRingBuilder {
    latency_step: Duration,
    seed: u64,
    virtual_time: bool,
}

impl Default for FakeRingBuilder {
    fn default() -> Self {
        Self {
            latency_step: Duration::from_millis(10),
            seed: 0,
            virtual_time: false,
        }
    }
}

impl FakeRingBuilder {
    /// Latency granularity for ops the scenario did not time: each draws a
    /// multiple of `step`, from one up to [`LATENCY_CLASSES`].
    ///
    /// Below roughly 1ms the classes stop being distinguishable — the sleep
    /// enforcing them has a floor around 50µs, so neighbouring ones drain as
    /// one batch. Same completion order, but fewer wakeups for tasks to
    /// interleave between.
    ///
    /// [`LATENCY_CLASSES`]: super::timeline::LATENCY_CLASSES
    pub fn latency_step(mut self, step: Duration) -> Self {
        self.latency_step = step;
        self
    }

    /// Seed for the drawn latencies. The same seed reproduces an interleaving
    /// exactly; sweeping seeds explores the ordering space.
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
            timeline: Timeline::new(self.latency_step.as_nanos() as Nanos, self.seed),
            waiting: Vec::new(),
            wakeup: None,
            scenario: Scenario::default(),
            cq: VecDeque::new(),
            next_seq: 0,
            opened: Vec::new(),
            pace,
        })));
        (FakeRing(handle.clone()), handle)
    }
}

/// The ring itself. Held by the driver; driven through [`FakeRingHandle`].
pub struct FakeRing(FakeRingHandle);

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
            let outcome = default_outcome(&sqe);

            match classify(&sqe) {
                Class::Wakeup => {
                    let deadline = now + timeout_duration(&sqe);
                    state.wakeup = Some(Wakeup {
                        deadline,
                        user_data: sqe.user_data,
                    });
                }
                Class::Waiting => arm(state, Pending { sqe, seq, outcome }, now),
                Class::Scheduled => {
                    state
                        .timeline
                        .schedule(Pending { sqe, seq, outcome }, None, now)
                }
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
                for pending in fired {
                    let sqe = pending.sqe;

                    // A cancel reports nothing of its own — it acts on another
                    // op — so it is resolved against live state rather than
                    // through an outcome.
                    if sqe.opcode == OP_ASYNC_CANCEL {
                        let result = apply_cancel(&mut state, sqe.addr);
                        state.cq.push_back(Completion {
                            user_data: sqe.user_data,
                            result,
                            flags: 0,
                        });
                        continue;
                    }

                    let seq = pending.seq;
                    let minted = matches!(pending.outcome, Outcome::NewFd(_));
                    let mut completion = complete_pending(pending);
                    if minted && completion.result >= 0 {
                        state.opened.push(completion.result);
                    }

                    // A multishot that is not finished gets `F_MORE` and goes
                    // back to await its next event. A terminal result, or a
                    // cancel, is what takes it out of flight.
                    if is_multishot(&sqe) && !ends_multishot(&sqe, completion.result) {
                        completion.flags |= IORING_CQE_F_MORE;
                        let outcome = default_outcome(&sqe);
                        arm(&mut state, Pending { sqe, seq, outcome }, fired_at);
                    }

                    state.cq.push_back(completion);
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
