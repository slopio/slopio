//! A submission/completion ring that never reaches the kernel.
//!
//! The fake accepts SQEs exactly as the real ring does, decodes them into
//! [`Sqe`], and schedules them on a [`Wheel`]. Which bucket an op lands in is
//! drawn from a seeded PRNG, so a seed fixes the completion order exactly —
//! the orderings that are rare and unreproducible against a real kernel (a
//! completion landing after its cancel, a zero-copy notification arriving
//! before the send it belongs to) become things a test asks for by seed.
//!
//! Not every submission is scheduled. Ops fall into three classes:
//!
//! - **scheduled** — the kernel finishes these on its own, so a bucket is the
//!   right model: sends, writes, opens, cancels.
//! - **waiting** — these block on something external. A recv on a socket nobody
//!   wrote to must never complete on its own, so it parks until a test says
//!   otherwise.
//! - **wakeup** — the `TIMEOUT` the runtime arms before it blocks. Not I/O at
//!   all: it is the upper bound on how long this ring may wait, so it bounds
//!   the sleep instead of taking a bucket.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::os::fd::RawFd;
use std::rc::Rc;
use std::time::Duration;

use io_uring::{opcode, squeue};

use super::clock::{self, Nanos};
use super::sqe::Sqe;
use super::wheel::{Outcome, Pending, Wheel};

/// The `ring_fd` a simulated ring reports.
///
/// Peer messaging looks up a target ring by fd and posts `MSG_RING` to it; a
/// fake has no descriptor to give. The sentinel is deliberately invalid so a
/// stray syscall on it fails loudly instead of touching an unrelated fd.
pub const SIMULATED_RING_FD: RawFd = -1;

/// One completion, as the three fields a CQE carries.
#[derive(Clone, Copy, Debug)]
pub struct Completion {
    pub user_data: u64,
    pub result: i32,
    pub flags: u32,
}

// Opcodes are matched as constants rather than through `opcode::X::CODE` paths
// so the arms below read as a table. Note the collisions the io-uring builders
// hide: `Recv`/`RecvMulti`/`RecvBundle` are all `IORING_OP_RECV`, and the
// recvmsg and accept pairs likewise share one opcode each.
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

/// How the simulator treats a submission. See the module docs.
enum Class {
    Scheduled,
    Waiting,
    Wakeup,
}

fn classify(sqe: &Sqe) -> Class {
    match sqe.opcode {
        OP_TIMEOUT => Class::Wakeup,
        OP_RECV | OP_READ | OP_RECVMSG | OP_ACCEPT | OP_CONNECT | OP_WAITID => Class::Waiting,
        _ => Class::Scheduled,
    }
}

/// What an op completes with when a test hasn't said otherwise.
fn default_outcome(sqe: &Sqe) -> Outcome {
    let result = match sqe.opcode {
        // A write-side op reports the byte count it was handed. (`WRITEV`'s
        // `len` is an iovec count, not a byte count — close enough until
        // something needs the real figure.)
        OP_SEND | OP_SEND_ZC | OP_SENDMSG | OP_WRITE | OP_WRITEV | OP_WRITE_FIXED => sqe.len as i32,
        _ => 0,
    };
    Outcome { result, flags: 0 }
}

/// The kernel's `__kernel_timespec`.
///
/// `io_uring::types::Timespec` is `repr(transparent)` over it but exposes no
/// accessors, so reading a `TIMEOUT`'s duration means restating the layout —
/// the same reinterpretation [`Sqe`] already relies on.
#[repr(C)]
struct KernelTimespec {
    tv_sec: i64,
    tv_nsec: i64,
}

/// The duration a `TIMEOUT` sqe was armed with.
fn timeout_duration(sqe: &Sqe) -> Nanos {
    if sqe.addr == 0 {
        return 0;
    }
    // SAFETY: a `TIMEOUT` sqe carries a pointer to a `Timespec` the submitter
    // keeps alive for as long as the op is in flight — the same contract the
    // kernel itself relies on to read it. The runtime's wakeup timer holds it
    // in a driver field, so it outlives this read.
    let ts = unsafe { &*(sqe.addr as *const KernelTimespec) };
    (ts.tv_sec.max(0) as Nanos) * 1_000_000_000 + ts.tv_nsec.max(0) as Nanos
}

/// The runtime's armed idle timer.
struct Wakeup {
    deadline: Nanos,
    user_data: u64,
}

struct FakeRingState {
    /// Pushed by the driver, not yet flushed by a `submit`.
    staged: Vec<Sqe>,
    /// Ops with a bucket, waiting for their tick.
    wheel: Wheel,
    /// Ops blocked on an event the simulation hasn't produced.
    waiting: Vec<Pending>,
    /// The bound on how long this ring may block, if the runtime armed one.
    wakeup: Option<Wakeup>,
    /// Completions waiting for the driver to drain them.
    cq: VecDeque<Completion>,
    next_seq: u64,
}

/// A test's handle on a [`FakeRing`], for inspecting what the runtime submitted
/// and deciding what comes back.
///
/// Cloneable and shared: the test keeps one, the runtime's driver holds the
/// other, and both see the same ring.
#[derive(Clone)]
pub struct FakeRingHandle(Rc<RefCell<FakeRingState>>);

impl FakeRingHandle {
    /// Every op submitted and not yet completed, in submission order.
    pub fn inflight(&self) -> Vec<Sqe> {
        let state = self.0.borrow();
        let mut all: Vec<&Pending> = state.wheel.iter().chain(state.waiting.iter()).collect();
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

    /// Complete an in-flight op now, ahead of whatever tick it was scheduled
    /// for: queue a completion for `user_data` and retire the op.
    ///
    /// Panics if no op with that `user_data` is in flight. Completing an op
    /// twice, or one that was never submitted, would hand the driver a slab
    /// index it dereferences unchecked — so this is caught here, where the
    /// mistake is, rather than as a crash somewhere unrelated.
    pub fn complete(&self, user_data: u64, result: i32, flags: u32) {
        let mut state = self.0.borrow_mut();
        let found = state.wheel.take_by_user_data(user_data).is_some() || {
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

    /// Queue a completion for an op that stays in flight — a multishot yielding
    /// one of many results, or a zero-copy send whose notification is still to
    /// come. `flags` should carry `IORING_CQE_F_MORE`.
    ///
    /// Panics if the op is not in flight, for the same reason as [`complete`].
    ///
    /// [`complete`]: Self::complete
    pub fn complete_keeping(&self, user_data: u64, result: i32, flags: u32) {
        let mut state = self.0.borrow_mut();
        assert!(
            state.wheel.contains(user_data)
                || state.waiting.iter().any(|p| p.sqe.user_data == user_data),
            "completing user_data {user_data:#x}, which is not in flight",
        );
        state.cq.push_back(Completion {
            user_data,
            result,
            flags,
        });
    }

    /// Move a parked op onto the wheel, as though the event it was waiting for
    /// just happened. It still serves its bucket delay rather than completing
    /// on the spot.
    ///
    /// Panics if no waiting op has that `user_data`.
    pub fn wake(&self, user_data: u64, result: i32) {
        let mut state = self.0.borrow_mut();
        let i = state
            .waiting
            .iter()
            .position(|p| p.sqe.user_data == user_data)
            .unwrap_or_else(|| panic!("waking user_data {user_data:#x}, which is not parked"));
        let mut pending = state.waiting.remove(i);
        pending.outcome = Outcome { result, flags: 0 };
        state.wheel.schedule(pending);
    }

    /// Completions queued but not yet drained by the driver.
    pub fn queued_completions(&self) -> usize {
        self.0.borrow().cq.len()
    }

    /// Ops parked with no event to complete them.
    pub fn parked(&self) -> usize {
        self.0.borrow().waiting.len()
    }
}

/// Builds a [`FakeRing`] together with the [`FakeRingHandle`] that drives it.
///
/// The two are made as a pair on purpose: a ring and its handle share one
/// state, and handing out a way to mint a second ring against the same handle
/// would give the driver two views of one in-flight list.
pub struct FakeRingBuilder {
    bucket_step: Duration,
    seed: u64,
}

impl Default for FakeRingBuilder {
    fn default() -> Self {
        Self {
            bucket_step: Duration::from_millis(10),
            seed: 0,
        }
    }
}

impl FakeRingBuilder {
    /// Latency granularity. Bucket `i` delays an op by `step * (i + 1)`, so the
    /// default 10ms spans 10–60ms.
    ///
    /// Shrinking it speeds a test up but changes more than the clock. The sleep
    /// this is enforced with has a floor around 50µs, so below roughly 1ms
    /// adjacent buckets stop being distinguishable and drain as one batch —
    /// same completion order, fewer wakeups, and therefore fewer chances for
    /// tasks to interleave between them.
    pub fn bucket_step(mut self, step: Duration) -> Self {
        self.bucket_step = step;
        self
    }

    /// Seed for bucket assignment. The same seed reproduces an interleaving
    /// exactly; sweeping seeds explores the ordering space.
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Produce the ring and its handle. Give the ring to the runtime, keep the
    /// handle.
    pub fn build(self) -> (FakeRing, FakeRingHandle) {
        let handle = FakeRingHandle(Rc::new(RefCell::new(FakeRingState {
            staged: Vec::new(),
            wheel: Wheel::new(self.bucket_step.as_nanos() as Nanos, self.seed),
            waiting: Vec::new(),
            wakeup: None,
            cq: VecDeque::new(),
            next_seq: 0,
        })));
        (FakeRing(handle.clone()), handle)
    }
}

/// The ring itself. Held by the driver; driven through [`FakeRingHandle`].
pub struct FakeRing(FakeRingHandle);

/// What a blocking wait decided to do.
enum Wait {
    /// Sleep to a tick's deadline, then fire it.
    Tick(super::wheel::Tick),
    /// Sleep to the runtime's own wakeup deadline. Nothing completes.
    Wakeup(Nanos),
}

impl FakeRing {
    /// Start building a ring and its handle.
    pub fn builder() -> FakeRingBuilder {
        FakeRingBuilder::default()
    }

    pub fn add(&mut self, entry: &squeue::Entry) {
        self.0.0.borrow_mut().staged.push(Sqe::decode(entry));
    }

    pub fn submit(&mut self) {
        let mut state = self.0.0.borrow_mut();
        let staged = std::mem::take(&mut state.staged);
        for sqe in staged {
            let seq = state.next_seq;
            state.next_seq += 1;

            match classify(&sqe) {
                Class::Wakeup => {
                    let deadline = clock::now() + timeout_duration(&sqe);
                    state.wakeup = Some(Wakeup {
                        deadline,
                        user_data: sqe.user_data,
                    });
                }
                Class::Waiting => {
                    let outcome = default_outcome(&sqe);
                    state.waiting.push(Pending { sqe, seq, outcome });
                }
                Class::Scheduled => {
                    let outcome = default_outcome(&sqe);
                    state.wheel.schedule(Pending { sqe, seq, outcome });
                }
            }
        }
    }

    /// Flush, then block until something completes.
    ///
    /// Whichever comes first: the next tick carrying work, or the deadline the
    /// runtime armed. If neither exists the runtime would sleep forever, which
    /// a test should see as a failure rather than a hang.
    pub fn submit_and_wait(&mut self) {
        self.submit();

        let wait = {
            let mut state = self.0.0.borrow_mut();
            if !state.cq.is_empty() {
                return; // the driver already has work to drain
            }
            let tick = state.wheel.next_tick();
            let wakeup = state.wakeup.as_ref().map(|w| w.deadline);
            match (tick, wakeup) {
                (Some(t), Some(w)) if w < t.deadline => Wait::Wakeup(w),
                (Some(t), _) => Wait::Tick(t),
                (None, Some(w)) => Wait::Wakeup(w),
                (None, None) => panic!(
                    "simulated ring: {} op(s) parked with no event to complete them \
                     and nothing scheduled — the runtime would never wake up",
                    state.waiting.len(),
                ),
            }
        };

        match wait {
            Wait::Wakeup(deadline) => {
                clock::sleep_until(deadline);
                let mut state = self.0.0.borrow_mut();
                if let Some(w) = state.wakeup.take() {
                    state.cq.push_back(Completion {
                        user_data: w.user_data,
                        result: -libc::ETIME,
                        flags: 0,
                    });
                }
            }
            Wait::Tick(tick) => {
                clock::sleep_until(tick.deadline);
                let mut state = self.0.0.borrow_mut();
                let fired = state.wheel.fire(tick);
                for pending in fired {
                    state.cq.push_back(Completion {
                        user_data: pending.sqe.user_data,
                        result: pending.outcome.result,
                        flags: pending.outcome.flags,
                    });
                }
                // The runtime arms its timer with `.count(1)`, so it completes
                // as soon as one completion event lands — not only on elapse.
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
        self.0.0.borrow_mut().cq.pop_front()
    }

    pub fn ring_fd(&self) -> RawFd {
        SIMULATED_RING_FD
    }
}
