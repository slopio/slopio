//! A submission/completion ring that never reaches the kernel.
//!
//! The fake accepts SQEs exactly as the real ring does, decodes them into
//! [`Sqe`], and parks them. Nothing completes on its own: a test decides which
//! op finishes, with what result, and in what order. That is the whole point —
//! the orderings that are rare and unreproducible against a real kernel (a
//! completion landing after its cancel, a zero-copy notification arriving
//! before the send it belongs to) become things a test simply asks for.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::os::fd::RawFd;
use std::rc::Rc;

use io_uring::squeue;

use super::sqe::Sqe;

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

#[derive(Default)]
struct FakeRingState {
    /// Pushed by the driver, not yet flushed by a `submit`.
    staged: Vec<Sqe>,
    /// Submitted, awaiting a completion the test has not posted yet.
    inflight: Vec<Sqe>,
    /// Completions waiting for the driver to drain them.
    cq: VecDeque<Completion>,
}

/// A test's handle on a [`FakeRing`], for inspecting what the runtime submitted
/// and deciding what comes back.
///
/// Cloneable and shared: the test keeps one, the runtime's driver holds the
/// other, and both see the same ring.
#[derive(Clone, Default)]
pub struct FakeRingHandle(Rc<RefCell<FakeRingState>>);

impl FakeRingHandle {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build the ring this handle drives. Hand the result to the runtime.
    pub fn ring(&self) -> FakeRing {
        FakeRing(self.clone())
    }

    /// Every op submitted and not yet completed, oldest first.
    pub fn inflight(&self) -> Vec<Sqe> {
        self.0.borrow().inflight.clone()
    }

    /// The oldest in-flight op, if any.
    pub fn next_inflight(&self) -> Option<Sqe> {
        self.0.borrow().inflight.first().copied()
    }

    /// In-flight ops matching `pred`, oldest first.
    pub fn inflight_matching(&self, pred: impl Fn(&Sqe) -> bool) -> Vec<Sqe> {
        self.0
            .borrow()
            .inflight
            .iter()
            .filter(|s| pred(s))
            .copied()
            .collect()
    }

    /// Complete an in-flight op: queue a completion for `user_data` and retire
    /// the op.
    ///
    /// Panics if no op with that `user_data` is in flight. Completing an op
    /// twice, or one that was never submitted, would hand the driver a slab
    /// index it dereferences unchecked — so this is caught here, where the
    /// mistake is, rather than as a crash somewhere unrelated.
    pub fn complete(&self, user_data: u64, result: i32, flags: u32) {
        let mut state = self.0.borrow_mut();
        let idx = state
            .inflight
            .iter()
            .position(|s| s.user_data == user_data)
            .unwrap_or_else(|| {
                panic!("completing user_data {user_data:#x}, which is not in flight")
            });
        state.inflight.remove(idx);
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
            state.inflight.iter().any(|s| s.user_data == user_data),
            "completing user_data {user_data:#x}, which is not in flight",
        );
        state.cq.push_back(Completion {
            user_data,
            result,
            flags,
        });
    }

    /// Completions queued but not yet drained by the driver.
    pub fn queued_completions(&self) -> usize {
        self.0.borrow().cq.len()
    }
}

/// The ring itself. Held by the driver; driven through [`FakeRingHandle`].
pub struct FakeRing(FakeRingHandle);

impl FakeRing {
    pub fn add(&mut self, entry: &squeue::Entry) {
        self.0.0.borrow_mut().staged.push(Sqe::decode(entry));
    }

    pub fn submit(&mut self) {
        let mut state = self.0.0.borrow_mut();
        let staged = std::mem::take(&mut state.staged);
        state.inflight.extend(staged);
    }

    /// Flush, then check there is something to come back with.
    ///
    /// The real ring blocks here until the kernel has at least one completion.
    /// A fake has no one to wait for, so an empty completion queue means the
    /// runtime would have blocked forever — a deadlock the test should see as a
    /// failure rather than a hang.
    pub fn submit_and_wait(&mut self) {
        self.submit();
        let state = self.0.0.borrow();
        assert!(
            !state.cq.is_empty(),
            "simulated ring: the runtime blocked with no completion available \
             and {} op(s) in flight — it would never wake up",
            state.inflight.len(),
        );
    }

    /// Take the next queued completion, oldest first.
    pub fn pop_completion(&mut self) -> Option<Completion> {
        self.0.0.borrow_mut().cq.pop_front()
    }

    pub fn ring_fd(&self) -> RawFd {
        SIMULATED_RING_FD
    }
}
