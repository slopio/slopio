use std::time::Duration;

/// How a worker's io_uring is set up.
///
/// The default is the runtime's usual ring: cooperative, deferred task work,
/// a single issuer. [`sqpoll`](Self::sqpoll) trades that for a kernel thread
/// that polls the submission queue, so submitting needs no syscall while it is
/// awake.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RingConfig {
    pub sqpoll: Option<SqPoll>,
}

impl RingConfig {
    pub fn sqpoll(sqpoll: SqPoll) -> Self {
        Self {
            sqpoll: Some(sqpoll),
        }
    }
}

/// The kernel submission-queue polling thread.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SqPoll {
    /// How long the thread spins on an empty queue before it sleeps.
    pub idle: Duration,
    /// The CPU it is pinned to; must be one the process may run on.
    pub cpu: Option<u32>,
}
