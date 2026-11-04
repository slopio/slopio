//! What a runtime is created with, and the copy each thread keeps of it.

use std::cell::Cell;

use crate::io_driver::RingOptions;

/// What a runtime is built with: its ring, and what its thread does later.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RuntimeOptions {
    pub ring: RingOptions,
    pub net: NetOptions,
}

impl From<RingOptions> for RuntimeOptions {
    fn from(ring: RingOptions) -> Self {
        Self {
            ring,
            net: NetOptions::default(),
        }
    }
}

/// How the thread's sockets are set up.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NetOptions {
    /// `SO_INCOMING_CPU` set to the thread's CPU on every TCP listener and UDP
    /// socket it binds. In a `SO_REUSEPORT` group the kernel then hands a
    /// connection, or a datagram, to the socket whose CPU received it, and
    /// falls back to its hash when none matches. Each accepted connection is
    /// counted in [`steering_stats`](crate::net::steering_stats). It pays once each worker's CPU takes one NIC queue's
    /// interrupts, which is the system's to configure; with fewer queues than
    /// workers, the workers without one get almost no connections. Needs a
    /// thread pinned to a CPU: a topology's.
    pub steer_connections: bool,
}

/// What this thread's runtime was created with, readable while it runs.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ThreadOptions {
    /// The peer id, `None` outside a topology.
    pub thread_id: Option<usize>,
    /// The CPU the thread is pinned to, `None` outside a topology.
    pub cpu: Option<usize>,
    pub ring: RingOptions,
    pub net: NetOptions,
}

impl ThreadOptions {
    pub(crate) fn new(
        thread_id: Option<usize>,
        cpu: Option<usize>,
        options: RuntimeOptions,
    ) -> Self {
        assert!(
            !options.net.steer_connections || cpu.is_some(),
            "steer_connections needs a thread pinned to a CPU: only a topology's threads are"
        );
        Self {
            thread_id,
            cpu,
            ring: options.ring,
            net: options.net,
        }
    }

    /// The CPU this thread's listeners ask `SO_INCOMING_CPU` for, if it steers.
    pub(crate) fn incoming_cpu(&self) -> Option<usize> {
        self.cpu.filter(|_| self.net.steer_connections)
    }
}

thread_local!(static OPTIONS: Cell<Option<ThreadOptions>> = const { Cell::new(None) });

/// What the runtime running on this thread was created with; `None` outside one.
pub fn thread_options() -> Option<ThreadOptions> {
    OPTIONS.get()
}

/// Holds the thread's options until the runtime returns.
pub(crate) struct Installed(());

impl Drop for Installed {
    fn drop(&mut self) {
        OPTIONS.set(None);
    }
}

/// Publishes `options` for the thread; one runtime at a time per thread.
pub(crate) fn install(options: ThreadOptions) -> Installed {
    assert!(
        OPTIONS.get().is_none(),
        "a runtime is already running on this thread"
    );
    OPTIONS.set(Some(options));
    Installed(())
}
