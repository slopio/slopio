use std::os::fd::RawFd;
use std::sync::Barrier;
use std::sync::atomic::{AtomicI32, Ordering};

/// Per-thread slot in the shared [`PeerTable`]. A single writer (the owning
/// worker) publishes `ring_fd` once, at startup; everyone else only reads.
pub struct PeerSlot {
    /// Wakeup doorbell, created by the parent before any worker starts.
    pub eventfd: RawFd,
    /// The worker's io_uring fd — MSG_RING target. `-1` until the owning
    /// worker has created its ring and called [`PeerTable::publish`].
    ring_fd: AtomicI32,
}

impl PeerSlot {
    fn new(eventfd: RawFd) -> Self {
        Self {
            eventfd,
            ring_fd: AtomicI32::new(-1),
        }
    }

    /// Ring fd of this peer, or `None` if it hasn't published yet (only
    /// possible before the startup barrier).
    pub fn ring_fd(&self) -> Option<RawFd> {
        match self.ring_fd.load(Ordering::Acquire) {
            -1 => None,
            fd => Some(fd),
        }
    }
}

/// Registry shared by every worker: one [`PeerSlot`] per thread plus a
/// one-shot startup barrier. Built by the parent, handed to each
/// [`RuntimeInner`](super::RuntimeInner)'s io_driver as an `Arc`.
///
/// The table is the *same* object on every thread — the only per-thread
/// bit is `thread_id`, which the runtime already tracks — so it lives in a
/// plain shared `Arc` rather than a scoped-thread-local.
pub struct PeerTable {
    slots: Box<[PeerSlot]>,
    ready: Barrier,
}

impl PeerTable {
    /// Build the table from the parent-created eventfds (one per worker).
    /// Ring fds start unset and are filled in by each worker via
    /// [`publish`](Self::publish).
    pub fn new(eventfds: &[RawFd]) -> Self {
        Self {
            slots: eventfds.iter().map(|&fd| PeerSlot::new(fd)).collect(),
            ready: Barrier::new(eventfds.len()),
        }
    }

    pub fn len(&self) -> usize {
        self.slots.len()
    }

    pub fn is_empty(&self) -> bool {
        self.slots.is_empty()
    }

    pub fn slot(&self, thread_id: usize) -> &PeerSlot {
        &self.slots[thread_id]
    }

    /// Called once by worker `thread_id` right after it has created its ring:
    /// publish the ring fd, then rendezvous so that when every worker returns
    /// from here the whole table is filled and visible.
    pub fn publish(&self, thread_id: usize, ring_fd: RawFd) {
        self.slots[thread_id]
            .ring_fd
            .store(ring_fd, Ordering::Release);
        self.ready.wait();
    }
}
