//! Simulated io_uring, for tests that need to choose what the kernel does.
//!
//! Nothing here performs IO. A [`FakeRing`] accepts the same SQEs the real
//! driver submits, decodes them into a readable [`Sqe`], and schedules them on
//! a [`Timeline`]. The driver above it — slabs, wakers, the
//! multishot state machine, buffer recycling — runs unmodified, so a test
//! exercises the real thing against a kernel it controls.
//!
//! Time is real: the timeline sleeps on `CLOCK_MONOTONIC` rather than inventing a
//! virtual clock. That is what lets the runtime's own timewheel keep working
//! untouched — it reads the same clock, and the simulation makes the time it
//! believes in actually pass.
//!
//! Nothing simulator-shaped lives in [`io_driver`](crate::io_driver); they meet
//! at its `RingBackend` enum, and at its `sys` module for the syscalls made
//! outside the ring on a descriptor from [`fds`].
//!
//! [`Timeline`]: timeline::Timeline

pub(crate) mod clock;
pub mod fake_ring;
pub mod fds;
pub mod scenario;
pub mod sqe;
pub mod timeline;
pub mod virtual_clock;

pub use fake_ring::{
    Completion, FakeRing, FakeRingBuilder, FakeRingHandle, SIMULATED_RING_FD, trigger_for,
};
pub use fds::{FIRST_SIMULATED_FD, FdKind, is_open, is_simulated, mint, simulated_socket};
pub use scenario::{AcceptEvent, ConnectEvent, Delay, ReadEvent, Scripted, Trigger, WriteEvent};
pub use sqe::{IOSQE_BUFFER_SELECT, Sqe};
pub use timeline::Outcome;
pub use virtual_clock::VirtualClock;
