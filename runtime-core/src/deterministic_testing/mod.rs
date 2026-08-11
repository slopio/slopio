//! Simulated io_uring, for tests that need to choose what the kernel does.
//!
//! Nothing here performs IO. A [`FakeRing`] accepts the same SQEs the real
//! driver submits, decodes them into a readable [`Sqe`], and holds them until a
//! test says how they finish. The driver above it — slabs, wakers, the
//! multishot state machine, buffer recycling — runs completely unmodified, so
//! what a test exercises is the real thing, driven by a kernel it controls.
//!
//! This module is deliberately separate from [`io_driver`](crate::io_driver):
//! nothing simulator-shaped lives in the driver. The one place they meet is the
//! driver's `RingBackend` enum, which names [`FakeRing`] as the alternative to a
//! real io_uring.

pub mod fake_ring;
pub mod sqe;

pub use fake_ring::{Completion, FakeRing, FakeRingHandle, SIMULATED_RING_FD};
pub use sqe::{IOSQE_BUFFER_SELECT, Sqe};
