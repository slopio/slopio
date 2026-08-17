//! Simulated io_uring, for tests that need to choose what the kernel does.
//!
//! Nothing here performs IO. A [`FakeRing`] accepts the same SQEs the real
//! driver submits, decodes them into a readable [`Sqe`], and schedules them on
//! a seeded timing [`Wheel`] that decides when each one completes. The driver
//! above it — slabs, wakers, the multishot state machine, buffer recycling —
//! runs completely unmodified, so what a test exercises is the real thing,
//! driven by a kernel it controls.
//!
//! Time is real: the wheel sleeps on `CLOCK_MONOTONIC` rather than inventing a
//! virtual clock. That is what lets the runtime's own timewheel keep working
//! untouched — it reads the same clock, and the simulation makes the time it
//! believes in actually pass.
//!
//! This module is deliberately separate from [`io_driver`](crate::io_driver):
//! nothing simulator-shaped lives in the driver. The one place they meet is the
//! driver's `RingBackend` enum, which names [`FakeRing`] as the alternative to a
//! real io_uring.
//!
//! [`Wheel`]: wheel::Wheel

mod clock;
pub mod fake_ring;
pub mod sqe;
pub mod wheel;

pub use fake_ring::{Completion, FakeRing, FakeRingBuilder, FakeRingHandle, SIMULATED_RING_FD};
pub use sqe::{IOSQE_BUFFER_SELECT, Sqe};
pub use wheel::{BUCKETS, Outcome};
