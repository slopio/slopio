//! Simulated io_uring, for tests that need to choose what the kernel does.
//!
//! Nothing here performs IO. A [`FakeRing`] accepts the same SQEs the real
//! driver submits, decodes them into a readable [`Sqe`], and schedules them on
//! a [`Timeline`]. The driver above it — slabs, wakers, the
//! multishot state machine, buffer recycling — runs unmodified, so a test
//! exercises the real thing against a kernel it controls.
//!
//! Time is either real or virtual, chosen on the builder. By default the
//! timeline sleeps on `CLOCK_MONOTONIC`. With
//! [`virtual_time`](FakeRingBuilder::virtual_time) it advances a
//! [`VirtualClock`] instead, which the runtime's timer stores read too, so a
//! wait is a jump rather than a sleep and both sides stay on one clock.
//!
//! Nothing simulator-shaped lives in [`io_driver`](crate::io_driver); they meet
//! at its `RingBackend` enum, and at its `sys` module for the syscalls made
//! outside the ring on a descriptor from [`fds`].
//!
//! [`Timeline`]: timeline::Timeline

pub mod actor;
pub(crate) mod clock;
pub mod dialogue;
pub mod fake_ring;
pub mod fds;
pub mod scenario;
pub mod sqe;
pub mod timeline;
pub mod virtual_clock;

pub use actor::{Actor, ActorCx, ReadReply, WriteReply};
pub use dialogue::{Check, Dialogue, DialogueReport, Fault, Faults};
pub use fake_ring::{
    Completion, FakeRing, FakeRingBuilder, FakeRingHandle, Peer, SIMULATED_RING_FD, trigger_for,
};
pub use fds::{
    FIRST_SIMULATED_FD, FdKind, is_open, is_simulated, mint, open_fds, simulated_socket,
};
pub use scenario::{
    AcceptEvent, ConnectEvent, Cut, Delay, Hiccup, ReadEvent, Scripted, Trigger, WriteEvent,
};
pub use sqe::{IOSQE_BUFFER_SELECT, Sqe};
pub use timeline::Outcome;
pub use virtual_clock::VirtualClock;
