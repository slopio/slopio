//! A clock a simulation advances by hand.
//!
//! It hands out real [`Instant`]s and [`SystemTime`]s, offset from a base taken
//! at construction, so everything downstream keeps its types and learns
//! nothing. Simulated time is just an origin plus an elapsed duration a test
//! controls, rather than one the OS does.

use std::cell::Cell;
use std::time::{Duration, Instant, SystemTime};

use super::clock::{self, Nanos};

pub struct VirtualClock {
    base: Instant,
    base_wall: SystemTime,
    /// The same instant as `base`, read raw.
    ///
    /// Two representations are needed because the simulated ring schedules in
    /// nanoseconds while the timewheel takes `Instant`s. Both read
    /// `CLOCK_MONOTONIC`, so fixing the bases together keeps them in step for
    /// as long as they advance together.
    base_nanos: Nanos,
    elapsed: Cell<Duration>,
}

impl Default for VirtualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl VirtualClock {
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
            base_wall: SystemTime::now(),
            base_nanos: clock::now(),
            elapsed: Cell::new(Duration::ZERO),
        }
    }

    pub fn now(&self) -> Instant {
        self.base + self.elapsed.get()
    }

    pub fn now_wall(&self) -> SystemTime {
        self.base_wall + self.elapsed.get()
    }

    /// The same instant as [`now`](Self::now), raw.
    pub fn now_nanos(&self) -> Nanos {
        self.base_nanos + self.elapsed.get().as_nanos() as Nanos
    }

    /// How far the clock has been advanced since it started.
    pub fn elapsed(&self) -> Duration {
        self.elapsed.get()
    }

    /// Move the clock forward by `step`.
    pub fn advance(&self, step: Duration) {
        self.elapsed.set(self.elapsed.get() + step);
    }

    /// Move the clock to `deadline`, if that is ahead of where it is.
    ///
    /// Never backwards: a simulation jumping to the earliest pending deadline
    /// would otherwise rewind whenever something nearer was scheduled after
    /// the jump, and every ordering built on due times would come apart.
    pub fn advance_to(&self, deadline: Instant) {
        if let Some(ahead) = deadline.checked_duration_since(self.now()) {
            self.advance(ahead);
        }
    }

    /// [`advance_to`](Self::advance_to), in raw nanoseconds.
    pub fn advance_to_nanos(&self, deadline: Nanos) {
        let now = self.now_nanos();
        if deadline > now {
            self.advance(Duration::from_nanos(deadline - now));
        }
    }
}
