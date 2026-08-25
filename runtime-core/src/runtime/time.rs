use std::{
    cell::UnsafeCell,
    future::Future,
    rc::Rc,
    task::Waker,
    time::{Duration, Instant, SystemTime},
};

#[cfg(feature = "simulation")]
use crate::deterministic_testing::VirtualClock;

use crate::runtime::TIME_DRIVER;
use crate::runtime::select::{SelectResult, select};

/// Wraps a future with a timeout. Returns `Ok(output)` if the future completes
/// before the deadline, or `Err(())` if the timeout fires first.
pub async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output, ()> {
    match select(future, Sleep::during(duration)).await {
        SelectResult::First(output) => Ok(output),
        SelectResult::Second(_) => Err(()),
    }
}

/// Where the timewheel gets the current time.
///
/// Two variants, chosen once when the runtime is built — the same shape as the
/// io_driver's `RingBackend`, and for the same reason: the set is closed, so an
/// enum keeps both arms inlinable instead of hiding them behind a call.
pub enum ClockSource {
    /// The OS clock. Every runtime that is not simulating.
    System,
    /// Advanced by hand by whatever is simulating the world.
    #[cfg(feature = "simulation")]
    Virtual(Rc<VirtualClock>),
}

impl ClockSource {
    #[inline]
    fn now(&self) -> Instant {
        match self {
            Self::System => Instant::now(),
            #[cfg(feature = "simulation")]
            Self::Virtual(clock) => clock.now(),
        }
    }

    #[inline]
    fn now_wall(&self) -> SystemTime {
        match self {
            Self::System => SystemTime::now(),
            #[cfg(feature = "simulation")]
            Self::Virtual(clock) => clock.now_wall(),
        }
    }
}

pub struct TimeDriver {
    inner: UnsafeCell<async_timers::TimeWheel>,
    clock: ClockSource,
}

impl Default for TimeDriver {
    fn default() -> Self {
        Self {
            inner: UnsafeCell::default(),
            clock: ClockSource::System,
        }
    }
}

pub struct Sleep {
    id: Option<usize>,
    duration: Duration,
}

impl Sleep {
    ///can be 10ms late
    pub fn during(duration: Duration) -> Self {
        Self { duration, id: None }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if let Some(id) = self.id {
            let result = TIME_DRIVER.with(|d| d.poll(id, cx.waker()));
            if result.is_ready() {
                self.id = None;
            }
            result
        } else {
            self.id = TIME_DRIVER.with(|d| d.register(self.duration, cx.waker()));
            std::task::Poll::Pending
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(id) = self.id {
            TIME_DRIVER.with(|d| d.drop(id));
        }
    }
}

impl TimeDriver {
    pub fn now(&self) -> SystemTime {
        let this = unsafe { &mut *self.inner.get() };
        this.now()
    }

    /// Drive the wheel with whichever clock this runtime was built on.
    pub fn tick(&self) {
        let this = unsafe { &mut *self.inner.get() };
        this.tick_at(self.clock.now(), self.clock.now_wall());
    }

    /// A driver on a clock a simulation advances rather than the OS.
    #[cfg(feature = "simulation")]
    pub fn simulated(clock: Rc<VirtualClock>) -> Self {
        Self {
            inner: UnsafeCell::default(),
            clock: ClockSource::Virtual(clock),
        }
    }
    fn register(&self, duration: Duration, waker: &Waker) -> Option<usize> {
        let this = unsafe { &mut *self.inner.get() };
        this.init_timer(duration, waker).ok()
    }
    fn drop(&self, id: usize) {
        let this = unsafe { &mut *self.inner.get() };
        this.drop(id);
    }

    fn poll(&self, id: usize, waker: &Waker) -> std::task::Poll<()> {
        let this = unsafe { &mut *self.inner.get() };
        this.poll(id, waker)
    }

    pub fn next_deadline(&self) -> Option<Duration> {
        let this = unsafe { &mut *self.inner.get() };
        match this.next_deadline() {
            Some(Duration::ZERO) => Some(Duration::ZERO),
            other => other,
        }
    }
}
