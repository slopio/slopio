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

// A duration the runtime keeps a dedicated store for. One number, not two: the
// constant that routes and the type parameter that sizes the store must agree,
// or a sleep lands in a store dimensioned for something else.
pub const FIXED_5S_MS: u64 = 5_000;
/// The one duration served exactly, outside the wheel.
pub const FIXED_5S: Duration = Duration::from_millis(FIXED_5S_MS);

pub struct TimeDriver {
    /// Any duration up to `MAX_DURATION`, at 10ms resolution.
    wheel: UnsafeCell<async_timers::TimeWheel>,
    /// [`FIXED_5S`] only. One duration per store is what keeps its queues
    /// FIFO — arming later can only end later, so nothing is ever sorted.
    fixed_5s: UnsafeCell<async_timers::Timers<FIXED_5S_MS>>,
    clock: ClockSource,
}

impl Default for TimeDriver {
    fn default() -> Self {
        Self {
            wheel: UnsafeCell::default(),
            fixed_5s: UnsafeCell::default(),
            clock: ClockSource::System,
        }
    }
}

/// Which store a [`Sleep`] is parked in, and under what name there. The wheel
/// hands out slab ids and the fixed stores hand out tokens; the two are not
/// interchangeable, so the store is part of what a `Sleep` remembers.
#[derive(Clone, Copy)]
enum Armed {
    Wheel(usize),
    Fixed5s(async_timers::Token<FIXED_5S_MS>),
}

pub struct Sleep {
    duration: Duration,
    armed: Option<Armed>,
    /// Fired once: every later poll is Ready without re-arming.
    fired: bool,
}

impl Sleep {
    /// Sleeps for `duration`, up to 10ms late.
    ///
    /// Two shapes are served: anything up to one minute, and 5s exactly.
    /// Anything else panics on the first poll — no store can hold it.
    pub fn during(duration: Duration) -> Self {
        Self {
            duration,
            armed: None,
            fired: false,
        }
    }
}

impl Future for Sleep {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        if self.fired {
            return std::task::Poll::Ready(());
        }
        if let Some(armed) = self.armed {
            let result = TIME_DRIVER.with(|d| d.poll(armed, cx.waker()));
            if result.is_ready() {
                self.armed = None;
                self.fired = true;
            }
            result
        } else {
            self.armed = Some(TIME_DRIVER.with(|d| d.arm(self.duration, cx.waker())));
            std::task::Poll::Pending
        }
    }
}

impl Drop for Sleep {
    fn drop(&mut self) {
        if let Some(armed) = self.armed {
            TIME_DRIVER.with(|d| d.cancel(armed));
        }
    }
}

impl TimeDriver {
    pub fn now(&self) -> SystemTime {
        let wheel = unsafe { &*self.wheel.get() };
        wheel.now()
    }

    /// Drive every store with whichever clock this runtime was built on.
    ///
    /// The wheel owns the tick counter and the fixed stores are driven off it,
    /// so everything armed and everything fired reads the same clock — which is
    /// what the deadline rings assume.
    pub fn tick(&self) {
        let wheel = unsafe { &mut *self.wheel.get() };
        wheel.tick_at(self.clock.now(), self.clock.now_wall());
        let now = wheel.ticks();

        let fixed_5s = unsafe { &mut *self.fixed_5s.get() };
        fixed_5s.tick(now);
    }

    /// A driver on a clock a simulation advances rather than the OS.
    #[cfg(feature = "simulation")]
    pub fn simulated(clock: Rc<VirtualClock>) -> Self {
        // Started on the virtual clock: a real `Instant::now()` would sit ahead
        // of it by however long setup took, a gap that changes every run.
        let wheel = async_timers::TimeWheel::new_at(clock.now(), clock.now_wall());
        Self {
            wheel: UnsafeCell::new(wheel),
            fixed_5s: UnsafeCell::default(),
            clock: ClockSource::Virtual(clock),
        }
    }

    /// Pick the store that can hold `duration` and arm it there.
    ///
    /// Fixed durations are matched before the wheel's range, not after: a fixed
    /// value that happens to fit the wheel must still go to its own store.
    fn arm(&self, duration: Duration, waker: &Waker) -> Armed {
        match duration {
            FIXED_5S => {
                // Armed off the wheel's tick counter, which is also what
                // `tick` drives this store with.
                let now = unsafe { &*self.wheel.get() }.ticks();
                let fixed_5s = unsafe { &mut *self.fixed_5s.get() };
                Armed::Fixed5s(fixed_5s.push(waker.clone(), now))
            }
            d if d <= async_timers::MAX_DURATION => {
                let wheel = unsafe { &mut *self.wheel.get() };
                let id = wheel.init_timer(d, waker).expect("within MAX_DURATION");
                Armed::Wheel(id)
            }
            d => unimplemented!(
                "no timer store holds {d:?}: a sleep is up to {:?}, or {FIXED_5S:?} exactly",
                async_timers::MAX_DURATION,
            ),
        }
    }

    fn cancel(&self, armed: Armed) {
        match armed {
            Armed::Wheel(id) => unsafe { &mut *self.wheel.get() }.drop(id),
            Armed::Fixed5s(token) => unsafe { &mut *self.fixed_5s.get() }.cancel(token),
        }
    }

    fn poll(&self, armed: Armed, waker: &Waker) -> std::task::Poll<()> {
        match armed {
            Armed::Wheel(id) => unsafe { &mut *self.wheel.get() }.poll(id, waker),
            Armed::Fixed5s(token) => unsafe { &mut *self.fixed_5s.get() }.poll(token, waker),
        }
    }

    /// The soonest any store has something to do, or None if nothing is armed.
    /// This is what the event loop blocks on when it runs out of work.
    pub fn next_deadline(&self) -> Option<Duration> {
        let wheel = unsafe { &*self.wheel.get() };
        let fixed_5s = unsafe { &*self.fixed_5s.get() };

        match (wheel.next_deadline(), fixed_5s.next_deadline(wheel.ticks())) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }
}
