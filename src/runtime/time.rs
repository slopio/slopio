use std::{cell::Cell, cell::UnsafeCell, task::Waker, time::Duration};

use crate::runtime::TIME_DRIVER;

pub(crate) struct TimeDriver {
    inner: UnsafeCell<async_timers::TimeWheel>,
    active_count: Cell<usize>,
}

impl Default for TimeDriver {
    fn default() -> Self {
        Self {
            inner: UnsafeCell::new(async_timers::TimeWheel::new()),
            active_count: Cell::new(0),
        }
    }
}

pub struct Sleep {
    id: Option<usize>,
    duration: Duration,
}

impl Sleep {
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
    pub fn tick(&self) {
        let this = unsafe { &mut *self.inner.get() };
        this.tick();
    }
    fn register(&self, duration: Duration, waker: &Waker) -> Option<usize> {
        let this = unsafe { &mut *self.inner.get() };
        let id = this.init_timer(duration, waker).ok();
        if id.is_some() {
            self.active_count.set(self.active_count.get() + 1);
        }
        id
    }
    fn drop(&self, id: usize) {
        let this = unsafe { &mut *self.inner.get() };
        this.drop(id);
        self.active_count
            .set(self.active_count.get().saturating_sub(1));
    }

    fn poll(&self, id: usize, waker: &Waker) -> std::task::Poll<()> {
        let this = unsafe { &mut *self.inner.get() };
        let result = this.poll(id, waker);
        if result.is_ready() {
            self.active_count
                .set(self.active_count.get().saturating_sub(1));
        }
        result
    }

    pub fn next_deadline(&self) -> Option<Duration> {
        let this = unsafe { &mut *self.inner.get() };
        match this.next_deadline() {
            // The underlying TimeWheel returns None both when there are no timers
            // AND when a timer sits in the current bucket (i==0). In the latter
            // case we must not block indefinitely — return a minimal duration so
            // the event loop wakes up and ticks again.
            None if self.active_count.get() > 0 => Some(Duration::from_millis(10)),
            other => other,
        }
    }
}
