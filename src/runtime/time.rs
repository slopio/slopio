use std::{cell::UnsafeCell, task::Waker, time::Duration};

use crate::runtime::TIME_DRIVER;

#[derive(Default)]
pub(crate) struct TimeDriver {
    inner: UnsafeCell<async_timers::TimeWheel>,
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
            TIME_DRIVER.with(|d| d.poll(id, cx.waker()))
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
        this.init_timer(duration, waker).ok()
    }
    fn drop(&self, id: usize) {
        let this = unsafe { &mut *self.inner.get() };
        this.drop(id);
    }

    fn poll(&self, id: usize, waker: &Waker) -> std::task::Poll<()> {
        let this = unsafe { &mut *self.inner.get() };
        dbg!(this.poll(id, waker))
    }

    pub fn next_deadline(&self) -> Option<Duration> {
        let this = unsafe { &mut *self.inner.get() };
        this.next_deadline()
    }
}
