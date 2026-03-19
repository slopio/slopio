use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Wake, Waker};

pub struct CountingWaker(AtomicUsize);

impl CountingWaker {
    pub fn count(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
}

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

pub fn make_waker() -> (Arc<CountingWaker>, Waker) {
    let counter = Arc::new(CountingWaker(AtomicUsize::new(0)));
    let waker = Waker::from(counter.clone());
    (counter, waker)
}
