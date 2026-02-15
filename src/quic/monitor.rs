use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

static CONNECTIONS_OPENED: AtomicUsize = AtomicUsize::new(0);
static CONNECTIONS_CLOSED: AtomicUsize = AtomicUsize::new(0);
static REQUESTS_SERVED: AtomicUsize = AtomicUsize::new(0);

const MAX_THREADS: usize = 128;
static REQUESTS_PER_THREAD: [AtomicUsize; MAX_THREADS] = {
    const ZERO: AtomicUsize = AtomicUsize::new(0);
    [ZERO; MAX_THREADS]
};

pub fn reset() {
    CONNECTIONS_OPENED.store(0, Relaxed);
    CONNECTIONS_CLOSED.store(0, Relaxed);
    REQUESTS_SERVED.store(0, Relaxed);
    for counter in &REQUESTS_PER_THREAD {
        counter.store(0, Relaxed);
    }
}

pub fn connections_opened() -> usize {
    CONNECTIONS_OPENED.load(Relaxed)
}

pub fn connections_closed() -> usize {
    CONNECTIONS_CLOSED.load(Relaxed)
}

pub fn requests_served() -> usize {
    REQUESTS_SERVED.load(Relaxed)
}

pub fn requests_served_by_thread(thread_id: usize) -> usize {
    REQUESTS_PER_THREAD[thread_id].load(Relaxed)
}

pub(super) fn record_connection_opened() {
    CONNECTIONS_OPENED.fetch_add(1, Relaxed);
}

pub(super) fn record_connection_closed() {
    CONNECTIONS_CLOSED.fetch_add(1, Relaxed);
}

pub(super) fn record_request_served() {
    REQUESTS_SERVED.fetch_add(1, Relaxed);
    let tid = crate::runtime::thread_id();
    if tid < MAX_THREADS {
        REQUESTS_PER_THREAD[tid].fetch_add(1, Relaxed);
    }
}
