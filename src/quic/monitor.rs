use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

static CONNECTIONS_OPENED: AtomicUsize = AtomicUsize::new(0);
static CONNECTIONS_CLOSED: AtomicUsize = AtomicUsize::new(0);
static REQUESTS_SERVED: AtomicUsize = AtomicUsize::new(0);

pub fn reset() {
    CONNECTIONS_OPENED.store(0, Relaxed);
    CONNECTIONS_CLOSED.store(0, Relaxed);
    REQUESTS_SERVED.store(0, Relaxed);
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

pub(super) fn record_connection_opened() {
    CONNECTIONS_OPENED.fetch_add(1, Relaxed);
}

pub(super) fn record_connection_closed() {
    CONNECTIONS_CLOSED.fetch_add(1, Relaxed);
}

pub(super) fn record_request_served() {
    REQUESTS_SERVED.fetch_add(1, Relaxed);
}
