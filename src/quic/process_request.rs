use std::{
    cell::UnsafeCell,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use slab::Slab;
use tquic::h3::{Header, NameValue};

use super::HttpRequest;

thread_local! {
    static STREAM_SLAB: UnsafeCell<Slab<StreamState>> = const { UnsafeCell::new(Slab::new()) };
}

fn stream_slab<F, R>(f: F) -> R
where
    F: FnOnce(&mut Slab<StreamState>) -> R,
{
    STREAM_SLAB.with(|cell| f(unsafe { &mut *cell.get() }))
}

pub(super) fn insert(headers: &[Header], fin: bool) -> usize {
    let mut state = StreamState {
        method: String::new(),
        path: String::new(),
        headers: Vec::new(),
        body: Vec::new(),
        status: if fin {
            StreamStatus::Complete
        } else {
            StreamStatus::Waiting
        },
        waker: None,
    };

    for h in headers {
        let name = String::from_utf8_lossy(h.name()).to_string();
        let value = String::from_utf8_lossy(h.value()).to_string();
        match name.as_str() {
            ":method" => state.method = value,
            ":path" => state.path = value,
            _ if !name.starts_with(':') => {
                state.headers.push((name, value));
            }
            _ => {}
        }
    }

    stream_slab(|slab| slab.insert(state))
}

pub(super) fn on_data(id: usize, chunk: &[u8], max_size: usize) -> bool {
    stream_slab(|slab| {
        let state = &mut slab[id];
        if state.body.len() + chunk.len() > max_size {
            return false;
        }
        state.body.extend_from_slice(chunk);
        true
    })
}

pub(super) fn on_finished(id: usize) {
    stream_slab(|slab| {
        let state = &mut slab[id];
        state.status = StreamStatus::Complete;
        if let Some(w) = state.waker.take() {
            w.wake();
        }
    });
}

pub(super) fn on_connection_lost(id: usize) {
    stream_slab(|slab| {
        let state = &mut slab[id];
        state.status = StreamStatus::ConnectionLost;
        if let Some(w) = state.waker.take() {
            w.wake();
        }
    });
}

pub(super) fn remove(id: usize) {
    stream_slab(|slab| {
        slab.remove(id);
    });
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamStatus {
    Waiting,
    Complete,
    ConnectionLost,
}

struct StreamState {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    status: StreamStatus,
    waker: Option<Waker>,
}

pub(super) struct Http3Request {
    id: usize,
}

impl Http3Request {
    pub fn new(id: usize) -> Self {
        Self { id }
    }
}

impl Future for Http3Request {
    type Output = Option<HttpRequest>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        stream_slab(|slab| {
            let state = &mut slab[self.id];
            match state.status {
                StreamStatus::Complete => Poll::Ready(Some(HttpRequest {
                    method: std::mem::take(&mut state.method),
                    path: std::mem::take(&mut state.path),
                    headers: std::mem::take(&mut state.headers),
                    body: std::mem::take(&mut state.body),
                })),
                StreamStatus::ConnectionLost => Poll::Ready(None),
                StreamStatus::Waiting => {
                    state.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }
}
