use std::{
    cell::UnsafeCell,
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use slab::Slab;
use tquic::h3::{Header, NameValue};

use super::Request;

thread_local! {
    static STREAM_SLAB: UnsafeCell<Slab<StreamState>> = UnsafeCell::new(Slab::new());
}

fn with_slab<F, R>(f: F) -> R
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
        complete: fin,
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

    with_slab(|slab| slab.insert(state))
}

pub(super) fn on_data(id: usize, chunk: &[u8]) {
    with_slab(|slab| {
        slab[id].body.extend_from_slice(chunk);
    });
}

pub(super) fn on_finished(id: usize) {
    with_slab(|slab| {
        let state = &mut slab[id];
        state.complete = true;
        if let Some(w) = state.waker.take() {
            w.wake();
        }
    });
}

pub(super) fn remove(id: usize) {
    with_slab(|slab| {
        slab.remove(id);
    });
}

struct StreamState {
    method: String,
    path: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    complete: bool,
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
    type Output = Request;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        with_slab(|slab| {
            let state = &mut slab[self.id];
            if state.complete {
                Poll::Ready(Request {
                    method: std::mem::take(&mut state.method),
                    path: std::mem::take(&mut state.path),
                    headers: std::mem::take(&mut state.headers),
                    body: std::mem::take(&mut state.body),
                })
            } else {
                state.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        })
    }
}
