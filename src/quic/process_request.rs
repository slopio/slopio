use std::{
    cell::UnsafeCell,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bumpalo::Bump;
use slab::Slab;
use tquic::h3::Header;

use crate::quic::stream::{StreamState, StreamStatus};

use super::{HttpBody, HttpRequest};

thread_local! {
    static STREAM_SLAB: UnsafeCell<Slab<StreamState>> = const { UnsafeCell::new(Slab::new()) };
}

pub fn stream_slab<F, R>(f: F) -> R
where
    F: FnOnce(&mut Slab<StreamState>) -> R,
{
    STREAM_SLAB.with(|cell| f(unsafe { &mut *cell.get() }))
}

pub(super) fn try_insert(headers: &[Header], fin: bool) -> Option<usize> {
    let pr = StreamState::try_from((headers, fin)).ok()?;
    Some(stream_slab(|slab| slab.insert(pr)))
}

pub(super) fn on_finished(id: usize) {
    stream_slab(|slab| {
        let state = &mut slab[id];
        state.set_status(super::stream::StreamStatus::Complete);
        if let Some(w) = state.waker.take() {
            w.wake();
        }
    });
}

pub(super) fn on_connection_lost(id: usize) {
    stream_slab(|slab| {
        let state = &mut slab[id];
        state.set_status(StreamStatus::ConnectionLost);
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
            match state.status() {
                StreamStatus::Complete => {
                    let body = if state.fin_on_headers {
                        None
                    } else {
                        Some(HttpBody::Buffered(std::mem::take(&mut state.buffered_data)))
                    };
                    let arena = std::mem::replace(&mut state.arena, Box::new(Bump::new()));
                    Poll::Ready(Some(HttpRequest {
                        arena,
                        method: state.method,
                        path_ptr: state.path_ptr,
                        header_ptrs: std::mem::take(&mut state.header_ptrs),
                        body,
                    }))
                }
                StreamStatus::ConnectionLost => Poll::Ready(None),
                StreamStatus::Waiting => {
                    state.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }
}
