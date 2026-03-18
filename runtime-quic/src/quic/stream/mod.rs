use std::{
    cell::UnsafeCell,
    future::Future,
    mem::MaybeUninit,
    pin::Pin,
    task::{Context, Poll, Waker},
};

use bumpalo::Bump;
use slab::Slab;
use tquic::h3::{Header, NameValue};

use crate::quic::{BodyKind, InvalidRequest, Method, RequestHead};

thread_local! {
    static STREAM_SLAB: UnsafeCell<Slab<StreamState>> = const { UnsafeCell::new(Slab::new()) };
}

#[derive(Debug)]
pub struct AllocBody {
    buf: Box<[MaybeUninit<u8>]>,
    filled: usize,
}

impl AllocBody {
    fn new(cap: usize) -> Self {
        let buf = Box::new_uninit_slice(cap);
        Self { buf, filled: 0 }
    }

    /// Returns the unfilled portion of the buffer as `&mut [u8]` for zero-copy writes.
    /// After writing `n` bytes into the returned slice, call `advance(n)`.
    pub fn unfilled(&mut self) -> &mut [u8] {
        // SAFETY: the region [filled..] is about to be written before any read.
        unsafe {
            let ptr = self.buf[self.filled..].as_mut_ptr() as *mut u8;
            std::slice::from_raw_parts_mut(ptr, self.buf.len() - self.filled)
        }
    }

    pub fn advance(&mut self, n: usize) {
        self.filled += n;
    }

    pub fn filled(&self) -> usize {
        self.filled
    }

    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Consumes the buffer, returning the initialized portion as a `Box<[u8]>`.
    pub fn into_bytes(self) -> Box<[u8]> {
        let filled = self.filled;
        // SAFETY: [0..filled] has been fully written by `write()`.
        unsafe {
            let raw = Box::into_raw(self.buf) as *mut MaybeUninit<u8>;
            let slice = std::slice::from_raw_parts_mut(raw as *mut u8, filled);
            Box::from_raw(slice)
        }
    }
}

#[derive(Debug)]
pub enum BodyType {
    Alloc(AllocBody),
    Streamed,
}

pub struct RequestMeta {
    pub(crate) arena: Box<Bump>,
    pub(crate) method: Method,
    pub(crate) path_ptr: *const str,
    pub(crate) header_ptrs: bumpalo::collections::Vec<'static, (*const str, *const str)>,
    pub(crate) body_kind: BodyKind,
}

impl From<RequestMeta> for RequestHead<'static> {
    fn from(m: RequestMeta) -> Self {
        let path = unsafe { &*m.path_ptr };
        let headers = unsafe {
            let raw: bumpalo::collections::Vec<'static, (&'static str, &'static str)> =
                std::mem::transmute(m.header_ptrs);
            raw
        };
        RequestHead {
            arena: m.arena,
            method: m.method,
            path,
            headers: std::mem::ManuallyDrop::new(headers),
            body_kind: m.body_kind,
        }
    }
}

pub struct StreamState {
    pub(crate) meta: Option<RequestMeta>,
    pub(crate) body: Option<BodyType>,
    pub(crate) fin: bool,
    pub(crate) status: StreamStatus,
    pub(crate) waker: Option<Waker>,
}

impl TryFrom<(&[Header], bool)> for StreamState {
    type Error = InvalidRequest;

    fn try_from((headers, fin): (&[Header], bool)) -> Result<Self, Self::Error> {
        let arena = Box::new(Bump::new());

        let mut method = Method::Get;
        let mut path_ptr: *const str = arena.alloc_str("");
        let mut header_ptrs: bumpalo::collections::Vec<'static, (*const str, *const str)> =
            bumpalo::collections::Vec::new_in(unsafe { &*(&*arena as *const Bump) });
        let mut content_length: Option<usize> = None;

        for h in headers {
            match h.name() {
                b":method" => method = Method::from_bytes(h.value()).unwrap_or(Method::Get),
                b":path" => {
                    path_ptr = arena.alloc_str(std::str::from_utf8(h.value()).unwrap_or(""))
                }
                b"content-length" => {
                    content_length = std::str::from_utf8(h.value())
                        .ok()
                        .and_then(|s| s.parse().ok());
                }
                n if !n.starts_with(b":") => {
                    let name = arena.alloc_str(std::str::from_utf8(h.name()).unwrap_or(""));
                    let value = arena.alloc_str(std::str::from_utf8(h.value()).unwrap_or(""));
                    header_ptrs.push((name, value));
                }
                _ => {}
            }
        }

        let body = match (fin, content_length) {
            (true, None) => None,
            (true, Some(_)) => return Err(InvalidRequest),
            (false, None) => Some(BodyType::Streamed),
            (false, Some(cap)) if cap < 8000 => Some(BodyType::Alloc(AllocBody::new(cap))),
            (false, Some(_)) => Some(BodyType::Streamed),
        };

        let body_kind = match &body {
            None => BodyKind::None,
            Some(BodyType::Alloc(alloc)) => BodyKind::Buffered {
                content_length: alloc.capacity(),
            },
            Some(BodyType::Streamed) => BodyKind::Streamed,
        };

        let status = if body.is_none() {
            StreamStatus::Complete
        } else {
            StreamStatus::WaitingBody
        };

        Ok(StreamState {
            meta: Some(RequestMeta {
                arena,
                method,
                path_ptr,
                header_ptrs,
                body_kind,
            }),
            body,
            fin,
            status,
            waker: None,
        })
    }
}

pub(super) fn take_meta(id: usize) -> Option<RequestMeta> {
    stream_slab(|slab| slab.get_mut(id)?.meta.take())
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StreamStatus {
    /// Body is being received, waiting for Finished event.
    WaitingBody,
    /// Body fully received (or no body).
    Complete,
    /// Connection was lost before the stream finished.
    ConnectionLost,
}

pub fn stream_slab<F, R>(f: F) -> R
where
    F: FnOnce(&mut Slab<StreamState>) -> R,
{
    STREAM_SLAB.with(|cell| f(unsafe { &mut *cell.get() }))
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

/// Resolves once the body is fully received.
/// Returns `None` if the connection was lost before body completion.
pub(super) struct WaitForBody {
    pub id: usize,
}

impl Future for WaitForBody {
    type Output = Result<Option<Box<[u8]>>, ()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        stream_slab(|slab| {
            let Some(state) = slab.get_mut(self.id) else {
                return Poll::Ready(Ok(None));
            };
            match state.status {
                StreamStatus::WaitingBody => {
                    state.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
                StreamStatus::Complete => {
                    let body = match state.body.take() {
                        Some(BodyType::Alloc(alloc)) => alloc.into_bytes(),
                        _ => Box::default(),
                    };
                    Poll::Ready(Ok(Some(body)))
                }
                StreamStatus::ConnectionLost => Poll::Ready(Err(())),
            }
        })
    }
}
