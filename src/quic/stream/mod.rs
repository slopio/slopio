use std::{mem::MaybeUninit, task::Waker};

use bumpalo::Bump;
use tquic::h3::{Header, NameValue};

use crate::quic::Method;

#[derive(Debug)]
pub struct InvalidRequest;

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

pub enum BodyType {
    Alloc(AllocBody),
    Streamed,
}

pub struct StreamState {
    arena: Box<Bump>,
    method: Method,
    path_ptr: *const str,
    header_ptrs: Vec<(*const str, *const str)>,
    body: Option<BodyType>,
    fin_on_headers: bool,
    status: StreamStatus,
    pub waker: Option<Waker>,
}

impl StreamState {
    pub fn status(&self) -> StreamStatus {
        self.status
    }

    pub fn body(&mut self) -> Option<&mut BodyType> {
        self.body.as_mut()
    }

    pub fn set_status(&mut self, status: StreamStatus) {
        self.status = status;
    }
}

impl TryFrom<(&[Header], bool)> for StreamState {
    type Error = InvalidRequest;

    fn try_from((headers, fin): (&[Header], bool)) -> Result<Self, Self::Error> {
        let arena = Box::new(Bump::new());

        let mut method = Method::Get;
        let mut path_ptr: *const str = arena.alloc_str("");
        let mut header_ptrs: Vec<(*const str, *const str)> = Vec::new();
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
                    let name = std::str::from_utf8(h.name()).unwrap_or("");
                    let value = std::str::from_utf8(h.value()).unwrap_or("");
                    header_ptrs.push((arena.alloc_str(name), arena.alloc_str(value)));
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

        Ok(StreamState {
            arena,
            method,
            path_ptr,
            header_ptrs,
            body,
            fin_on_headers: fin,
            status: if fin {
                StreamStatus::Complete
            } else {
                StreamStatus::Waiting
            },
            waker: None,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StreamStatus {
    Waiting,
    Complete,
    ConnectionLost,
}
