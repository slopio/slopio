use std::{future::Future, pin::Pin};

mod balancer;
mod config;
mod error;
mod server;

pub use balancer::register_socket as register_balancer_socket;
pub use config::Http3ServerConfig;
pub use error::{Http3ServerError, InvalidRequest};
pub use server::H3Server;

#[cfg(feature = "test-monitor")]
pub mod monitor;

pub mod stream;

pub use crate::http::Method;

/// Headers and metadata available immediately when a request arrives,
/// before any body data is read.
pub struct RequestHead {
    #[allow(dead_code)]
    arena: Box<bumpalo::Bump>,
    pub method: Method,
    path_ptr: *const str,
    header_ptrs: Vec<(*const str, *const str)>,
    pub body_kind: BodyKind,
}

// SAFETY: RequestHead is only ever used on a single thread (the thread-local
// slab that produced it). The raw pointers are valid for the lifetime of the
// arena which is owned by this struct.
unsafe impl Send for RequestHead {}

impl RequestHead {
    pub fn path(&self) -> &str {
        unsafe { &*self.path_ptr }
    }

    pub fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.header_ptrs.iter().map(|&(k, v)| unsafe { (&*k, &*v) })
    }
}

/// Describes what body (if any) will follow the headers.
pub enum BodyKind {
    /// No body — request is complete (e.g. GET, or fin=true on headers).
    None,
    /// Body will be buffered in full before the handler is called.
    Buffered { content_length: usize },
    /// Body is streamed — not yet implemented.
    Streamed,
}

pub trait HttpHandler: 'static {
    /// Called immediately when headers arrive.
    /// Return `Err(response)` to reject early without reading the body.
    fn on_headers(
        &self,
        head: &RequestHead,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpResponse>> + 'static>>;

    /// Called when the request has no body.
    fn no_body(&self, head: RequestHead) -> Pin<Box<dyn Future<Output = HttpResponse> + 'static>>;

    /// Called when the full body has been buffered.
    fn buffered_body(
        &self,
        head: RequestHead,
        body: Box<[u8]>,
    ) -> Pin<Box<dyn Future<Output = HttpResponse> + 'static>>;
}

pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn new(status: u16) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: Vec::new(),
        }
    }

    pub fn header(mut self, name: &str, value: &str) -> Self {
        self.headers.push((name.to_string(), value.to_string()));
        self
    }

    pub fn body(mut self, body: impl Into<Vec<u8>>) -> Self {
        self.body = body.into();
        self
    }
}
