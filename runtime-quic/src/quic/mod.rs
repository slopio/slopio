use std::{future::Future, mem::ManuallyDrop, pin::Pin};

mod balancer;
mod config;
mod connection;
mod error;
mod handler;
mod server;

pub use balancer::register_socket as register_balancer_socket;
pub use config::Http3ServerConfig;
pub use error::{Http3ServerError, InvalidRequest};
pub use server::H3Server;

#[cfg(feature = "test-monitor")]
pub mod monitor;

pub mod stream;

pub use runtime_http::http::Method;

/// Headers and metadata available immediately when a request arrives,
/// before any body data is read.
pub struct RequestHead<'arena> {
    pub method: Method,
    pub path: &'arena str,
    pub headers: ManuallyDrop<bumpalo::collections::Vec<'arena, (&'arena str, &'arena str)>>,
    pub body_kind: BodyKind,
    arena: Box<bumpalo::Bump>,
}

impl Drop for RequestHead<'_> {
    fn drop(&mut self) {
        // SAFETY: headers borrows from arena, so we must drop headers first.
        // After this, the compiler drops arena normally.
        unsafe { ManuallyDrop::drop(&mut self.headers) };
    }
}

// SAFETY: RequestHead is only ever used on a single thread (the thread-local
// slab that produced it). The arena is owned by this struct and all references
// into it are valid for 'arena which is bounded by the struct's lifetime.
unsafe impl Send for RequestHead<'_> {}

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
        head: &RequestHead<'static>,
    ) -> Pin<Box<dyn Future<Output = Result<(), HttpResponse>> + 'static>>;

    /// Called when the request has no body.
    fn no_body(
        &self,
        head: RequestHead<'static>,
    ) -> Pin<Box<dyn Future<Output = HttpResponse> + 'static>>;
    fn buffered_body(
        &self,
        head: RequestHead<'static>,
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
