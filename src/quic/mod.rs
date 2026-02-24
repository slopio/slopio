use crate::quic::stream::BodyType;

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

pub struct HttpRequest {
    #[allow(dead_code)]
    arena: Box<bumpalo::Bump>,
    pub method: Method,
    path_ptr: *const str,
    header_ptrs: Vec<(*const str, *const str)>,
    pub body: Option<BodyType>,
}

// SAFETY: HttpRequest is only ever used on a single thread (the thread-local
// slab that produced it). The raw pointers are valid for the lifetime of the
// arena which is owned by this struct.
unsafe impl Send for HttpRequest {}

impl HttpRequest {
    pub fn path(&self) -> &str {
        unsafe { &*self.path_ptr }
    }

    pub fn headers(&self) -> impl Iterator<Item = (&str, &str)> {
        self.header_ptrs.iter().map(|&(k, v)| unsafe { (&*k, &*v) })
    }
}

pub enum HttpBody {
    /// Body data was received and fully buffered.
    Buffered(Vec<u8>),
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
