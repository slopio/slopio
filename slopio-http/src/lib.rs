//! HTTP types for the runtime: the `http` crate's API, on [`slopio_core::bytes::Bytes`].
//!
//! Requests, responses, header maps, URIs, methods and status codes look and
//! behave like their `http` 1.x counterparts. Header values and URIs can share
//! received bytes instead of copying them, and nothing is `Send`: each worker
//! keeps its own.

mod chars;
mod error;
pub mod header;
pub mod method;
pub mod request;
pub mod response;
pub mod status;
pub mod uri;
pub mod version;

pub use error::{Error, Result};
pub use header::{HeaderMap, HeaderName, HeaderValue};
pub use method::Method;
pub use request::Request;
pub use response::Response;
pub use status::StatusCode;
pub use uri::Uri;
pub use version::Version;
