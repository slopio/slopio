//! The error of the builders, wrapping whichever part was invalid.

use std::convert::Infallible;
use std::error;
use std::fmt;

use crate::http::header::{InvalidHeaderName, InvalidHeaderValue};
use crate::http::method::InvalidMethod;
use crate::http::status::InvalidStatusCode;
use crate::http::uri::{InvalidUri, InvalidUriParts};

pub type Result<T> = std::result::Result<T, Error>;

/// A part given to a builder was invalid.
pub struct Error {
    inner: Kind,
}

enum Kind {
    StatusCode(InvalidStatusCode),
    Method(InvalidMethod),
    Uri(InvalidUri),
    UriParts(InvalidUriParts),
    HeaderName(InvalidHeaderName),
    HeaderValue(InvalidHeaderValue),
}

impl Error {
    /// The wrapped error is a `T`.
    pub fn is<T: error::Error + 'static>(&self) -> bool {
        self.get_ref().is::<T>()
    }

    pub fn get_ref(&self) -> &(dyn error::Error + 'static) {
        match &self.inner {
            Kind::StatusCode(e) => e,
            Kind::Method(e) => e,
            Kind::Uri(e) => e,
            Kind::UriParts(e) => e,
            Kind::HeaderName(e) => e,
            Kind::HeaderValue(e) => e,
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Error").field(&self.get_ref()).finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.get_ref(), f)
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.get_ref().source()
    }
}

macro_rules! from_errors {
    ($($ty:ty => $kind:ident),*) => {$(
        impl From<$ty> for Error {
            fn from(e: $ty) -> Error {
                Error { inner: Kind::$kind(e) }
            }
        }
    )*};
}

from_errors!(
    InvalidStatusCode => StatusCode,
    InvalidMethod => Method,
    InvalidUri => Uri,
    InvalidUriParts => UriParts,
    InvalidHeaderName => HeaderName,
    InvalidHeaderValue => HeaderValue
);

impl From<Infallible> for Error {
    fn from(never: Infallible) -> Error {
        match never {}
    }
}
