//! An HTTP response.

use std::fmt;

use crate::header::{HeaderMap, HeaderName, HeaderValue};
use crate::{Error, Result, StatusCode, Version};

/// A response head and a body of any type.
#[derive(Clone, Default)]
pub struct Response<T> {
    head: Parts,
    body: T,
}

/// The head of a response.
#[derive(Clone, Default)]
pub struct Parts {
    pub status: StatusCode,
    pub version: Version,
    pub headers: HeaderMap,
}

/// Builds a [`Response`]; the first invalid part is reported by [`body`](Builder::body).
#[derive(Debug)]
pub struct Builder {
    head: std::result::Result<Parts, Error>,
}

impl Response<()> {
    pub fn builder() -> Builder {
        Builder::new()
    }
}

impl<T> Response<T> {
    /// A `200 OK` HTTP/1.1 response with no headers.
    pub fn new(body: T) -> Response<T> {
        Response::from_parts(Parts::default(), body)
    }

    pub fn from_parts(head: Parts, body: T) -> Response<T> {
        Response { head, body }
    }

    pub fn into_parts(self) -> (Parts, T) {
        (self.head, self.body)
    }

    pub fn status(&self) -> StatusCode {
        self.head.status
    }

    pub fn status_mut(&mut self) -> &mut StatusCode {
        &mut self.head.status
    }

    pub fn version(&self) -> Version {
        self.head.version
    }

    pub fn version_mut(&mut self) -> &mut Version {
        &mut self.head.version
    }

    pub fn headers(&self) -> &HeaderMap {
        &self.head.headers
    }

    pub fn headers_mut(&mut self) -> &mut HeaderMap {
        &mut self.head.headers
    }

    pub fn body(&self) -> &T {
        &self.body
    }

    pub fn body_mut(&mut self) -> &mut T {
        &mut self.body
    }

    pub fn into_body(self) -> T {
        self.body
    }

    /// The same head with `f` applied to the body.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Response<U> {
        Response {
            head: self.head,
            body: f(self.body),
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Response<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Response")
            .field("status", &self.status())
            .field("version", &self.version())
            .field("headers", self.headers())
            .field("body", self.body())
            .finish()
    }
}

impl fmt::Debug for Parts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Parts")
            .field("status", &self.status)
            .field("version", &self.version)
            .field("headers", &self.headers)
            .finish()
    }
}

impl Builder {
    pub fn new() -> Builder {
        Builder {
            head: Ok(Parts::default()),
        }
    }

    pub fn status<T>(self, status: T) -> Builder
    where
        StatusCode: TryFrom<T>,
        <StatusCode as TryFrom<T>>::Error: Into<Error>,
    {
        self.and_then(|mut head| {
            head.status = StatusCode::try_from(status).map_err(Into::into)?;
            Ok(head)
        })
    }

    pub fn version(self, version: Version) -> Builder {
        self.and_then(|mut head| {
            head.version = version;
            Ok(head)
        })
    }

    /// Append a header; earlier values of the same name stay.
    pub fn header<K, V>(self, key: K, value: V) -> Builder
    where
        HeaderName: TryFrom<K>,
        <HeaderName as TryFrom<K>>::Error: Into<Error>,
        HeaderValue: TryFrom<V>,
        <HeaderValue as TryFrom<V>>::Error: Into<Error>,
    {
        self.and_then(|mut head| {
            let name = HeaderName::try_from(key).map_err(Into::into)?;
            let value = HeaderValue::try_from(value).map_err(Into::into)?;
            head.headers.append(name, value);
            Ok(head)
        })
    }

    pub fn status_ref(&self) -> Option<&StatusCode> {
        self.head.as_ref().ok().map(|head| &head.status)
    }

    pub fn version_ref(&self) -> Option<&Version> {
        self.head.as_ref().ok().map(|head| &head.version)
    }

    pub fn headers_ref(&self) -> Option<&HeaderMap> {
        self.head.as_ref().ok().map(|head| &head.headers)
    }

    /// `None` once a part was invalid.
    pub fn headers_mut(&mut self) -> Option<&mut HeaderMap> {
        self.head.as_mut().ok().map(|head| &mut head.headers)
    }

    pub fn body<T>(self, body: T) -> Result<Response<T>> {
        Ok(Response::from_parts(self.head?, body))
    }

    fn and_then(self, f: impl FnOnce(Parts) -> Result<Parts>) -> Builder {
        Builder {
            head: self.head.and_then(f),
        }
    }
}

impl Default for Builder {
    fn default() -> Builder {
        Builder::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_200_with_no_headers() {
        let mut res = Response::new(());
        assert_eq!(res.status(), StatusCode::OK);
        *res.status_mut() = StatusCode::NOT_FOUND;
        *res.version_mut() = Version::HTTP_2;
        let (parts, ()) = res.into_parts();
        assert_eq!(parts.status, 404);
        assert!(parts.headers.is_empty());
    }

    #[test]
    fn builder() {
        let res = Response::builder()
            .status(201)
            .header("location", "/items/7")
            .body(())
            .unwrap();
        assert_eq!(res.status(), StatusCode::CREATED);
        assert_eq!(res.headers()["location"], "/items/7");
        assert!(Response::builder().status(1000).body(()).is_err());
    }
}
