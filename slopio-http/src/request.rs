//! An HTTP request.

use std::fmt;

use crate::header::{HeaderMap, HeaderName, HeaderValue};
use crate::{Error, Method, Result, Uri, Version};

/// A request head and a body of any type.
#[derive(Clone, Default)]
pub struct Request<T> {
    head: Parts,
    body: T,
}

/// The head of a request.
#[derive(Clone, Default)]
pub struct Parts {
    pub method: Method,
    pub uri: Uri,
    pub version: Version,
    pub headers: HeaderMap,
}

/// Builds a [`Request`]; the first invalid part is reported by [`body`](Builder::body).
#[derive(Debug)]
pub struct Builder {
    head: std::result::Result<Parts, Error>,
}

macro_rules! method_shortcuts {
    ($($fn:ident => $method:ident),*) => {$(
        #[doc = concat!("A builder for a `", stringify!($method), "` request to `uri`.")]
        pub fn $fn<U>(uri: U) -> Builder
        where
            Uri: TryFrom<U>,
            <Uri as TryFrom<U>>::Error: Into<Error>,
        {
            Builder::new().method(Method::$method).uri(uri)
        }
    )*};
}

impl Request<()> {
    pub fn builder() -> Builder {
        Builder::new()
    }

    method_shortcuts!(
        get => GET,
        put => PUT,
        post => POST,
        delete => DELETE,
        options => OPTIONS,
        head => HEAD,
        connect => CONNECT,
        patch => PATCH,
        trace => TRACE
    );
}

impl<T> Request<T> {
    /// A `GET /` HTTP/1.1 request with no headers.
    pub fn new(body: T) -> Request<T> {
        Request::from_parts(Parts::default(), body)
    }

    pub fn from_parts(head: Parts, body: T) -> Request<T> {
        Request { head, body }
    }

    pub fn into_parts(self) -> (Parts, T) {
        (self.head, self.body)
    }

    pub fn method(&self) -> &Method {
        &self.head.method
    }

    pub fn method_mut(&mut self) -> &mut Method {
        &mut self.head.method
    }

    pub fn uri(&self) -> &Uri {
        &self.head.uri
    }

    pub fn uri_mut(&mut self) -> &mut Uri {
        &mut self.head.uri
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
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> Request<U> {
        Request {
            head: self.head,
            body: f(self.body),
        }
    }
}

impl<T: fmt::Debug> fmt::Debug for Request<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Request")
            .field("method", self.method())
            .field("uri", self.uri())
            .field("version", &self.version())
            .field("headers", self.headers())
            .field("body", self.body())
            .finish()
    }
}

impl fmt::Debug for Parts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Parts")
            .field("method", &self.method)
            .field("uri", &self.uri)
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

    pub fn method<T>(self, method: T) -> Builder
    where
        Method: TryFrom<T>,
        <Method as TryFrom<T>>::Error: Into<Error>,
    {
        self.and_then(|mut head| {
            head.method = Method::try_from(method).map_err(Into::into)?;
            Ok(head)
        })
    }

    pub fn uri<T>(self, uri: T) -> Builder
    where
        Uri: TryFrom<T>,
        <Uri as TryFrom<T>>::Error: Into<Error>,
    {
        self.and_then(|mut head| {
            head.uri = Uri::try_from(uri).map_err(Into::into)?;
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

    pub fn method_ref(&self) -> Option<&Method> {
        self.head.as_ref().ok().map(|head| &head.method)
    }

    pub fn uri_ref(&self) -> Option<&Uri> {
        self.head.as_ref().ok().map(|head| &head.uri)
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

    pub fn body<T>(self, body: T) -> Result<Request<T>> {
        Ok(Request::from_parts(self.head?, body))
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
    use crate::header::CONTENT_TYPE;

    #[test]
    fn builder_collects_every_part() {
        let req = Request::post("https://example.com/upload")
            .header("content-type", "application/json")
            .header(&CONTENT_TYPE, HeaderValue::from_static("text/plain"))
            .version(Version::HTTP_2)
            .body("{}")
            .unwrap();
        assert_eq!(req.method(), Method::POST);
        assert_eq!(req.uri().path(), "/upload");
        assert_eq!(req.version(), Version::HTTP_2);
        assert_eq!(req.headers().get_all(CONTENT_TYPE).iter().count(), 2);
        assert_eq!(*req.body(), "{}");
    }

    #[test]
    fn the_first_invalid_part_is_reported() {
        let err = Request::get("/ok")
            .header("bad name", "v")
            .uri("not a uri")
            .body(())
            .unwrap_err();
        assert!(err.is::<crate::header::InvalidHeaderName>());
    }

    #[test]
    fn uri_can_be_given_owned_or_borrowed() {
        let uri: Uri = "/x".parse().unwrap();
        assert!(Request::get(&uri).body(()).is_ok());
        assert!(Request::get(uri).body(()).is_ok());
        assert!(Request::get(String::from("/y")).body(()).is_ok());
    }

    #[test]
    fn map_keeps_the_head() {
        let req = Request::head("/").body(3).unwrap().map(|n| n * 2);
        assert_eq!(req.method(), Method::HEAD);
        assert_eq!(req.into_body(), 6);
    }
}
