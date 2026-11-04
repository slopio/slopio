//! The request method.

use std::fmt;
use std::str::FromStr;

use crate::http::chars::is_tchar;

/// An HTTP method: one of the standard ones, or any valid token.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct Method(Inner);

#[derive(Clone, PartialEq, Eq, Hash)]
enum Inner {
    Options,
    Get,
    Post,
    Put,
    Delete,
    Head,
    Trace,
    Connect,
    Patch,
    Query,
    Extension(Box<str>),
}

/// The bytes are not a valid method token.
#[derive(Debug)]
pub struct InvalidMethod(());

impl Method {
    pub const OPTIONS: Method = Method(Inner::Options);
    pub const GET: Method = Method(Inner::Get);
    pub const POST: Method = Method(Inner::Post);
    pub const PUT: Method = Method(Inner::Put);
    pub const DELETE: Method = Method(Inner::Delete);
    pub const HEAD: Method = Method(Inner::Head);
    pub const TRACE: Method = Method(Inner::Trace);
    pub const CONNECT: Method = Method(Inner::Connect);
    pub const PATCH: Method = Method(Inner::Patch);
    pub const QUERY: Method = Method(Inner::Query);

    /// Parse a method; anything but the standard methods is kept as an extension.
    pub fn from_bytes(src: &[u8]) -> Result<Method, InvalidMethod> {
        let inner = match src {
            b"GET" => Inner::Get,
            b"POST" => Inner::Post,
            b"PUT" => Inner::Put,
            b"DELETE" => Inner::Delete,
            b"HEAD" => Inner::Head,
            b"OPTIONS" => Inner::Options,
            b"CONNECT" => Inner::Connect,
            b"PATCH" => Inner::Patch,
            b"TRACE" => Inner::Trace,
            b"QUERY" => Inner::Query,
            _ if !src.is_empty() && src.iter().all(|&b| is_tchar(b)) => {
                // tchar is ASCII, so the bytes are UTF-8.
                Inner::Extension(std::str::from_utf8(src).unwrap().into())
            }
            _ => return Err(InvalidMethod(())),
        };
        Ok(Method(inner))
    }

    /// Safe methods do not change server state (RFC 9110 §9.2.1).
    pub fn is_safe(&self) -> bool {
        matches!(
            self.0,
            Inner::Get | Inner::Head | Inner::Options | Inner::Trace | Inner::Query
        )
    }

    /// Repeating the request has the same effect as sending it once (RFC 9110 §9.2.2).
    pub fn is_idempotent(&self) -> bool {
        self.is_safe() || matches!(self.0, Inner::Put | Inner::Delete)
    }

    pub fn as_str(&self) -> &str {
        match &self.0 {
            Inner::Options => "OPTIONS",
            Inner::Get => "GET",
            Inner::Post => "POST",
            Inner::Put => "PUT",
            Inner::Delete => "DELETE",
            Inner::Head => "HEAD",
            Inner::Trace => "TRACE",
            Inner::Connect => "CONNECT",
            Inner::Patch => "PATCH",
            Inner::Query => "QUERY",
            Inner::Extension(name) => name,
        }
    }
}

impl Default for Method {
    fn default() -> Method {
        Method::GET
    }
}

impl AsRef<str> for Method {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Ord for Method {
    fn cmp(&self, other: &Method) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialOrd for Method {
    fn partial_cmp(&self, other: &Method) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq<&Method> for Method {
    fn eq(&self, other: &&Method) -> bool {
        self == *other
    }
}

impl PartialEq<Method> for &Method {
    fn eq(&self, other: &Method) -> bool {
        *self == other
    }
}

impl PartialEq<str> for Method {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for Method {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<Method> for str {
    fn eq(&self, other: &Method) -> bool {
        self == other.as_str()
    }
}

impl PartialEq<Method> for &str {
    fn eq(&self, other: &Method) -> bool {
        *self == other.as_str()
    }
}

impl fmt::Debug for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for Method {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&Method> for Method {
    fn from(method: &Method) -> Method {
        method.clone()
    }
}

impl TryFrom<&[u8]> for Method {
    type Error = InvalidMethod;

    fn try_from(src: &[u8]) -> Result<Method, InvalidMethod> {
        Method::from_bytes(src)
    }
}

impl TryFrom<&str> for Method {
    type Error = InvalidMethod;

    fn try_from(src: &str) -> Result<Method, InvalidMethod> {
        Method::from_bytes(src.as_bytes())
    }
}

impl FromStr for Method {
    type Err = InvalidMethod;

    fn from_str(src: &str) -> Result<Method, InvalidMethod> {
        Method::from_bytes(src.as_bytes())
    }
}

impl fmt::Display for InvalidMethod {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid HTTP method")
    }
}

impl std::error::Error for InvalidMethod {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_methods_parse_to_their_constants() {
        assert_eq!(Method::from_bytes(b"GET").unwrap(), Method::GET);
        assert_eq!(Method::from_bytes(b"QUERY").unwrap(), Method::QUERY);
        assert_eq!(Method::POST, "POST");
    }

    #[test]
    fn extensions_are_tokens() {
        let m = Method::from_bytes(b"PROPFIND").unwrap();
        assert_eq!(m.as_str(), "PROPFIND");
        assert!(!m.is_safe());
        assert!(Method::from_bytes(b"").is_err());
        assert!(Method::from_bytes(b"GE T").is_err());
        assert!(Method::from_bytes("MÉTHODE".as_bytes()).is_err());
    }

    #[test]
    fn methods_are_case_sensitive() {
        assert_ne!(Method::from_bytes(b"get").unwrap(), Method::GET);
    }

    #[test]
    fn idempotence() {
        assert!(Method::PUT.is_idempotent());
        assert!(!Method::POST.is_idempotent());
        assert!(!Method::PATCH.is_idempotent());
    }
}
