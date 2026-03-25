#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
    Patch,
    Head,
    Options,
    Trace,
    Connect,
}

impl Method {
    pub const fn as_str(&self) -> &'static str {
        match self {
            Method::Get => "GET",
            Method::Post => "POST",
            Method::Put => "PUT",
            Method::Delete => "DELETE",
            Method::Patch => "PATCH",
            Method::Head => "HEAD",
            Method::Options => "OPTIONS",
            Method::Trace => "TRACE",
            Method::Connect => "CONNECT",
        }
    }

    pub fn from_bytes(b: &[u8]) -> Option<Self> {
        match b {
            b"GET" => Some(Method::Get),
            b"POST" => Some(Method::Post),
            b"PUT" => Some(Method::Put),
            b"DELETE" => Some(Method::Delete),
            b"PATCH" => Some(Method::Patch),
            b"HEAD" => Some(Method::Head),
            b"OPTIONS" => Some(Method::Options),
            b"TRACE" => Some(Method::Trace),
            b"CONNECT" => Some(Method::Connect),
            _ => None,
        }
    }

    /// Returns true if the method typically carries a request body.
    pub const fn has_body(&self) -> bool {
        matches!(self, Method::Post | Method::Put | Method::Patch)
    }
}

impl std::fmt::Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Method {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "GET" => Ok(Method::Get),
            "POST" => Ok(Method::Post),
            "PUT" => Ok(Method::Put),
            "DELETE" => Ok(Method::Delete),
            "PATCH" => Ok(Method::Patch),
            "HEAD" => Ok(Method::Head),
            "OPTIONS" => Ok(Method::Options),
            "TRACE" => Ok(Method::Trace),
            "CONNECT" => Ok(Method::Connect),
            _ => Err(()),
        }
    }
}

pub trait Handler<H, R, S, E> {
    fn process_request(&self, stream: &mut S) -> impl Future<Output = Result<Option<R>, E>>;
}
