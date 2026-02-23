pub struct Request<'arena> {
    pub method: Method,
    pub path: &'arena str,
    pub headers: &'arena [(&'arena str, &'arena [u8])],
    pub body: Vec<u8>,
}

#[derive(Debug)]
pub enum ParseError {
    UnexpectedEof,
    HeadersTooLarge,
    LineTooLong,
    InvalidRequest(httparse::Error),
    MissingField(&'static str),
    InvalidHeaderName,
    UnknownMethod,
    BodyTooLarge,
    ConflictingContentLength,
    UnsupportedTransferEncoding,
    UnsupportedVersion,
    Io(std::io::Error),
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ParseError::UnexpectedEof => write!(f, "connection closed before request was complete"),
            ParseError::HeadersTooLarge => write!(f, "request headers exceeded size limit"),
            ParseError::LineTooLong => write!(f, "request line or header line too long"),
            ParseError::InvalidRequest(e) => write!(f, "malformed request: {e}"),
            ParseError::MissingField(field) => write!(f, "missing required field: {field}"),
            ParseError::BodyTooLarge => write!(f, "request body exceeds size limit"),
            ParseError::ConflictingContentLength => {
                write!(f, "conflicting Content-Length headers")
            }
            ParseError::UnsupportedTransferEncoding => {
                write!(f, "unsupported Transfer-Encoding")
            }
            ParseError::InvalidHeaderName => write!(f, "invalid character in header name"),
            ParseError::UnsupportedVersion => write!(f, "unsupported HTTP version"),
            ParseError::UnknownMethod => write!(f, "unknown HTTP method"),
            ParseError::Io(e) => write!(f, "I/O error: {e}"),
        }
    }
}

impl std::error::Error for ParseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ParseError::InvalidRequest(e) => Some(e),
            ParseError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ParseError {
    fn from(e: std::io::Error) -> Self {
        ParseError::Io(e)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

impl std::fmt::Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Method::Get => f.write_str("GET"),
            Method::Post => f.write_str("POST"),
            Method::Put => f.write_str("PUT"),
            Method::Delete => f.write_str("DELETE"),
            Method::Patch => f.write_str("PATCH"),
            Method::Head => f.write_str("HEAD"),
            Method::Options => f.write_str("OPTIONS"),
            Method::Trace => f.write_str("TRACE"),
            Method::Connect => f.write_str("CONNECT"),
        }
    }
}

impl Method {
    pub(crate) fn from_str(s: &str) -> Result<Self, ParseError> {
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
            _ => Err(ParseError::UnknownMethod),
        }
    }
}
