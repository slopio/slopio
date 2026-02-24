use std::{fmt, io};

use tquic::h3::Http3Error;

#[derive(Debug)]
pub enum Http3ServerError {
    Io(io::Error),
    Quic(tquic::Error),
    Http3(Http3Error),
    Tls(String),
}

impl fmt::Display for Http3ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Quic(e) => write!(f, "QUIC error: {e}"),
            Self::Http3(e) => write!(f, "HTTP/3 error: {e}"),
            Self::Tls(e) => write!(f, "TLS error: {e}"),
        }
    }
}

impl From<io::Error> for Http3ServerError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<tquic::Error> for Http3ServerError {
    fn from(e: tquic::Error) -> Self {
        Self::Quic(e)
    }
}

impl From<Http3Error> for Http3ServerError {
    fn from(e: Http3Error) -> Self {
        Self::Http3(e)
    }
}

#[derive(Debug)]
pub struct InvalidRequest;
