use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use runtime_core::bytes::Bytes;

use super::{ErrorKind, InvalidUri};

/// The scheme of a URI, e.g. `http`.
#[derive(Clone)]
pub struct Scheme(Inner);

#[derive(Clone)]
enum Inner {
    Http,
    Https,
    Other(Bytes),
}

pub(super) const MAX_SCHEME_LEN: usize = 64;

/// `scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )`, checked loosely like `http` does.
const fn is_scheme_char(b: u8) -> bool {
    matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'+' | b'-' | b'.')
}

impl Scheme {
    pub const HTTP: Scheme = Scheme(Inner::Http);
    pub const HTTPS: Scheme = Scheme(Inner::Https);

    pub fn as_str(&self) -> &str {
        match &self.0 {
            Inner::Http => "http",
            Inner::Https => "https",
            // Validated scheme characters are ASCII.
            Inner::Other(bytes) => unsafe { std::str::from_utf8_unchecked(bytes) },
        }
    }

    /// Parse a whole scheme, without `://`.
    pub(super) fn parse_exact(src: Bytes) -> Result<Scheme, InvalidUri> {
        match &src[..] {
            b"http" => return Ok(Scheme::HTTP),
            b"https" => return Ok(Scheme::HTTPS),
            _ => {}
        }
        if src.len() > MAX_SCHEME_LEN {
            return Err(ErrorKind::SchemeTooLong.into());
        }
        if src.is_empty() || !src.iter().all(|&b| is_scheme_char(b)) {
            return Err(ErrorKind::InvalidScheme.into());
        }
        Ok(Scheme(Inner::Other(src)))
    }

    /// The scheme a URI starts with and the length of its `scheme://` prefix, if it has one.
    pub(super) fn parse_prefix(src: &Bytes) -> Result<Option<(Scheme, usize)>, InvalidUri> {
        if src.len() >= 7 && src[..7].eq_ignore_ascii_case(b"http://") {
            return Ok(Some((Scheme::HTTP, 7)));
        }
        if src.len() >= 8 && src[..8].eq_ignore_ascii_case(b"https://") {
            return Ok(Some((Scheme::HTTPS, 8)));
        }
        let Some(colon) = src.iter().position(|&b| !is_scheme_char(b)) else {
            return Ok(None);
        };
        if colon == 0 || src[colon] != b':' || !src[colon..].starts_with(b"://") {
            return Ok(None);
        }
        if colon > MAX_SCHEME_LEN {
            return Err(ErrorKind::SchemeTooLong.into());
        }
        Ok(Some((Scheme(Inner::Other(src.slice(..colon))), colon + 3)))
    }
}

impl fmt::Debug for Scheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl fmt::Display for Scheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for Scheme {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Case-insensitive.
impl PartialEq for Scheme {
    fn eq(&self, other: &Scheme) -> bool {
        match (&self.0, &other.0) {
            (Inner::Http, Inner::Http) | (Inner::Https, Inner::Https) => true,
            _ => self.as_str().eq_ignore_ascii_case(other.as_str()),
        }
    }
}

impl Eq for Scheme {}

impl PartialEq<str> for Scheme {
    fn eq(&self, other: &str) -> bool {
        self.as_str().eq_ignore_ascii_case(other)
    }
}

impl PartialEq<Scheme> for str {
    fn eq(&self, other: &Scheme) -> bool {
        other == self
    }
}

impl Hash for Scheme {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for b in self.as_str().bytes() {
            state.write_u8(b.to_ascii_lowercase());
        }
    }
}

impl FromStr for Scheme {
    type Err = InvalidUri;

    fn from_str(src: &str) -> Result<Scheme, InvalidUri> {
        Scheme::try_from(src.as_bytes())
    }
}

impl TryFrom<&[u8]> for Scheme {
    type Error = InvalidUri;

    fn try_from(src: &[u8]) -> Result<Scheme, InvalidUri> {
        match src {
            b"http" => Ok(Scheme::HTTP),
            b"https" => Ok(Scheme::HTTPS),
            _ => Scheme::parse_exact(Bytes::copy_from_slice(src)),
        }
    }
}

impl TryFrom<&str> for Scheme {
    type Error = InvalidUri;

    fn try_from(src: &str) -> Result<Scheme, InvalidUri> {
        Scheme::try_from(src.as_bytes())
    }
}

impl TryFrom<Bytes> for Scheme {
    type Error = InvalidUri;

    fn try_from(src: Bytes) -> Result<Scheme, InvalidUri> {
        Scheme::parse_exact(src)
    }
}
