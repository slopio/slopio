//! URIs as HTTP uses them: origin form (`/path?q`), absolute form
//! (`https://host/path`), authority form (`host:443`) and `*`.

mod authority;
mod builder;
mod path;
mod scheme;

use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use crate::bytes::Bytes;

pub use authority::{Authority, Port};
pub use builder::Builder;
pub use path::PathAndQuery;
pub use scheme::Scheme;

/// The query index is a `u16`, so a URI stays below 64 KiB.
const MAX_LEN: usize = (u16::MAX - 1) as usize;

/// A request target. Parsed from received bytes, its parts share them.
#[derive(Clone)]
pub struct Uri {
    scheme: Option<Scheme>,
    authority: Option<Authority>,
    path_and_query: PathAndQuery,
}

/// The parts of a [`Uri`], to take apart or put together.
#[derive(Debug, Default)]
pub struct Parts {
    pub scheme: Option<Scheme>,
    pub authority: Option<Authority>,
    pub path_and_query: Option<PathAndQuery>,
}

/// The bytes are not a valid URI or URI component.
#[derive(Debug)]
pub struct InvalidUri(ErrorKind);

/// The parts do not make a valid URI.
#[derive(Debug)]
pub struct InvalidUriParts(InvalidUri);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorKind {
    InvalidUriChar,
    InvalidScheme,
    InvalidAuthority,
    InvalidPort,
    InvalidFormat,
    SchemeMissing,
    AuthorityMissing,
    PathAndQueryMissing,
    TooLong,
    Empty,
    SchemeTooLong,
    PathDoesNotStartWithSlash,
}

impl Uri {
    /// Panics if `src` is not a valid URI.
    pub fn from_static(src: &'static str) -> Uri {
        Uri::from_shared(Bytes::from_static(src.as_bytes())).expect("static str is not a valid URI")
    }

    /// Parse `src` without copying it: every part is a view on it.
    pub fn from_shared(src: Bytes) -> Result<Uri, InvalidUri> {
        if src.len() > MAX_LEN {
            return Err(ErrorKind::TooLong.into());
        }
        match &src[..] {
            [] => return Err(ErrorKind::Empty.into()),
            b"/" => return Ok(Uri::default()),
            [b'/', ..] | b"*" => {
                return Ok(Uri {
                    scheme: None,
                    authority: None,
                    path_and_query: PathAndQuery::from_shared(src)?,
                });
            }
            _ => {}
        }
        parse_full(src)
    }

    pub fn builder() -> Builder {
        Builder::new()
    }

    pub fn from_parts(parts: Parts) -> Result<Uri, InvalidUriParts> {
        if parts.scheme.is_some() {
            if parts.authority.is_none() {
                return Err(ErrorKind::AuthorityMissing.into());
            }
            if parts.path_and_query.is_none() {
                return Err(ErrorKind::PathAndQueryMissing.into());
            }
        } else if parts.authority.is_some() && parts.path_and_query.is_some() {
            return Err(ErrorKind::SchemeMissing.into());
        }
        Ok(Uri {
            scheme: parts.scheme,
            authority: parts.authority,
            path_and_query: parts.path_and_query.unwrap_or(PathAndQuery::empty()),
        })
    }

    pub fn into_parts(self) -> Parts {
        let path_and_query = (!self.path_and_query.is_empty()).then_some(self.path_and_query);
        Parts {
            scheme: self.scheme,
            authority: self.authority,
            path_and_query,
        }
    }

    pub fn path_and_query(&self) -> Option<&PathAndQuery> {
        (!self.path_and_query.is_empty()).then_some(&self.path_and_query)
    }

    /// The path: `/` for an absolute URI without one, `""` in authority form.
    pub fn path(&self) -> &str {
        if self.has_path() {
            self.path_and_query.path()
        } else {
            ""
        }
    }

    pub fn query(&self) -> Option<&str> {
        self.path_and_query.query()
    }

    pub fn scheme(&self) -> Option<&Scheme> {
        self.scheme.as_ref()
    }

    pub fn scheme_str(&self) -> Option<&str> {
        self.scheme.as_ref().map(Scheme::as_str)
    }

    pub fn authority(&self) -> Option<&Authority> {
        self.authority.as_ref()
    }

    pub fn host(&self) -> Option<&str> {
        self.authority.as_ref().map(Authority::host)
    }

    pub fn port(&self) -> Option<Port<&str>> {
        self.authority.as_ref().and_then(Authority::port)
    }

    pub fn port_u16(&self) -> Option<u16> {
        self.port().map(|p| p.as_u16())
    }

    fn has_path(&self) -> bool {
        !self.path_and_query.is_empty() || self.scheme.is_some()
    }
}

/// A URI that does not start with `/`: absolute form or authority form.
fn parse_full(mut src: Bytes) -> Result<Uri, InvalidUri> {
    let scheme = match Scheme::parse_prefix(&src)? {
        Some((scheme, prefix)) => {
            src.advance(prefix);
            Some(scheme)
        }
        None => None,
    };

    let authority_end = Authority::parse(&src)?;
    let Some(scheme) = scheme else {
        // Authority form: nothing may follow the authority.
        if authority_end != src.len() {
            return Err(ErrorKind::InvalidFormat.into());
        }
        return Ok(Uri {
            scheme: None,
            authority: Some(Authority::from_valid(src)),
            path_and_query: PathAndQuery::empty(),
        });
    };

    if authority_end == 0 {
        return Err(ErrorKind::InvalidFormat.into());
    }
    let authority = Authority::from_valid(src.split_to(authority_end));
    let path_and_query = if src.is_empty() {
        PathAndQuery::slash()
    } else {
        PathAndQuery::from_shared(src)?
    };
    Ok(Uri {
        scheme: Some(scheme),
        authority: Some(authority),
        path_and_query,
    })
}

impl Default for Uri {
    fn default() -> Uri {
        Uri {
            scheme: None,
            authority: None,
            path_and_query: PathAndQuery::slash(),
        }
    }
}

impl fmt::Display for Uri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(scheme) = &self.scheme {
            write!(f, "{scheme}://")?;
        }
        if let Some(authority) = &self.authority {
            write!(f, "{authority}")?;
        }
        write!(f, "{}", self.path())?;
        if let Some(query) = self.query() {
            write!(f, "?{query}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for Uri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl PartialEq for Uri {
    fn eq(&self, other: &Uri) -> bool {
        self.scheme == other.scheme
            && self.authority == other.authority
            && self.path() == other.path()
            && self.query() == other.query()
    }
}

impl Eq for Uri {}

/// Scheme and authority compare case-insensitively, path and query exactly.
impl PartialEq<str> for Uri {
    fn eq(&self, other: &str) -> bool {
        let mut rest = other.as_bytes();
        let mut take = |s: &str, ignore_case: bool| {
            let Some((head, tail)) = rest.split_at_checked(s.len()) else {
                return false;
            };
            let same = if ignore_case {
                head.eq_ignore_ascii_case(s.as_bytes())
            } else {
                head == s.as_bytes()
            };
            rest = tail;
            same
        };
        if let Some(scheme) = &self.scheme
            && !(take(scheme.as_str(), true) && take("://", false))
        {
            return false;
        }
        if let Some(authority) = &self.authority
            && !take(authority.as_str(), true)
        {
            return false;
        }
        if !take(self.path(), false) {
            return false;
        }
        if let Some(query) = self.query()
            && !(take("?", false) && take(query, false))
        {
            return false;
        }
        rest.is_empty()
    }
}

impl PartialEq<&str> for Uri {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

impl PartialEq<Uri> for str {
    fn eq(&self, other: &Uri) -> bool {
        other == self
    }
}

impl Hash for Uri {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.scheme.hash(state);
        self.authority.hash(state);
        self.path().hash(state);
        self.query().hash(state);
    }
}

impl FromStr for Uri {
    type Err = InvalidUri;

    fn from_str(src: &str) -> Result<Uri, InvalidUri> {
        Uri::try_from(src.as_bytes())
    }
}

impl TryFrom<&[u8]> for Uri {
    type Error = InvalidUri;

    fn try_from(src: &[u8]) -> Result<Uri, InvalidUri> {
        Uri::from_shared(Bytes::copy_from_slice(src))
    }
}

impl TryFrom<&str> for Uri {
    type Error = InvalidUri;

    fn try_from(src: &str) -> Result<Uri, InvalidUri> {
        Uri::try_from(src.as_bytes())
    }
}

impl TryFrom<&String> for Uri {
    type Error = InvalidUri;

    fn try_from(src: &String) -> Result<Uri, InvalidUri> {
        Uri::try_from(src.as_bytes())
    }
}

impl TryFrom<String> for Uri {
    type Error = InvalidUri;

    fn try_from(src: String) -> Result<Uri, InvalidUri> {
        Uri::try_from(src.as_bytes())
    }
}

impl TryFrom<Bytes> for Uri {
    type Error = InvalidUri;

    fn try_from(src: Bytes) -> Result<Uri, InvalidUri> {
        Uri::from_shared(src)
    }
}

impl TryFrom<Parts> for Uri {
    type Error = InvalidUriParts;

    fn try_from(parts: Parts) -> Result<Uri, InvalidUriParts> {
        Uri::from_parts(parts)
    }
}

impl From<&Uri> for Uri {
    fn from(uri: &Uri) -> Uri {
        uri.clone()
    }
}

impl From<Uri> for Parts {
    fn from(uri: Uri) -> Parts {
        uri.into_parts()
    }
}

impl From<ErrorKind> for InvalidUri {
    fn from(kind: ErrorKind) -> InvalidUri {
        InvalidUri(kind)
    }
}

impl From<ErrorKind> for InvalidUriParts {
    fn from(kind: ErrorKind) -> InvalidUriParts {
        InvalidUriParts(kind.into())
    }
}

impl fmt::Display for InvalidUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.0 {
            ErrorKind::InvalidUriChar => "invalid uri character",
            ErrorKind::InvalidScheme => "invalid scheme",
            ErrorKind::InvalidAuthority => "invalid authority",
            ErrorKind::InvalidPort => "invalid port",
            ErrorKind::InvalidFormat => "invalid format",
            ErrorKind::SchemeMissing => "scheme missing",
            ErrorKind::AuthorityMissing => "authority missing",
            ErrorKind::PathAndQueryMissing => "path missing",
            ErrorKind::TooLong => "uri too long",
            ErrorKind::Empty => "empty string",
            ErrorKind::SchemeTooLong => "scheme too long",
            ErrorKind::PathDoesNotStartWithSlash => "path does not start with '/'",
        })
    }
}

impl std::error::Error for InvalidUri {}

impl fmt::Display for InvalidUriParts {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for InvalidUriParts {}

#[cfg(test)]
mod tests;
