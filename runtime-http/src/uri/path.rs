use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use runtime_core::bytes::Bytes;

use super::{ErrorKind, InvalidUri, MAX_LEN};

/// The path and query of a URI, e.g. `/search?q=io_uring`. A fragment is dropped.
#[derive(Clone)]
pub struct PathAndQuery {
    data: Bytes,
    /// Index of the `?`, or `NONE`.
    query: u16,
}

const NONE: u16 = u16::MAX;

enum Class {
    Valid,
    Query,
    Fragment,
    High,
    Invalid,
}

/// Path bytes, after the WHATWG path state; `"`, `{` and `}` pass as clients send them raw.
const fn path_class(b: u8) -> Class {
    match b {
        b'?' => Class::Query,
        b'#' => Class::Fragment,
        0x21 | 0x24..=0x3B | 0x3D | 0x40..=0x5F | 0x61..=0x7A | 0x7C | 0x7E => Class::Valid,
        b'"' | b'{' | b'}' => Class::Valid,
        0x80..=0xFF => Class::High,
        _ => Class::Invalid,
    }
}

/// Query bytes, after the WHATWG query state.
const fn query_class(b: u8) -> Class {
    match b {
        b'#' => Class::Fragment,
        0x21 | 0x24..=0x3B | 0x3D | 0x3F..=0x7E => Class::Valid,
        0x80..=0xFF => Class::High,
        _ => Class::Invalid,
    }
}

impl PathAndQuery {
    /// Panics if `src` is not a valid path and query.
    pub fn from_static(src: &'static str) -> PathAndQuery {
        PathAndQuery::from_shared(Bytes::from_static(src.as_bytes()))
            .expect("static str is not a valid path")
    }

    /// Wrap `src` without copying.
    pub fn from_shared(mut src: Bytes) -> Result<PathAndQuery, InvalidUri> {
        if src.is_empty() {
            return Err(ErrorKind::Empty.into());
        }
        if src.len() > MAX_LEN {
            return Err(ErrorKind::TooLong.into());
        }
        if &src[..] == b"*" {
            return Ok(PathAndQuery {
                data: src,
                query: NONE,
            });
        }
        if !matches!(src[0], b'/' | b'?' | b'#') {
            return Err(ErrorKind::PathDoesNotStartWithSlash.into());
        }

        let mut query = NONE;
        let mut fragment = None;
        let mut high = false;
        let mut i = 0;
        while i < src.len() {
            match path_class(src[i]) {
                Class::Valid => {}
                Class::Query => {
                    query = i as u16;
                    i += 1;
                    break;
                }
                Class::Fragment => {
                    fragment = Some(i);
                    break;
                }
                Class::High => high = true,
                Class::Invalid => return Err(ErrorKind::InvalidUriChar.into()),
            }
            i += 1;
        }
        if query != NONE {
            while i < src.len() {
                match query_class(src[i]) {
                    Class::Valid | Class::Query => {}
                    Class::Fragment => {
                        fragment = Some(i);
                        break;
                    }
                    Class::High => high = true,
                    Class::Invalid => return Err(ErrorKind::InvalidUriChar.into()),
                }
                i += 1;
            }
        }

        if let Some(at) = fragment {
            src.truncate(at);
        }
        if high && std::str::from_utf8(&src).is_err() {
            return Err(ErrorKind::InvalidUriChar.into());
        }
        Ok(PathAndQuery { data: src, query })
    }

    pub(super) const fn empty() -> PathAndQuery {
        PathAndQuery {
            data: Bytes::new(),
            query: NONE,
        }
    }

    pub(super) const fn slash() -> PathAndQuery {
        PathAndQuery {
            data: Bytes::from_static(b"/"),
            query: NONE,
        }
    }

    pub(super) fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// The path; `/` when empty.
    pub fn path(&self) -> &str {
        let path = match self.query {
            NONE => self.as_str(),
            q => &self.as_str()[..q as usize],
        };
        if path.is_empty() { "/" } else { path }
    }

    /// The query, without its `?`.
    pub fn query(&self) -> Option<&str> {
        match self.query {
            NONE => None,
            q => Some(&self.as_str()[q as usize + 1..]),
        }
    }

    pub fn as_str(&self) -> &str {
        // Checked UTF-8 at construction.
        unsafe { std::str::from_utf8_unchecked(&self.data) }
    }

    pub fn as_shared(&self) -> &Bytes {
        &self.data
    }
}

impl fmt::Debug for PathAndQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for PathAndQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.as_str() {
            "" => f.write_str("/"),
            s if s.starts_with(['/', '*']) => f.write_str(s),
            s => write!(f, "/{s}"),
        }
    }
}

impl PartialEq for PathAndQuery {
    fn eq(&self, other: &PathAndQuery) -> bool {
        self.data == other.data
    }
}

impl Eq for PathAndQuery {}

impl Hash for PathAndQuery {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.data.hash(state);
    }
}

impl PartialEq<str> for PathAndQuery {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for PathAndQuery {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

impl PartialEq<PathAndQuery> for str {
    fn eq(&self, other: &PathAndQuery) -> bool {
        other == self
    }
}

impl FromStr for PathAndQuery {
    type Err = InvalidUri;

    fn from_str(src: &str) -> Result<PathAndQuery, InvalidUri> {
        PathAndQuery::try_from(src.as_bytes())
    }
}

impl TryFrom<&[u8]> for PathAndQuery {
    type Error = InvalidUri;

    fn try_from(src: &[u8]) -> Result<PathAndQuery, InvalidUri> {
        PathAndQuery::from_shared(Bytes::copy_from_slice(src))
    }
}

impl TryFrom<&str> for PathAndQuery {
    type Error = InvalidUri;

    fn try_from(src: &str) -> Result<PathAndQuery, InvalidUri> {
        PathAndQuery::try_from(src.as_bytes())
    }
}

impl TryFrom<String> for PathAndQuery {
    type Error = InvalidUri;

    fn try_from(src: String) -> Result<PathAndQuery, InvalidUri> {
        PathAndQuery::try_from(src.as_bytes())
    }
}

impl TryFrom<Bytes> for PathAndQuery {
    type Error = InvalidUri;

    fn try_from(src: Bytes) -> Result<PathAndQuery, InvalidUri> {
        PathAndQuery::from_shared(src)
    }
}
