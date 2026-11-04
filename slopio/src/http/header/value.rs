use std::cmp::Ordering;
use std::fmt::{self, Write};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use crate::bytes::{Bytes, BytesMut};

use super::HeaderName;
use crate::http::chars::{is_value_byte, is_visible_ascii};

/// A header field value: any bytes but CR, LF, NUL and DEL.
///
/// Built from received bytes with [`from_shared`](Self::from_shared), it shares
/// them instead of copying.
#[derive(Clone)]
pub struct HeaderValue {
    inner: Bytes,
    is_sensitive: bool,
}

/// The bytes contain CR, LF, NUL or another control byte.
#[derive(Debug)]
pub struct InvalidHeaderValue(());

/// The value holds bytes outside visible ASCII, so it is not a `str`.
#[derive(Debug)]
pub struct ToStrError(());

impl HeaderValue {
    /// A value from a constant; usable in `const`.
    ///
    /// Panics (at compile time in a `const`) if `src` holds a forbidden byte.
    pub const fn from_static(src: &'static str) -> HeaderValue {
        let bytes = src.as_bytes();
        let mut i = 0;
        while i < bytes.len() {
            if !is_visible_ascii(bytes[i]) {
                panic!("HeaderValue::from_static with invalid bytes");
            }
            i += 1;
        }
        HeaderValue {
            inner: Bytes::from_static(bytes),
            is_sensitive: false,
        }
    }

    /// Copy `src` into a value. Only visible ASCII, space and tab are allowed.
    #[allow(clippy::should_implement_trait)]
    pub fn from_str(src: &str) -> Result<HeaderValue, InvalidHeaderValue> {
        if !src.bytes().all(is_visible_ascii) {
            return Err(InvalidHeaderValue(()));
        }
        Ok(HeaderValue::copied(src.as_bytes()))
    }

    /// Copy `src` into a value.
    pub fn from_bytes(src: &[u8]) -> Result<HeaderValue, InvalidHeaderValue> {
        validate(src)?;
        Ok(HeaderValue::copied(src))
    }

    /// Wrap `src` without copying.
    pub fn from_shared(src: Bytes) -> Result<HeaderValue, InvalidHeaderValue> {
        validate(&src)?;
        Ok(HeaderValue {
            inner: src,
            is_sensitive: false,
        })
    }

    /// Wrap `src` without copying or checking it.
    ///
    /// # Safety
    /// `src` holds no byte [`from_bytes`](Self::from_bytes) would reject.
    pub unsafe fn from_shared_unchecked(src: Bytes) -> HeaderValue {
        debug_assert!(
            validate(&src).is_ok(),
            "from_shared_unchecked with invalid bytes"
        );
        HeaderValue {
            inner: src,
            is_sensitive: false,
        }
    }

    /// The name's text as a value, e.g. for `access-control-allow-headers`.
    pub fn from_name(name: HeaderName) -> HeaderValue {
        name.into()
    }

    fn copied(src: &[u8]) -> HeaderValue {
        HeaderValue {
            inner: Bytes::copy_from_slice(src),
            is_sensitive: false,
        }
    }

    /// The value as text, if it is all visible ASCII.
    pub fn to_str(&self) -> Result<&str, ToStrError> {
        if !self.inner.iter().all(|&b| is_visible_ascii(b)) {
            return Err(ToStrError(()));
        }
        // Visible ASCII is UTF-8.
        Ok(unsafe { std::str::from_utf8_unchecked(&self.inner) })
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.inner
    }

    /// The shared bytes behind the value.
    pub fn as_shared(&self) -> &Bytes {
        &self.inner
    }

    /// Mark the value as secret: `Debug` hides it, and HPACK never indexes it.
    pub fn set_sensitive(&mut self, sensitive: bool) {
        self.is_sensitive = sensitive;
    }

    pub fn is_sensitive(&self) -> bool {
        self.is_sensitive
    }
}

fn validate(src: &[u8]) -> Result<(), InvalidHeaderValue> {
    if src.iter().all(|&b| is_value_byte(b)) {
        Ok(())
    } else {
        Err(InvalidHeaderValue(()))
    }
}

impl AsRef<[u8]> for HeaderValue {
    fn as_ref(&self) -> &[u8] {
        &self.inner
    }
}

impl fmt::Debug for HeaderValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_sensitive {
            return f.write_str("Sensitive");
        }
        f.write_char('"')?;
        for &b in self.inner.iter() {
            match b {
                b'"' => f.write_str("\\\"")?,
                b if is_visible_ascii(b) => f.write_char(b as char)?,
                b => write!(f, "\\x{b:x}")?,
            }
        }
        f.write_char('"')
    }
}

impl From<HeaderName> for HeaderValue {
    fn from(name: HeaderName) -> HeaderValue {
        HeaderValue {
            inner: name.into_bytes(),
            is_sensitive: false,
        }
    }
}

macro_rules! from_integers {
    ($($ty:ty),*) => {$(
        impl From<$ty> for HeaderValue {
            fn from(n: $ty) -> HeaderValue {
                let mut buf = BytesMut::with_capacity(20);
                let _ = write!(buf, "{n}");
                HeaderValue {
                    inner: buf.freeze(),
                    is_sensitive: false,
                }
            }
        }
    )*};
}

from_integers!(u16, i16, u32, i32, u64, i64, usize, isize);

impl FromStr for HeaderValue {
    type Err = InvalidHeaderValue;

    fn from_str(src: &str) -> Result<HeaderValue, InvalidHeaderValue> {
        HeaderValue::from_str(src)
    }
}

impl From<&HeaderValue> for HeaderValue {
    fn from(value: &HeaderValue) -> HeaderValue {
        value.clone()
    }
}

impl TryFrom<&str> for HeaderValue {
    type Error = InvalidHeaderValue;

    fn try_from(src: &str) -> Result<HeaderValue, InvalidHeaderValue> {
        HeaderValue::from_str(src)
    }
}

impl TryFrom<&String> for HeaderValue {
    type Error = InvalidHeaderValue;

    fn try_from(src: &String) -> Result<HeaderValue, InvalidHeaderValue> {
        HeaderValue::from_str(src)
    }
}

impl TryFrom<String> for HeaderValue {
    type Error = InvalidHeaderValue;

    fn try_from(src: String) -> Result<HeaderValue, InvalidHeaderValue> {
        HeaderValue::from_str(&src)
    }
}

impl TryFrom<&[u8]> for HeaderValue {
    type Error = InvalidHeaderValue;

    fn try_from(src: &[u8]) -> Result<HeaderValue, InvalidHeaderValue> {
        HeaderValue::from_bytes(src)
    }
}

impl TryFrom<Vec<u8>> for HeaderValue {
    type Error = InvalidHeaderValue;

    fn try_from(src: Vec<u8>) -> Result<HeaderValue, InvalidHeaderValue> {
        HeaderValue::from_bytes(&src)
    }
}

impl TryFrom<Bytes> for HeaderValue {
    type Error = InvalidHeaderValue;

    fn try_from(src: Bytes) -> Result<HeaderValue, InvalidHeaderValue> {
        HeaderValue::from_shared(src)
    }
}

impl Hash for HeaderValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl PartialEq for HeaderValue {
    fn eq(&self, other: &HeaderValue) -> bool {
        self.inner == other.inner
    }
}

impl Eq for HeaderValue {}

impl PartialOrd for HeaderValue {
    fn partial_cmp(&self, other: &HeaderValue) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeaderValue {
    fn cmp(&self, other: &HeaderValue) -> Ordering {
        self.inner.cmp(&other.inner)
    }
}

macro_rules! cmp_with {
    ($($ty:ty => $as_bytes:expr),*) => {$(
        impl PartialEq<$ty> for HeaderValue {
            fn eq(&self, other: &$ty) -> bool {
                self.as_bytes() == $as_bytes(other)
            }
        }

        impl PartialEq<HeaderValue> for $ty {
            fn eq(&self, other: &HeaderValue) -> bool {
                $as_bytes(self) == other.as_bytes()
            }
        }

        impl PartialOrd<$ty> for HeaderValue {
            fn partial_cmp(&self, other: &$ty) -> Option<Ordering> {
                self.as_bytes().partial_cmp($as_bytes(other))
            }
        }

        impl PartialOrd<HeaderValue> for $ty {
            fn partial_cmp(&self, other: &HeaderValue) -> Option<Ordering> {
                $as_bytes(self).partial_cmp(other.as_bytes())
            }
        }
    )*};
}

cmp_with!(
    str => str::as_bytes,
    [u8] => std::convert::identity,
    String => String::as_bytes
);

impl PartialEq<HeaderValue> for &str {
    fn eq(&self, other: &HeaderValue) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl PartialEq<HeaderValue> for &HeaderValue {
    fn eq(&self, other: &HeaderValue) -> bool {
        **self == *other
    }
}

impl<T: ?Sized> PartialEq<&T> for HeaderValue
where
    HeaderValue: PartialEq<T>,
{
    fn eq(&self, other: &&T) -> bool {
        *self == **other
    }
}

impl fmt::Display for InvalidHeaderValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid HTTP header value")
    }
}

impl std::error::Error for InvalidHeaderValue {}

impl fmt::Display for ToStrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("header value holds bytes outside visible ASCII")
    }
}

impl std::error::Error for ToStrError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn control_bytes_are_rejected() {
        assert!(HeaderValue::from_bytes(b"a\r\nb").is_err());
        assert!(HeaderValue::from_bytes(b"a\0").is_err());
        assert!(HeaderValue::from_bytes(b"a\x7f").is_err());
        assert!(HeaderValue::from_bytes(b"tab\tand space").is_ok());
    }

    #[test]
    fn obs_text_is_a_value_but_not_a_str() {
        let v = HeaderValue::from_bytes(b"caf\xc3\xa9").unwrap();
        assert!(v.to_str().is_err());
        assert!(HeaderValue::from_str("café").is_err());
    }

    #[test]
    fn from_shared_does_not_copy() {
        let bytes = Bytes::copy_from_slice(b"gzip");
        let at = bytes.as_ptr();
        let v = HeaderValue::from_shared(bytes).unwrap();
        assert_eq!(v.as_bytes().as_ptr(), at);
        assert_eq!(v, "gzip");
    }

    #[test]
    fn integers_and_debug() {
        assert_eq!(HeaderValue::from(1024usize), "1024");
        assert_eq!(HeaderValue::from(-3i32), "-3");
        let v = HeaderValue::from_bytes(b"say \"hi\"\xff").unwrap();
        assert_eq!(format!("{v:?}"), r#""say \"hi\"\xff""#);
        let mut secret = HeaderValue::from_static("token");
        secret.set_sensitive(true);
        assert_eq!(format!("{secret:?}"), "Sensitive");
    }

    #[test]
    fn from_static_is_const() {
        const JSON: HeaderValue = HeaderValue::from_static("application/json");
        assert_eq!(JSON, "application/json");
    }
}
