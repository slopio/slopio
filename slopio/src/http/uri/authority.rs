use std::fmt;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use crate::bytes::Bytes;

use super::{ErrorKind, InvalidUri, MAX_LEN};

/// The authority of a URI: `[userinfo@]host[:port]`.
#[derive(Clone)]
pub struct Authority {
    data: Bytes,
}

/// A port, with the text it was parsed from.
#[derive(Clone, Copy)]
pub struct Port<T> {
    port: u16,
    repr: T,
}

/// Bytes allowed in an authority; `%` and the delimiters are handled apart.
const fn is_authority_char(b: u8) -> bool {
    matches!(b,
        b'!' | b'$' | b'&' | b'\'' | b'(' | b')' | b'*' | b'+' | b',' | b'-' | b'.'
        | b'0'..=b'9' | b':' | b';' | b'=' | b'@' | b'[' | b']' | b'_' | b'~'
        | b'a'..=b'z' | b'A'..=b'Z')
}

impl Authority {
    /// Panics if `src` is not a valid authority.
    pub fn from_static(src: &'static str) -> Authority {
        Authority::from_shared(Bytes::from_static(src.as_bytes()))
            .expect("static str is not a valid authority")
    }

    /// Wrap `src` without copying.
    pub fn from_shared(src: Bytes) -> Result<Authority, InvalidUri> {
        if src.len() > MAX_LEN {
            return Err(ErrorKind::TooLong.into());
        }
        if Authority::parse(&src)? != src.len() {
            return Err(ErrorKind::InvalidAuthority.into());
        }
        Ok(Authority { data: src })
    }

    /// The length of the authority at the start of `s`: up to the first `/`, `?` or `#`.
    pub(super) fn parse(s: &[u8]) -> Result<usize, InvalidUri> {
        if s.is_empty() {
            return Err(ErrorKind::Empty.into());
        }
        // An IPv6 address with a port: [FEDC:BA98:7654:3210:FEDC:BA98:7654:3210]:80
        const MAX_COLONS: u32 = 8;
        let mut colons = 0;
        let mut open_bracket = false;
        let mut close_bracket = false;
        let mut percent = false;
        let mut at = None;
        let mut end = s.len();

        for (i, &b) in s.iter().enumerate() {
            match b {
                b'/' | b'?' | b'#' => {
                    end = i;
                    break;
                }
                // Percent-encoding is allowed in the userinfo and in an IPv6 zone id only.
                b'%' => percent = true,
                b':' => {
                    if colons >= MAX_COLONS {
                        return Err(ErrorKind::InvalidAuthority.into());
                    }
                    colons += 1;
                }
                b'[' => {
                    if percent || open_bracket {
                        return Err(ErrorKind::InvalidAuthority.into());
                    }
                    open_bracket = true;
                }
                b']' => {
                    if !open_bracket || close_bracket {
                        return Err(ErrorKind::InvalidAuthority.into());
                    }
                    close_bracket = true;
                    colons = 0;
                    percent = false;
                }
                b'@' => {
                    at = Some(i);
                    colons = 0;
                    percent = false;
                }
                b if is_authority_char(b) => {}
                _ => return Err(ErrorKind::InvalidUriChar.into()),
            }
        }

        if open_bracket != close_bracket || colons > 1 || percent {
            return Err(ErrorKind::InvalidAuthority.into());
        }
        if end > 0 && at == Some(end - 1) {
            return Err(ErrorKind::InvalidAuthority.into());
        }
        Ok(end)
    }

    pub(super) fn from_valid(data: Bytes) -> Authority {
        Authority { data }
    }

    /// The host, without userinfo or port; an IPv6 host keeps its brackets.
    pub fn host(&self) -> &str {
        let host_port = self.as_str().rsplit('@').next().unwrap_or("");
        if host_port.starts_with('[') {
            match host_port.find(']') {
                Some(i) => &host_port[..=i],
                None => host_port,
            }
        } else {
            host_port.split(':').next().unwrap_or("")
        }
    }

    pub fn port(&self) -> Option<Port<&str>> {
        let s = self.as_str();
        let colon = s.rfind(':')?;
        Port::from_str(&s[colon + 1..]).ok()
    }

    pub fn port_u16(&self) -> Option<u16> {
        self.port().map(|p| p.as_u16())
    }

    pub fn as_str(&self) -> &str {
        // Validated authority bytes are ASCII.
        unsafe { std::str::from_utf8_unchecked(&self.data) }
    }

    pub fn as_shared(&self) -> &Bytes {
        &self.data
    }
}

impl<T: AsRef<str>> Port<T> {
    pub fn as_u16(&self) -> u16 {
        self.port
    }

    pub fn as_str(&self) -> &str {
        self.repr.as_ref()
    }
}

impl<'a> Port<&'a str> {
    fn from_str(repr: &'a str) -> Result<Port<&'a str>, InvalidUri> {
        if repr.is_empty() || !repr.bytes().all(|b| b.is_ascii_digit()) {
            return Err(ErrorKind::InvalidPort.into());
        }
        let port = repr
            .parse()
            .map_err(|_| InvalidUri::from(ErrorKind::InvalidPort))?;
        Ok(Port { port, repr })
    }
}

impl<T> fmt::Debug for Port<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Port").field(&self.port).finish()
    }
}

impl<T> fmt::Display for Port<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.port, f)
    }
}

impl<T> PartialEq<u16> for Port<T> {
    fn eq(&self, other: &u16) -> bool {
        self.port == *other
    }
}

impl<T, U> PartialEq<Port<U>> for Port<T> {
    fn eq(&self, other: &Port<U>) -> bool {
        self.port == other.port
    }
}

impl fmt::Debug for Authority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl fmt::Display for Authority {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl AsRef<str> for Authority {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Case-insensitive.
impl PartialEq for Authority {
    fn eq(&self, other: &Authority) -> bool {
        self.as_str().eq_ignore_ascii_case(other.as_str())
    }
}

impl Eq for Authority {}

impl PartialEq<str> for Authority {
    fn eq(&self, other: &str) -> bool {
        self.as_str().eq_ignore_ascii_case(other)
    }
}

impl PartialEq<&str> for Authority {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq_ignore_ascii_case(other)
    }
}

impl PartialEq<Authority> for str {
    fn eq(&self, other: &Authority) -> bool {
        other == self
    }
}

impl Hash for Authority {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(self.data.len());
        for &b in self.data.iter() {
            state.write_u8(b.to_ascii_lowercase());
        }
    }
}

impl FromStr for Authority {
    type Err = InvalidUri;

    fn from_str(src: &str) -> Result<Authority, InvalidUri> {
        Authority::try_from(src.as_bytes())
    }
}

impl TryFrom<&[u8]> for Authority {
    type Error = InvalidUri;

    fn try_from(src: &[u8]) -> Result<Authority, InvalidUri> {
        if Authority::parse(src)? != src.len() {
            return Err(ErrorKind::InvalidAuthority.into());
        }
        Ok(Authority {
            data: Bytes::copy_from_slice(src),
        })
    }
}

impl TryFrom<&str> for Authority {
    type Error = InvalidUri;

    fn try_from(src: &str) -> Result<Authority, InvalidUri> {
        Authority::try_from(src.as_bytes())
    }
}

impl TryFrom<String> for Authority {
    type Error = InvalidUri;

    fn try_from(src: String) -> Result<Authority, InvalidUri> {
        Authority::try_from(src.as_bytes())
    }
}

impl TryFrom<Bytes> for Authority {
    type Error = InvalidUri;

    fn try_from(src: Bytes) -> Result<Authority, InvalidUri> {
        Authority::from_shared(src)
    }
}
