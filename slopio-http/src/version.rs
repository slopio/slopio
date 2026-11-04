//! The protocol version.

use std::fmt;

/// An HTTP version.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Version(Http);

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum Http {
    Http09,
    Http10,
    Http11,
    H2,
    H3,
}

impl Version {
    pub const HTTP_09: Version = Version(Http::Http09);
    pub const HTTP_10: Version = Version(Http::Http10);
    pub const HTTP_11: Version = Version(Http::Http11);
    pub const HTTP_2: Version = Version(Http::H2);
    pub const HTTP_3: Version = Version(Http::H3);
}

impl Default for Version {
    fn default() -> Version {
        Version::HTTP_11
    }
}

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.0 {
            Http::Http09 => "HTTP/0.9",
            Http::Http10 => "HTTP/1.0",
            Http::Http11 => "HTTP/1.1",
            Http::H2 => "HTTP/2.0",
            Http::H3 => "HTTP/3.0",
        })
    }
}
