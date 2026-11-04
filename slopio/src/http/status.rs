//! The response status code.

use std::fmt;
use std::num::NonZeroU16;
use std::str::FromStr;

/// A status code, any number in `100..=999`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StatusCode(NonZeroU16);

/// The number is outside `100..=999`, or the bytes are not three digits.
#[derive(Debug)]
pub struct InvalidStatusCode(());

/// "100101102...999": three digits per code, so `as_str` needs no allocation.
static DIGITS: [u8; 2700] = {
    let mut digits = [0; 2700];
    let mut code = 100;
    while code < 1000 {
        let at = (code - 100) * 3;
        digits[at] = b'0' + (code / 100) as u8;
        digits[at + 1] = b'0' + (code / 10 % 10) as u8;
        digits[at + 2] = b'0' + (code % 10) as u8;
        code += 1;
    }
    digits
};

impl StatusCode {
    pub const fn from_u16(code: u16) -> Result<StatusCode, InvalidStatusCode> {
        if code < 100 || code > 999 {
            return Err(InvalidStatusCode(()));
        }
        match NonZeroU16::new(code) {
            Some(code) => Ok(StatusCode(code)),
            None => Err(InvalidStatusCode(())),
        }
    }

    /// Parse exactly three ASCII digits.
    pub fn from_bytes(src: &[u8]) -> Result<StatusCode, InvalidStatusCode> {
        let [a, b, c] = src else {
            return Err(InvalidStatusCode(()));
        };
        if ![a, b, c].iter().all(|d| d.is_ascii_digit()) {
            return Err(InvalidStatusCode(()));
        }
        let code = (a - b'0') as u16 * 100 + (b - b'0') as u16 * 10 + (c - b'0') as u16;
        StatusCode::from_u16(code)
    }

    pub const fn as_u16(&self) -> u16 {
        self.0.get()
    }

    /// The three digits, e.g. `"404"`.
    pub fn as_str(&self) -> &str {
        let at = (self.as_u16() as usize - 100) * 3;
        // ASCII digits.
        unsafe { std::str::from_utf8_unchecked(&DIGITS[at..at + 3]) }
    }

    /// The registered reason phrase, if the code is a known one.
    pub fn canonical_reason(&self) -> Option<&'static str> {
        canonical_reason(self.as_u16())
    }

    pub fn is_informational(&self) -> bool {
        (100..200).contains(&self.as_u16())
    }

    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.as_u16())
    }

    pub fn is_redirection(&self) -> bool {
        (300..400).contains(&self.as_u16())
    }

    pub fn is_client_error(&self) -> bool {
        (400..500).contains(&self.as_u16())
    }

    pub fn is_server_error(&self) -> bool {
        (500..600).contains(&self.as_u16())
    }
}

macro_rules! status_codes {
    ($(($code:literal, $name:ident, $reason:literal);)+) => {
        impl StatusCode {
            $(
                #[doc = concat!("`", stringify!($code), " ", $reason, "`")]
                pub const $name: StatusCode = StatusCode(NonZeroU16::new($code).unwrap());
            )+
        }

        fn canonical_reason(code: u16) -> Option<&'static str> {
            match code {
                $($code => Some($reason),)+
                _ => None,
            }
        }
    };
}

status_codes! {
    (100, CONTINUE, "Continue");
    (101, SWITCHING_PROTOCOLS, "Switching Protocols");
    (102, PROCESSING, "Processing");
    (103, EARLY_HINTS, "Early Hints");
    (200, OK, "OK");
    (201, CREATED, "Created");
    (202, ACCEPTED, "Accepted");
    (203, NON_AUTHORITATIVE_INFORMATION, "Non Authoritative Information");
    (204, NO_CONTENT, "No Content");
    (205, RESET_CONTENT, "Reset Content");
    (206, PARTIAL_CONTENT, "Partial Content");
    (207, MULTI_STATUS, "Multi-Status");
    (208, ALREADY_REPORTED, "Already Reported");
    (226, IM_USED, "IM Used");
    (300, MULTIPLE_CHOICES, "Multiple Choices");
    (301, MOVED_PERMANENTLY, "Moved Permanently");
    (302, FOUND, "Found");
    (303, SEE_OTHER, "See Other");
    (304, NOT_MODIFIED, "Not Modified");
    (305, USE_PROXY, "Use Proxy");
    (307, TEMPORARY_REDIRECT, "Temporary Redirect");
    (308, PERMANENT_REDIRECT, "Permanent Redirect");
    (400, BAD_REQUEST, "Bad Request");
    (401, UNAUTHORIZED, "Unauthorized");
    (402, PAYMENT_REQUIRED, "Payment Required");
    (403, FORBIDDEN, "Forbidden");
    (404, NOT_FOUND, "Not Found");
    (405, METHOD_NOT_ALLOWED, "Method Not Allowed");
    (406, NOT_ACCEPTABLE, "Not Acceptable");
    (407, PROXY_AUTHENTICATION_REQUIRED, "Proxy Authentication Required");
    (408, REQUEST_TIMEOUT, "Request Timeout");
    (409, CONFLICT, "Conflict");
    (410, GONE, "Gone");
    (411, LENGTH_REQUIRED, "Length Required");
    (412, PRECONDITION_FAILED, "Precondition Failed");
    (413, PAYLOAD_TOO_LARGE, "Payload Too Large");
    (414, URI_TOO_LONG, "URI Too Long");
    (415, UNSUPPORTED_MEDIA_TYPE, "Unsupported Media Type");
    (416, RANGE_NOT_SATISFIABLE, "Range Not Satisfiable");
    (417, EXPECTATION_FAILED, "Expectation Failed");
    (418, IM_A_TEAPOT, "I'm a teapot");
    (421, MISDIRECTED_REQUEST, "Misdirected Request");
    (422, UNPROCESSABLE_ENTITY, "Unprocessable Entity");
    (423, LOCKED, "Locked");
    (424, FAILED_DEPENDENCY, "Failed Dependency");
    (425, TOO_EARLY, "Too Early");
    (426, UPGRADE_REQUIRED, "Upgrade Required");
    (428, PRECONDITION_REQUIRED, "Precondition Required");
    (429, TOO_MANY_REQUESTS, "Too Many Requests");
    (431, REQUEST_HEADER_FIELDS_TOO_LARGE, "Request Header Fields Too Large");
    (451, UNAVAILABLE_FOR_LEGAL_REASONS, "Unavailable For Legal Reasons");
    (500, INTERNAL_SERVER_ERROR, "Internal Server Error");
    (501, NOT_IMPLEMENTED, "Not Implemented");
    (502, BAD_GATEWAY, "Bad Gateway");
    (503, SERVICE_UNAVAILABLE, "Service Unavailable");
    (504, GATEWAY_TIMEOUT, "Gateway Timeout");
    (505, HTTP_VERSION_NOT_SUPPORTED, "HTTP Version Not Supported");
    (506, VARIANT_ALSO_NEGOTIATES, "Variant Also Negotiates");
    (507, INSUFFICIENT_STORAGE, "Insufficient Storage");
    (508, LOOP_DETECTED, "Loop Detected");
    (510, NOT_EXTENDED, "Not Extended");
    (511, NETWORK_AUTHENTICATION_REQUIRED, "Network Authentication Required");
}

impl Default for StatusCode {
    fn default() -> StatusCode {
        StatusCode::OK
    }
}

impl fmt::Debug for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.as_u16(), f)
    }
}

/// `"404 Not Found"`, or just the digits for an unknown code.
impl fmt::Display for StatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.canonical_reason() {
            Some(reason) => write!(f, "{} {reason}", self.as_u16()),
            None => write!(f, "{}", self.as_u16()),
        }
    }
}

impl PartialEq<u16> for StatusCode {
    fn eq(&self, other: &u16) -> bool {
        self.as_u16() == *other
    }
}

impl PartialEq<StatusCode> for u16 {
    fn eq(&self, other: &StatusCode) -> bool {
        *self == other.as_u16()
    }
}

impl From<StatusCode> for u16 {
    fn from(status: StatusCode) -> u16 {
        status.as_u16()
    }
}

impl From<&StatusCode> for StatusCode {
    fn from(status: &StatusCode) -> StatusCode {
        *status
    }
}

impl TryFrom<u16> for StatusCode {
    type Error = InvalidStatusCode;

    fn try_from(code: u16) -> Result<StatusCode, InvalidStatusCode> {
        StatusCode::from_u16(code)
    }
}

impl TryFrom<&[u8]> for StatusCode {
    type Error = InvalidStatusCode;

    fn try_from(src: &[u8]) -> Result<StatusCode, InvalidStatusCode> {
        StatusCode::from_bytes(src)
    }
}

impl TryFrom<&str> for StatusCode {
    type Error = InvalidStatusCode;

    fn try_from(src: &str) -> Result<StatusCode, InvalidStatusCode> {
        StatusCode::from_bytes(src.as_bytes())
    }
}

impl FromStr for StatusCode {
    type Err = InvalidStatusCode;

    fn from_str(src: &str) -> Result<StatusCode, InvalidStatusCode> {
        StatusCode::from_bytes(src.as_bytes())
    }
}

impl fmt::Display for InvalidStatusCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid status code")
    }
}

impl std::error::Error for InvalidStatusCode {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_is_100_to_999() {
        assert!(StatusCode::from_u16(99).is_err());
        assert!(StatusCode::from_u16(1000).is_err());
        assert_eq!(StatusCode::from_u16(999).unwrap().as_str(), "999");
        assert_eq!(StatusCode::from_u16(100).unwrap(), StatusCode::CONTINUE);
    }

    #[test]
    fn parses_exactly_three_digits() {
        assert_eq!(
            StatusCode::from_bytes(b"204").unwrap(),
            StatusCode::NO_CONTENT
        );
        assert!(StatusCode::from_bytes(b"20").is_err());
        assert!(StatusCode::from_bytes(b"2045").is_err());
        assert!(StatusCode::from_bytes(b"2a4").is_err());
        assert!(StatusCode::from_bytes(b"099").is_err());
    }

    #[test]
    fn display_and_classes() {
        assert_eq!(StatusCode::NOT_FOUND.to_string(), "404 Not Found");
        assert_eq!(StatusCode::from_u16(299).unwrap().to_string(), "299");
        assert!(StatusCode::NOT_MODIFIED.is_redirection());
        assert!(StatusCode::OK.is_success());
        assert_eq!(StatusCode::OK, 200);
    }
}
