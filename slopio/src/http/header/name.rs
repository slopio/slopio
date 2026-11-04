use std::fmt;
use std::str::FromStr;

use crate::bytes::{Bytes, BytesMut};

use crate::http::chars::is_tchar;

/// A header field name, always lowercase.
///
/// The standard names are constants in [`header`](crate::http::header) and cost
/// nothing to clone; any other name keeps its lowercase bytes.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct HeaderName {
    repr: Repr,
}

// A name equal to a standard one is always `Standard`, so derived `Eq`/`Hash` hold.
#[derive(Clone, PartialEq, Eq, Hash)]
enum Repr {
    Standard(StandardHeader),
    Custom(Bytes),
}

/// The bytes are empty or hold a byte that is not a token character.
#[derive(Debug)]
pub struct InvalidHeaderName(());

/// Names this long are lowercased on the stack.
const SCRATCH: usize = 64;

impl HeaderName {
    /// Parse a name in any case; it is stored lowercase.
    pub fn from_bytes(src: &[u8]) -> Result<HeaderName, InvalidHeaderName> {
        validate(src)?;
        if !src.iter().any(u8::is_ascii_uppercase) {
            return Ok(HeaderName::lowercase(src));
        }
        if src.len() <= SCRATCH {
            let mut scratch = [0; SCRATCH];
            let lower = &mut scratch[..src.len()];
            lower.copy_from_slice(src);
            lower.make_ascii_lowercase();
            return Ok(HeaderName::lowercase(lower));
        }
        let mut lower = BytesMut::from(src);
        lower.make_ascii_lowercase();
        Ok(HeaderName::from_valid_lowercase(lower.freeze()))
    }

    /// Parse a name that must already be lowercase, as HTTP/2 requires.
    pub fn from_lowercase(src: &[u8]) -> Result<HeaderName, InvalidHeaderName> {
        validate(src)?;
        if src.iter().any(u8::is_ascii_uppercase) {
            return Err(InvalidHeaderName(()));
        }
        Ok(HeaderName::lowercase(src))
    }

    /// Like [`from_lowercase`](Self::from_lowercase), keeping `src` instead of copying it.
    pub fn from_shared_lowercase(src: Bytes) -> Result<HeaderName, InvalidHeaderName> {
        validate(&src)?;
        if src.iter().any(u8::is_ascii_uppercase) {
            return Err(InvalidHeaderName(()));
        }
        Ok(HeaderName::from_valid_lowercase(src))
    }

    /// A name from a constant; usable in `const`.
    ///
    /// Panics (at compile time in a `const`) unless `src` is a lowercase token.
    pub const fn from_static(src: &'static str) -> HeaderName {
        let bytes = src.as_bytes();
        if bytes.is_empty() {
            panic!("HeaderName::from_static with an empty name");
        }
        let mut i = 0;
        while i < bytes.len() {
            if !is_tchar(bytes[i]) || bytes[i].is_ascii_uppercase() {
                panic!("HeaderName::from_static needs a lowercase token");
            }
            i += 1;
        }
        match StandardHeader::from_lowercase(bytes) {
            Some(h) => HeaderName {
                repr: Repr::Standard(h),
            },
            None => HeaderName {
                repr: Repr::Custom(Bytes::from_static(bytes)),
            },
        }
    }

    pub fn as_str(&self) -> &str {
        match &self.repr {
            Repr::Standard(h) => h.as_str(),
            // Validated tokens are ASCII.
            Repr::Custom(bytes) => unsafe { std::str::from_utf8_unchecked(bytes) },
        }
    }

    pub(crate) fn into_bytes(self) -> Bytes {
        match self.repr {
            Repr::Standard(h) => Bytes::from_static(h.as_str().as_bytes()),
            Repr::Custom(bytes) => bytes,
        }
    }

    fn lowercase(src: &[u8]) -> HeaderName {
        let repr = match StandardHeader::from_lowercase(src) {
            Some(h) => Repr::Standard(h),
            None => Repr::Custom(Bytes::copy_from_slice(src)),
        };
        HeaderName { repr }
    }

    fn from_valid_lowercase(src: Bytes) -> HeaderName {
        let repr = match StandardHeader::from_lowercase(&src) {
            Some(h) => Repr::Standard(h),
            None => Repr::Custom(src),
        };
        HeaderName { repr }
    }
}

fn validate(src: &[u8]) -> Result<(), InvalidHeaderName> {
    if !src.is_empty() && src.iter().all(|&b| is_tchar(b)) {
        Ok(())
    } else {
        Err(InvalidHeaderName(()))
    }
}

macro_rules! standard_headers {
    ($(($variant:ident, $konst:ident, $name:literal);)+) => {
        #[derive(Clone, Copy, PartialEq, Eq, Hash)]
        enum StandardHeader {
            $($variant,)+
        }

        #[cfg(test)]
        const STANDARD: &[(StandardHeader, &[u8])] = &[$((StandardHeader::$variant, $name),)+];

        impl StandardHeader {
            fn as_str(self) -> &'static str {
                let name: &'static [u8] = match self {
                    $(StandardHeader::$variant => $name,)+
                };
                // The table is ASCII.
                unsafe { std::str::from_utf8_unchecked(name) }
            }

            const fn from_lowercase(src: &[u8]) -> Option<StandardHeader> {
                match src {
                    $($name => Some(StandardHeader::$variant),)+
                    _ => None,
                }
            }
        }

        $(
            #[doc = concat!("`", stringify!($konst), "`")]
            pub const $konst: HeaderName = HeaderName {
                repr: Repr::Standard(StandardHeader::$variant),
            };
        )+
    };
}

standard_headers! {
    (Accept, ACCEPT, b"accept");
    (AcceptCharset, ACCEPT_CHARSET, b"accept-charset");
    (AcceptEncoding, ACCEPT_ENCODING, b"accept-encoding");
    (AcceptLanguage, ACCEPT_LANGUAGE, b"accept-language");
    (AcceptRanges, ACCEPT_RANGES, b"accept-ranges");
    (AccessControlAllowCredentials, ACCESS_CONTROL_ALLOW_CREDENTIALS, b"access-control-allow-credentials");
    (AccessControlAllowHeaders, ACCESS_CONTROL_ALLOW_HEADERS, b"access-control-allow-headers");
    (AccessControlAllowMethods, ACCESS_CONTROL_ALLOW_METHODS, b"access-control-allow-methods");
    (AccessControlAllowOrigin, ACCESS_CONTROL_ALLOW_ORIGIN, b"access-control-allow-origin");
    (AccessControlExposeHeaders, ACCESS_CONTROL_EXPOSE_HEADERS, b"access-control-expose-headers");
    (AccessControlMaxAge, ACCESS_CONTROL_MAX_AGE, b"access-control-max-age");
    (AccessControlRequestHeaders, ACCESS_CONTROL_REQUEST_HEADERS, b"access-control-request-headers");
    (AccessControlRequestMethod, ACCESS_CONTROL_REQUEST_METHOD, b"access-control-request-method");
    (Age, AGE, b"age");
    (Allow, ALLOW, b"allow");
    (AltSvc, ALT_SVC, b"alt-svc");
    (Authorization, AUTHORIZATION, b"authorization");
    (CacheControl, CACHE_CONTROL, b"cache-control");
    (CacheStatus, CACHE_STATUS, b"cache-status");
    (CdnCacheControl, CDN_CACHE_CONTROL, b"cdn-cache-control");
    (Connection, CONNECTION, b"connection");
    (ContentDisposition, CONTENT_DISPOSITION, b"content-disposition");
    (ContentEncoding, CONTENT_ENCODING, b"content-encoding");
    (ContentLanguage, CONTENT_LANGUAGE, b"content-language");
    (ContentLength, CONTENT_LENGTH, b"content-length");
    (ContentLocation, CONTENT_LOCATION, b"content-location");
    (ContentRange, CONTENT_RANGE, b"content-range");
    (ContentSecurityPolicy, CONTENT_SECURITY_POLICY, b"content-security-policy");
    (ContentSecurityPolicyReportOnly, CONTENT_SECURITY_POLICY_REPORT_ONLY, b"content-security-policy-report-only");
    (ContentType, CONTENT_TYPE, b"content-type");
    (Cookie, COOKIE, b"cookie");
    (Dnt, DNT, b"dnt");
    (Date, DATE, b"date");
    (Etag, ETAG, b"etag");
    (Expect, EXPECT, b"expect");
    (Expires, EXPIRES, b"expires");
    (Forwarded, FORWARDED, b"forwarded");
    (From, FROM, b"from");
    (Host, HOST, b"host");
    (IfMatch, IF_MATCH, b"if-match");
    (IfModifiedSince, IF_MODIFIED_SINCE, b"if-modified-since");
    (IfNoneMatch, IF_NONE_MATCH, b"if-none-match");
    (IfRange, IF_RANGE, b"if-range");
    (IfUnmodifiedSince, IF_UNMODIFIED_SINCE, b"if-unmodified-since");
    (LastModified, LAST_MODIFIED, b"last-modified");
    (Link, LINK, b"link");
    (Location, LOCATION, b"location");
    (MaxForwards, MAX_FORWARDS, b"max-forwards");
    (Origin, ORIGIN, b"origin");
    (Pragma, PRAGMA, b"pragma");
    (ProxyAuthenticate, PROXY_AUTHENTICATE, b"proxy-authenticate");
    (ProxyAuthorization, PROXY_AUTHORIZATION, b"proxy-authorization");
    (PublicKeyPins, PUBLIC_KEY_PINS, b"public-key-pins");
    (PublicKeyPinsReportOnly, PUBLIC_KEY_PINS_REPORT_ONLY, b"public-key-pins-report-only");
    (Range, RANGE, b"range");
    (Referer, REFERER, b"referer");
    (ReferrerPolicy, REFERRER_POLICY, b"referrer-policy");
    (Refresh, REFRESH, b"refresh");
    (RetryAfter, RETRY_AFTER, b"retry-after");
    (SecWebSocketAccept, SEC_WEBSOCKET_ACCEPT, b"sec-websocket-accept");
    (SecWebSocketExtensions, SEC_WEBSOCKET_EXTENSIONS, b"sec-websocket-extensions");
    (SecWebSocketKey, SEC_WEBSOCKET_KEY, b"sec-websocket-key");
    (SecWebSocketProtocol, SEC_WEBSOCKET_PROTOCOL, b"sec-websocket-protocol");
    (SecWebSocketVersion, SEC_WEBSOCKET_VERSION, b"sec-websocket-version");
    (Server, SERVER, b"server");
    (SetCookie, SET_COOKIE, b"set-cookie");
    (StrictTransportSecurity, STRICT_TRANSPORT_SECURITY, b"strict-transport-security");
    (Te, TE, b"te");
    (Trailer, TRAILER, b"trailer");
    (TransferEncoding, TRANSFER_ENCODING, b"transfer-encoding");
    (UserAgent, USER_AGENT, b"user-agent");
    (Upgrade, UPGRADE, b"upgrade");
    (UpgradeInsecureRequests, UPGRADE_INSECURE_REQUESTS, b"upgrade-insecure-requests");
    (Vary, VARY, b"vary");
    (Via, VIA, b"via");
    (Warning, WARNING, b"warning");
    (WwwAuthenticate, WWW_AUTHENTICATE, b"www-authenticate");
    (XContentTypeOptions, X_CONTENT_TYPE_OPTIONS, b"x-content-type-options");
    (XDnsPrefetchControl, X_DNS_PREFETCH_CONTROL, b"x-dns-prefetch-control");
    (XFrameOptions, X_FRAME_OPTIONS, b"x-frame-options");
    (XXssProtection, X_XSS_PROTECTION, b"x-xss-protection");
}

impl AsRef<str> for HeaderName {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl AsRef<[u8]> for HeaderName {
    fn as_ref(&self) -> &[u8] {
        self.as_str().as_bytes()
    }
}

impl fmt::Debug for HeaderName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_str(), f)
    }
}

impl fmt::Display for HeaderName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl From<&HeaderName> for HeaderName {
    fn from(name: &HeaderName) -> HeaderName {
        name.clone()
    }
}

impl FromStr for HeaderName {
    type Err = InvalidHeaderName;

    fn from_str(src: &str) -> Result<HeaderName, InvalidHeaderName> {
        HeaderName::from_bytes(src.as_bytes())
    }
}

impl TryFrom<&str> for HeaderName {
    type Error = InvalidHeaderName;

    fn try_from(src: &str) -> Result<HeaderName, InvalidHeaderName> {
        HeaderName::from_bytes(src.as_bytes())
    }
}

impl TryFrom<&String> for HeaderName {
    type Error = InvalidHeaderName;

    fn try_from(src: &String) -> Result<HeaderName, InvalidHeaderName> {
        HeaderName::from_bytes(src.as_bytes())
    }
}

impl TryFrom<String> for HeaderName {
    type Error = InvalidHeaderName;

    fn try_from(src: String) -> Result<HeaderName, InvalidHeaderName> {
        HeaderName::from_bytes(src.as_bytes())
    }
}

impl TryFrom<&[u8]> for HeaderName {
    type Error = InvalidHeaderName;

    fn try_from(src: &[u8]) -> Result<HeaderName, InvalidHeaderName> {
        HeaderName::from_bytes(src)
    }
}

impl TryFrom<Vec<u8>> for HeaderName {
    type Error = InvalidHeaderName;

    fn try_from(src: Vec<u8>) -> Result<HeaderName, InvalidHeaderName> {
        HeaderName::from_bytes(&src)
    }
}

/// Case-insensitive, as header names are.
impl PartialEq<str> for HeaderName {
    fn eq(&self, other: &str) -> bool {
        self.as_str().eq_ignore_ascii_case(other)
    }
}

impl PartialEq<&str> for HeaderName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str().eq_ignore_ascii_case(other)
    }
}

impl PartialEq<HeaderName> for str {
    fn eq(&self, other: &HeaderName) -> bool {
        other == self
    }
}

impl PartialEq<HeaderName> for &str {
    fn eq(&self, other: &HeaderName) -> bool {
        other == *self
    }
}

impl PartialEq<&HeaderName> for HeaderName {
    fn eq(&self, other: &&HeaderName) -> bool {
        self == *other
    }
}

impl PartialEq<HeaderName> for &HeaderName {
    fn eq(&self, other: &HeaderName) -> bool {
        *self == other
    }
}

impl fmt::Display for InvalidHeaderName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("invalid HTTP header name")
    }
}

impl std::error::Error for InvalidHeaderName {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn standard_names_resolve_to_their_constant() {
        assert_eq!(
            HeaderName::from_bytes(b"Content-Type").unwrap(),
            CONTENT_TYPE
        );
        assert_eq!(
            HeaderName::from_lowercase(b"content-type").unwrap(),
            CONTENT_TYPE
        );
        assert_eq!(HeaderName::from_static("content-type"), CONTENT_TYPE);
        assert_eq!(CONTENT_TYPE.as_str(), "content-type");
    }

    #[test]
    fn custom_names_are_stored_lowercase() {
        let name = HeaderName::from_bytes(b"X-Request-Id").unwrap();
        assert_eq!(name.as_str(), "x-request-id");
        assert_eq!(name, "X-REQUEST-ID");
        let long = "X-".repeat(40);
        assert_eq!(
            HeaderName::from_bytes(long.as_bytes()).unwrap().as_str(),
            long.to_lowercase()
        );
    }

    #[test]
    fn from_lowercase_rejects_uppercase() {
        assert!(HeaderName::from_lowercase(b"Host").is_err());
        assert!(HeaderName::from_shared_lowercase(Bytes::from_static(b"Host")).is_err());
    }

    #[test]
    fn invalid_names() {
        assert!(HeaderName::from_bytes(b"").is_err());
        assert!(HeaderName::from_bytes(b"bad name").is_err());
        assert!(HeaderName::from_bytes(b"colon:").is_err());
        assert!(HeaderName::from_bytes(b":path").is_err());
    }

    #[test]
    fn shared_custom_names_keep_their_bytes() {
        let bytes = Bytes::copy_from_slice(b"x-trace");
        let at = bytes.as_ptr();
        let name = HeaderName::from_shared_lowercase(bytes).unwrap();
        assert_eq!(name.as_str().as_ptr(), at);
    }

    #[test]
    fn every_constant_round_trips() {
        for &(h, name) in STANDARD {
            let parsed = HeaderName::from_lowercase(name).unwrap();
            assert!(parsed.repr == Repr::Standard(h));
        }
    }
}
