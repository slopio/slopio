/// Well-known HTTP header names as byte-string constants.
pub struct Header;

impl Header {
    pub const CONTENT_TYPE: &[u8] = b"content-type";
    pub const CONTENT_LENGTH: &[u8] = b"content-length";
    pub const TRANSFER_ENCODING: &[u8] = b"transfer-encoding";
    pub const HOST: &[u8] = b"host";
    pub const CONNECTION: &[u8] = b"connection";
    pub const ACCEPT: &[u8] = b"accept";
    pub const AUTHORIZATION: &[u8] = b"authorization";
    pub const USER_AGENT: &[u8] = b"user-agent";
    pub const CACHE_CONTROL: &[u8] = b"cache-control";
    pub const DATE: &[u8] = b"date";
    pub const SERVER: &[u8] = b"server";
    pub const LOCATION: &[u8] = b"location";
    pub const COOKIE: &[u8] = b"cookie";
    pub const SET_COOKIE: &[u8] = b"set-cookie";
}
