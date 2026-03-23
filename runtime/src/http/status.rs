#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StatusCode(u16);

impl StatusCode {
    pub const OK: Self = Self(200);
    pub const CREATED: Self = Self(201);
    pub const NO_CONTENT: Self = Self(204);

    pub const MOVED_PERMANENTLY: Self = Self(301);
    pub const FOUND: Self = Self(302);
    pub const NOT_MODIFIED: Self = Self(304);

    pub const BAD_REQUEST: Self = Self(400);
    pub const UNAUTHORIZED: Self = Self(401);
    pub const FORBIDDEN: Self = Self(403);
    pub const NOT_FOUND: Self = Self(404);
    pub const METHOD_NOT_ALLOWED: Self = Self(405);
    pub const REQUEST_TIMEOUT: Self = Self(408);
    pub const PAYLOAD_TOO_LARGE: Self = Self(413);
    pub const URI_TOO_LONG: Self = Self(414);
    pub const TOO_MANY_REQUESTS: Self = Self(429);

    pub const INTERNAL_SERVER_ERROR: Self = Self(500);
    pub const NOT_IMPLEMENTED: Self = Self(501);
    pub const BAD_GATEWAY: Self = Self(502);
    pub const SERVICE_UNAVAILABLE: Self = Self(503);
    pub const GATEWAY_TIMEOUT: Self = Self(504);

    pub const fn new(code: u16) -> Self {
        Self(code)
    }

    pub const fn as_u16(&self) -> u16 {
        self.0
    }

    pub const fn reason(&self) -> &'static str {
        match self.0 {
            200 => "OK",
            201 => "Created",
            204 => "No Content",
            301 => "Moved Permanently",
            302 => "Found",
            304 => "Not Modified",
            400 => "Bad Request",
            401 => "Unauthorized",
            403 => "Forbidden",
            404 => "Not Found",
            405 => "Method Not Allowed",
            408 => "Request Timeout",
            413 => "Payload Too Large",
            414 => "URI Too Long",
            429 => "Too Many Requests",
            500 => "Internal Server Error",
            501 => "Not Implemented",
            502 => "Bad Gateway",
            503 => "Service Unavailable",
            504 => "Gateway Timeout",
            _ => "Unknown",
        }
    }

    pub const fn is_informational(&self) -> bool {
        self.0 >= 100 && self.0 < 200
    }

    pub const fn is_success(&self) -> bool {
        self.0 >= 200 && self.0 < 300
    }

    pub const fn is_redirect(&self) -> bool {
        self.0 >= 300 && self.0 < 400
    }

    pub const fn is_client_error(&self) -> bool {
        self.0 >= 400 && self.0 < 500
    }

    pub const fn is_server_error(&self) -> bool {
        self.0 >= 500 && self.0 < 600
    }
}

impl std::fmt::Display for StatusCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {}", self.0, self.reason())
    }
}
