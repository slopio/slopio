pub struct Response<'arena> {
    status: u16,
    reason: &'static str,
    headers: bumpalo::collections::Vec<'arena, (&'arena str, &'arena str)>,
    body: &'arena [u8],
}

impl<'arena> Response<'arena> {
    pub fn new(arena: &'arena bumpalo::Bump, status: u16, reason: &'static str) -> Self {
        Self {
            status,
            reason,
            headers: bumpalo::collections::Vec::new_in(arena),
            body: &[],
        }
    }

    pub fn header(mut self, name: &'arena str, value: &'arena str) -> Self {
        self.headers.push((name, value));
        self
    }

    pub fn body(mut self, body: &'arena [u8]) -> Self {
        self.body = body;
        self
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(256 + self.body.len());
        out.extend_from_slice(format!("HTTP/1.1 {} {}\r\n", self.status, self.reason).as_bytes());
        for (k, v) in &self.headers {
            out.extend_from_slice(format!("{}: {}\r\n", k, v).as_bytes());
        }
        out.extend_from_slice(format!("Content-Length: {}\r\n\r\n", self.body.len()).as_bytes());
        out.extend_from_slice(self.body);
        out
    }
}
