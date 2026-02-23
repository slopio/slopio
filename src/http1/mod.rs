use bumpalo::Bump;

use crate::{
    http1::request::{Method, ParseError, Request},
    http1::response::Response,
    io_driver::operations::sockets::{SocketWrapper, recv::socket_recv_init, send::socket_send},
};

pub mod request;
pub mod response;

const READ_BUF_SIZE: usize = 8192;
const MAX_HEADERS: usize = 64;
const MAX_HEAD_SIZE: usize = 64 * 1024;
const MAX_LINE_SIZE: usize = 8192;
const MAX_BODY_SIZE: usize = 8 * 1024 * 1024;

pub struct HttpConnection {
    socket: SocketWrapper,
    buf: Vec<u8>,
    consumed: usize,
    arena: Bump,
}

impl HttpConnection {
    pub fn new(socket: SocketWrapper) -> Self {
        Self {
            socket,
            buf: Vec::with_capacity(READ_BUF_SIZE),
            consumed: 0,
            arena: Bump::new(),
        }
    }

    pub fn arena(&self) -> &Bump {
        &self.arena
    }

    pub async fn recv(&mut self) -> Result<Option<Request<'_>>, ParseError> {
        self.arena.reset();

        if self.consumed > 0 {
            self.buf.drain(..self.consumed);
            self.consumed = 0;
        }

        if self.buf.is_empty() {
            self.buf.resize(READ_BUF_SIZE, 0u8);
            let n = socket_recv_init(&self.socket, &mut self.buf, READ_BUF_SIZE).await?;
            self.buf.truncate(n);
            if n == 0 {
                return Ok(None);
            }
        }

        let mut cursor = 0;
        let mut state = ParseState::RequestLine {
            headers: bumpalo::collections::Vec::with_capacity_in(16, &self.arena),
        };

        let head_end = loop {
            if self.buf.len() >= MAX_HEAD_SIZE {
                return Err(ParseError::HeadersTooLarge);
            }

            while cursor < self.buf.len() {
                let remaining = &self.buf[cursor..];
                let Some(line_end) = find_crlf(remaining) else {
                    if remaining.len() > MAX_LINE_SIZE {
                        return Err(ParseError::LineTooLong);
                    }
                    break; // incomplete line, need more data
                };
                let line = &remaining[..line_end];
                if memchr::memchr(b'\r', line).is_some() {
                    return Err(ParseError::InvalidRequest(httparse::Error::Token));
                }
                cursor += line_end + 2;

                state = match state {
                    ParseState::RequestLine { headers } => {
                        let (m, p) = parse_request_line(line)?;
                        ParseState::Headers {
                            method: Method::from_str(m).ok_or(ParseError::UnknownMethod)?,
                            path: self.arena.alloc_str(p) as &str,
                            headers,
                        }
                    }
                    ParseState::Headers {
                        method,
                        path,
                        mut headers,
                    } => {
                        if line.is_empty() {
                            state = ParseState::Done {
                                method,
                                path,
                                headers,
                            };
                            break;
                        }
                        let (name, value) = parse_header_line(line)?;
                        let name: &str = self.arena.alloc_str(name);
                        let value: &[u8] = self.arena.alloc_slice_copy(value);
                        if headers.len() >= MAX_HEADERS {
                            return Err(ParseError::HeadersTooLarge);
                        }
                        headers.push((name, value));
                        ParseState::Headers {
                            method,
                            path,
                            headers,
                        }
                    }
                    done @ ParseState::Done { .. } => {
                        state = done;
                        break;
                    }
                };
            }

            if matches!(state, ParseState::Done { .. }) {
                break cursor;
            }

            // Need more data — read from socket
            let prev_len = self.buf.len();
            self.buf.resize(prev_len + READ_BUF_SIZE, 0u8);
            let n =
                socket_recv_init(&self.socket, &mut self.buf[prev_len..], READ_BUF_SIZE).await?;
            self.buf.truncate(prev_len + n);
            if n == 0 {
                return Err(ParseError::UnexpectedEof);
            }
        };

        let ParseState::Done {
            method,
            path,
            headers,
        } = state
        else {
            unreachable!()
        };

        let headers = headers.into_bump_slice();

        if headers
            .iter()
            .any(|(k, _)| k.eq_ignore_ascii_case("transfer-encoding"))
        {
            return Err(ParseError::UnsupportedTransferEncoding);
        }

        let mut content_length: usize = 0;
        let mut cl_found = false;
        for (k, v) in headers.iter() {
            if k.eq_ignore_ascii_case("content-length") {
                let val: usize = std::str::from_utf8(v)
                    .ok()
                    .and_then(|s| s.trim().parse().ok())
                    .ok_or(ParseError::MissingField("content-length value"))?;
                if cl_found && val != content_length {
                    return Err(ParseError::ConflictingContentLength);
                }
                content_length = val;
                cl_found = true;
            }
        }
        if content_length > MAX_BODY_SIZE {
            return Err(ParseError::BodyTooLarge);
        }

        let mut body = self.buf[head_end..].to_vec();
        self.consumed = self.buf.len();

        while body.len() < content_length {
            let needed = content_length - body.len();
            let prev = body.len();
            body.resize(prev + needed.min(READ_BUF_SIZE), 0u8);
            let n = socket_recv_init(&self.socket, &mut body[prev..], needed.min(READ_BUF_SIZE))
                .await?;
            body.truncate(prev + n);
            if n == 0 {
                return Err(ParseError::UnexpectedEof);
            }
        }
        body.truncate(content_length);

        Ok(Some(Request {
            method,
            path,
            headers,
            body,
        }))
    }

    pub async fn send(&self, response: Response<'_>) -> std::io::Result<()> {
        let bytes = response.serialize();
        let mut sent = 0;
        while sent < bytes.len() {
            let n = socket_send(&self.socket, &bytes[sent..]).await?;
            sent += n;
        }
        Ok(())
    }
}

enum ParseState<'arena> {
    RequestLine {
        headers: bumpalo::collections::Vec<'arena, (&'arena str, &'arena [u8])>,
    },
    Headers {
        method: Method,
        path: &'arena str,
        headers: bumpalo::collections::Vec<'arena, (&'arena str, &'arena [u8])>,
    },
    Done {
        method: Method,
        path: &'arena str,
        headers: bumpalo::collections::Vec<'arena, (&'arena str, &'arena [u8])>,
    },
}

fn find_crlf(buf: &[u8]) -> Option<usize> {
    memchr::memmem::find(buf, b"\r\n")
}

fn parse_request_line(line: &[u8]) -> Result<(&str, &str), ParseError> {
    let line = std::str::from_utf8(line).map_err(|_| ParseError::MissingField("request line"))?;
    let mut parts = line.splitn(3, ' ');
    let method = parts.next().ok_or(ParseError::MissingField("method"))?;
    let path = parts.next().ok_or(ParseError::MissingField("path"))?;
    let _version = parts.next().ok_or(ParseError::MissingField("version"))?;
    Ok((method, path))
}

fn parse_header_line(line: &[u8]) -> Result<(&str, &[u8]), ParseError> {
    let colon = memchr::memchr(b':', line).ok_or(ParseError::MissingField("header colon"))?;
    let name_bytes = &line[..colon];
    if name_bytes.iter().any(|&b| b == b' ' || b == b'\t') {
        return Err(ParseError::InvalidHeaderName);
    }
    let name =
        std::str::from_utf8(name_bytes).map_err(|_| ParseError::MissingField("header name"))?;
    let mut value_start = colon + 1;
    while value_start < line.len() && (line[value_start] == b' ' || line[value_start] == b'\t') {
        value_start += 1;
    }
    Ok((name, &line[value_start..]))
}
