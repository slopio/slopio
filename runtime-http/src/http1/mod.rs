use bumpalo::Bump;

use crate::{
    http1::request::{Method, ParseError, Request},
    http1::response::Response,
};
use runtime_core::net::{SocketWrapper, socket_recv_init, socket_send};

pub mod request;
pub mod response;

const READ_BUF_SIZE: usize = 8192;
const MAX_HEADERS: usize = 64;
const MAX_HEAD_SIZE: usize = 64 * 1024;
const MAX_LINE_SIZE: usize = 8192;
const MAX_BODY_SIZE: usize = 8 * 1024 * 1024;

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
