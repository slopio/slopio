use std::fmt::Debug;

use runtime_core::channels::Receiver;

use crate::codec::frames::headers::Headers;
use crate::server::output::SharedOutput;
use crate::server::stream::events_in::InputStreamEvent;
use crate::server::stream::window::SendWindow;

pub mod events_in;
mod window;

#[derive(Debug)]
pub enum StreamError {
    Reset,
    WindowOverflow,
    ContentLengthMismatch,
    Protocol,
}

pub struct Stream {
    pub headers: Headers,
    pub output: SharedOutput,
    receiver: Receiver<InputStreamEvent>,
    send_window: SendWindow,
    state: StreamState,
}

struct Chunks();

pub enum StreamState {
    HeadersOnly,
    StreamingBody(Chunks),
    AccumulatingBody { len: usize },
}

impl StreamState {
    fn from_headers(headers: &Headers) -> Self {
        match headers
            .fields()
            .get(http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<usize>().ok())
        {
            Some(len) => Self::AccumulatingBody { len },
            None => Self::StreamingBody(Chunks()),
        }
    }
}

impl Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stream")
            .field("headers", &self.headers)
            .finish()
    }
}

impl Stream {
    pub fn new(
        headers: Headers,
        output: SharedOutput,
        receiver: Receiver<InputStreamEvent>,
        initial_window_size: u32,
    ) -> Self {
        let state = StreamState::from_headers(&headers);
        Self {
            headers,
            output,
            receiver,
            send_window: SendWindow::init(initial_window_size),
            state,
        }
    }
}

pub async fn handle_stream(mut stream: Stream) {
    match stream.state {
        StreamState::HeadersOnly => todo!(),
        StreamState::StreamingBody(chunks) => todo!(),
        StreamState::AccumulatingBody { len } => todo!(),
    }
}
