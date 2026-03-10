use std::fmt::Debug;

use runtime_core::channels::Receiver;

use crate::codec::frames::headers::Headers;
use crate::server::output::SharedOutput;
use crate::server::stream::body::Body;
use crate::server::stream::events_in::{InputStreamEvent, WindowModification};
use crate::server::stream::window::SendWindow;
use crate::types::{FLOW_CONTROL_ERROR, PROTOCOL_ERROR};

mod body;
pub mod events_in;
mod window;

enum StreamError {
    Reset,
    WindowOverflow,
    ContentLengthMismatch,
}

pub struct Stream {
    pub headers: Headers,
    pub output: SharedOutput,
    receiver: Receiver<InputStreamEvent>,
    send_window: SendWindow,
    body: Body,
}

impl Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stream")
            .field("headers", &self.headers)
            .field("body", &self.body)
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
        let body = if headers.is_end_stream() {
            Body::done()
        } else {
            let content_length = headers
                .fields()
                .get(http::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            Body::receiving(content_length)
        };
        Stream {
            headers,
            output,
            receiver,
            send_window: SendWindow::init(initial_window_size),
            body,
        }
    }

    /// Drain window events that are already buffered on the receiver (non-blocking).
    fn drain_window_events(&mut self) -> Result<(), StreamError> {
        if let Some(events) = self.receiver.try_recv_all() {
            for event in events {
                if let InputStreamEvent::Window(modification) = event {
                    match modification {
                        WindowModification::Increment(inc) => {
                            if !self.send_window.add_ok(inc) {
                                return Err(StreamError::WindowOverflow);
                            }
                        }
                        WindowModification::InitialSizeChange { old, new } => {
                            if !self.send_window.adjust_initial(old, new) {
                                return Err(StreamError::WindowOverflow);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Wait for enough send window to send data.
    async fn wait_for_window(&mut self) -> Result<(), StreamError> {
        while self.send_window.get() <= 0 {
            match self.receiver.recv().await {
                Some(InputStreamEvent::Window(modification)) => match modification {
                    WindowModification::Increment(inc) => {
                        if !self.send_window.add_ok(inc) {
                            return Err(StreamError::WindowOverflow);
                        }
                    }
                    WindowModification::InitialSizeChange { old, new } => {
                        if !self.send_window.adjust_initial(old, new) {
                            return Err(StreamError::WindowOverflow);
                        }
                    }
                },
                Some(InputStreamEvent::ResetFrame) | None => return Err(StreamError::Reset),
                _ => {}
            }
        }
        Ok(())
    }
}

pub async fn handle_stream(mut stream: Stream) {
    use crate::codec::frames::headers::{Headers, Pseudo};
    use crate::codec::frames::stream_id::StreamId;
    use http::{HeaderMap, StatusCode, header};

    let stream_id = stream.headers.stream_id();

    loop {
        match stream
            .body
            .next(&stream.receiver, &mut stream.send_window)
            .await
        {
            Ok(Some(chunk)) => {
                let len = chunk.len() as u32;
                stream.output.send_window_update(stream_id, len);
                stream.output.send_window_update(StreamId::ZERO, len);
            }
            Ok(None) => break,
            Err(StreamError::WindowOverflow) => {
                stream.output.send_rst_stream(stream_id, FLOW_CONTROL_ERROR);
                stream.output.send_stream_done(stream_id);
                return;
            }
            Err(StreamError::ContentLengthMismatch) => {
                stream.output.send_rst_stream(stream_id, PROTOCOL_ERROR);
                stream.output.send_stream_done(stream_id);
                return;
            }
            Err(StreamError::Reset) => {
                stream.output.send_stream_done(stream_id);
                return;
            }
        }
    }

    // Drain any buffered window events before sending
    if stream.drain_window_events().is_err() {
        stream.output.send_rst_stream(stream_id, FLOW_CONTROL_ERROR);
        stream.output.send_stream_done(stream_id);
        return;
    }

    // Send response headers
    let pseudo = Pseudo::response(StatusCode::OK);
    let mut fields = HeaderMap::new();
    fields.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
    let response_headers = Headers::new(stream_id, pseudo, fields);
    stream.output.send_headers(&response_headers);

    // Send response body with flow control
    let data = b"hello from h2 stream!".to_vec();
    let mut offset = 0;
    while offset < data.len() {
        match stream.wait_for_window().await {
            Err(StreamError::WindowOverflow) => {
                stream.output.send_rst_stream(stream_id, FLOW_CONTROL_ERROR);
                stream.output.send_stream_done(stream_id);
                return;
            }
            Err(StreamError::Reset | StreamError::ContentLengthMismatch) => {
                stream.output.send_stream_done(stream_id);
                return;
            }
            Ok(()) => {}
        }
        let available = stream.send_window.get() as usize;
        let end = (offset + available).min(data.len());
        let chunk = data[offset..end].to_vec();
        let end_stream = end == data.len();
        let chunk_len = chunk.len() as i64;
        stream.output.send_data(stream_id, end_stream, chunk);
        stream.send_window.consume(chunk_len);
        offset = end;
    }

    stream.output.send_stream_done(stream_id);
}
