use std::fmt::Debug;

use runtime_core::channels::Receiver;

use crate::codec::frames::headers::Headers;
use crate::codec::frames::headers::Pseudo;
use crate::server::output::SharedOutput;
use crate::server::stream::body::Body;
use crate::server::stream::events_in::{InputStreamEvent, WindowModification};
use crate::server::stream::window::SendWindow;
use crate::types::{FLOW_CONTROL_ERROR, PROTOCOL_ERROR};
use http::{HeaderMap, StatusCode, header};

pub mod bod2y;
mod body;
pub mod events_in;
mod window;

#[derive(Debug)]
pub enum StreamError {
    Reset,
    WindowOverflow,
    ContentLengthMismatch,
}

pub struct Stream {
    pub headers: Headers,
    pub output: SharedOutput,
    receiver: Receiver<InputStreamEvent>,
    send_window: SendWindow,
    body: Option<Body>,
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
            None
        } else {
            match headers
                .fields()
                .get(http::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok())
            {
                Some(size) => Some(Body::Entire {
                    size: size as usize,
                }),
                None => Some(Body::Chunked),
            }
        };
        Stream {
            headers,
            output,
            receiver,
            send_window: SendWindow::init(initial_window_size),
            body,
        }
    }

    fn drain_window_events(&mut self) -> Result<(), StreamError> {
        if let Some(events) = self.receiver.try_recv_all() {
            for event in events {
                if let InputStreamEvent::Window(modification) = event {
                    match modification {
                        WindowModification::Increment(inc) => {
                            self.send_window.add(inc)?;
                        }
                        WindowModification::InitialSizeChange { old, new } => {
                            self.send_window.adjust_initial(old, new)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_error(&self, error: StreamError) {
        let stream_id = self.headers.stream_id();
        match error {
            StreamError::WindowOverflow => {
                self.output.send_rst_stream(stream_id, FLOW_CONTROL_ERROR);
            }
            StreamError::ContentLengthMismatch => {
                self.output.send_rst_stream(stream_id, PROTOCOL_ERROR);
            }
            StreamError::Reset => {}
        }
        self.output.send_stream_done(stream_id);
    }

    async fn wait_for_window(&mut self) -> Result<(), StreamError> {
        while self.send_window.get() <= 0 {
            match self.receiver.recv().await {
                Some(InputStreamEvent::Window(modification)) => match modification {
                    WindowModification::Increment(inc) => {
                        self.send_window.add(inc)?;
                    }
                    WindowModification::InitialSizeChange { old, new } => {
                        self.send_window.adjust_initial(old, new)?;
                    }
                },
                Some(InputStreamEvent::ResetFrame) | None => return Err(StreamError::Reset),
                _ => {}
            }
        }
        Ok(())
    }
}

pub fn handle_non_data(stream: &mut Stream, event: InputStreamEvent) -> Result<(), StreamError> {
    match event {
        InputStreamEvent::Header { .. } => unimplemented!(),
        InputStreamEvent::Window(window_modification) => match window_modification {
            WindowModification::Increment(increment) => stream.send_window.add(increment)?,
            WindowModification::InitialSizeChange { old, new } => {
                stream.send_window.adjust_initial(old, new)?
            }
        },
        InputStreamEvent::ResetFrame => return Err(StreamError::Reset),
        _ => unreachable!(),
    }
    Ok(())
}

pub async fn handle_stream(mut stream: Stream) {
    let k: Result<(), StreamError> = {
        let stream_id = stream.headers.stream_id();

        match stream.body {
            Some(Body::Chunked) => while let Some(event) = stream.receiver.recv().await {},
            Some(Body::Entire { size }) => {
                let mut body = Vec::with_capacity(size);
                while let Some(event) = stream.receiver.recv().await {
                    match event {
                        InputStreamEvent::Data { end_stream, data } => {
                            body.extend_from_slice(&data);
                            if end_stream {
                                unimplemented!()
                            }
                        }
                        e => {
                            handle_non_data(&mut stream, e).unwrap();
                        }
                    }
                }
            }
            None => {}
        }

        let pseudo = Pseudo::response(StatusCode::OK);
        let mut fields = HeaderMap::new();
        fields.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
        let response_headers = Headers::new(stream_id, pseudo, fields);
        stream.output.send_headers(&response_headers);

        let data = b"hello from h2 stream!".to_vec();
        let mut offset = 0;
        while offset < data.len() {
            if let Err(e) = stream.wait_for_window().await {
                stream.handle_error(e);
                return;
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
        Ok(())
    };
}
