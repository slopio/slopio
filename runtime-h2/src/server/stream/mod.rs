use std::fmt::Debug;

use runtime_core::channels::Receiver;
use runtime_streams::StreamExt;

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

pub enum StreamState {
    HeadersOnly,
    StreamingBody,
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
            None => Self::StreamingBody,
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

    /// Pulls the next data chunk from the stream.
    /// Returns `Ok(Some(data))` for each DATA frame,
    /// `Ok(None)` when END_STREAM is received or sender is gone,
    /// `Err` on reset or window overflow.
    pub async fn recv_data(&mut self) -> Result<Option<Vec<u8>>, StreamError> {
        loop {
            match self.receiver.recv().await {
                Some(InputStreamEvent::Data { data, end_stream }) => {
                    if end_stream {
                        if data.is_empty() {
                            return Ok(None);
                        }
                        // TODO: you may want to signal end_stream separately
                        return Ok(Some(data));
                    }
                    return Ok(Some(data));
                }
                Some(InputStreamEvent::Window(modification)) => {
                    use events_in::WindowModification;
                    match modification {
                        WindowModification::Increment(inc) => self.send_window.add(inc)?,
                        WindowModification::InitialSizeChange { old, new } => {
                            self.send_window.adjust_initial(old, new)?
                        }
                    }
                    // loop again — not a data event
                }
                Some(InputStreamEvent::Header { .. }) => {
                    // trailers — treat as end of body
                    return Ok(None);
                }
                Some(InputStreamEvent::ResetFrame) => return Err(StreamError::Reset),
                None => return Ok(None),
            }
        }
    }

    fn write_err(&mut self, error: StreamError) {}
    fn write_response(&mut self, response: Response) {}
}

pub async fn handle_stream(mut stream: Stream) {
    let res: Result<Option<Response>, StreamError> = {
        match stream.state {
            StreamState::HeadersOnly => process_headers_only(&stream.headers).await,
            StreamState::StreamingBody => process_stream(&mut stream).await,
            StreamState::AccumulatingBody { len } => {
                let mut acc = Vec::with_capacity(len);
                while let Ok(Some(data)) = stream.recv_data().await {
                    acc.extend_from_slice(&data);
                }
                if len == acc.len() {
                    process_accumulated_body(&stream.headers, acc).await
                } else {
                    Err(StreamError::ContentLengthMismatch)
                }
            }
        }
    };
    match res {
        Ok(Some(response)) => stream.write_response(response),
        Ok(None) => {}
        Err(err) => stream.write_err(err),
    }
}

struct Response;

async fn process_headers_only(stream: &Headers) -> Result<Option<Response>, StreamError> {
    Ok(None)
}

async fn process_accumulated_body(
    headers: &Headers,
    body: Vec<u8>,
) -> Result<Option<Response>, StreamError> {
    Ok(None)
}

async fn process_stream(stream: &mut Stream) -> Result<Option<Response>, StreamError> {
    Ok(None)
}
