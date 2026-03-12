use std::fmt::Debug;
use std::future::Future;
use std::thread::yield_now;

use runtime_core::channels::Receiver;

use http::{HeaderMap, StatusCode};

use crate::codec::frames::headers::{Headers, Pseudo};
use crate::codec::frames::stream_id::StreamId;
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

pub trait Handler {
    fn process_headers_only(
        &self,
        headers: &Headers,
    ) -> impl Future<Output = Result<Option<Response>, StreamError>>;

    fn process_accumulated_body(
        &self,
        headers: &Headers,
        body: Vec<u8>,
    ) -> impl Future<Output = Result<Option<Response>, StreamError>>;

    fn process_stream(
        &self,
        stream: &mut Stream,
    ) -> impl Future<Output = Result<Option<Response>, StreamError>>;
}

#[derive(Debug)]
pub struct Response {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Option<Vec<u8>>,
}

pub struct Stream {
    pub headers: Headers,
    pub output: SharedOutput,
    receiver: Receiver<InputStreamEvent>,
    send_window: SendWindow,
    state: StreamState,
    end_of_data: bool,
}

pub enum StreamState {
    HeadersOnly,
    StreamingBody,
    AccumulatingBody { len: usize },
}

impl StreamState {
    fn from_headers(headers: &Headers) -> Self {
        if headers.is_end_stream() {
            return Self::HeadersOnly;
        }
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
            end_of_data: false,
        }
    }

    /// Pulls the next data chunk from the stream.
    /// Returns `Ok(Some(data))` for each DATA frame,
    /// `Ok(None)` when END_STREAM is received or sender is gone,
    /// `Err` on reset or window overflow.
    pub async fn recv_data(&mut self) -> Result<Option<Vec<u8>>, StreamError> {
        if self.end_of_data {
            return Ok(None);
        }
        loop {
            match self.receiver.recv().await {
                Some(InputStreamEvent::Data { data, end_stream }) => {
                    if end_stream {
                        self.end_of_data = true;
                        if data.is_empty() {
                            return Ok(None);
                        }
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

    fn write_err(&mut self, error: StreamError) {
        let stream_id = self.headers.stream_id();
        let error_code = match error {
            StreamError::Reset => crate::types::CANCEL,
            StreamError::WindowOverflow => crate::types::FLOW_CONTROL_ERROR,
            StreamError::ContentLengthMismatch => crate::types::PROTOCOL_ERROR,
            StreamError::Protocol => crate::types::PROTOCOL_ERROR,
        };
        self.output.send_rst_stream(stream_id, error_code);
        self.output.send_stream_done(stream_id);
    }

    fn drain_pending_events(&mut self) -> Result<(), StreamError> {
        if let Some(events) = self.receiver.try_recv_all() {
            for event in events {
                match event {
                    InputStreamEvent::Window(modification) => {
                        self.apply_window(modification)?;
                    }
                    InputStreamEvent::ResetFrame => {
                        return Err(StreamError::Reset);
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    fn apply_window(
        &mut self,
        modification: events_in::WindowModification,
    ) -> Result<(), StreamError> {
        use events_in::WindowModification;
        match modification {
            WindowModification::Increment(inc) => self.send_window.add(inc),
            WindowModification::InitialSizeChange { old, new } => {
                self.send_window.adjust_initial(old, new)
            }
        }
    }

    async fn wait_for_window(&mut self) -> Result<(), StreamError> {
        loop {
            if self.send_window.get() > 0 {
                return Ok(());
            }
            match self.receiver.recv().await {
                Some(InputStreamEvent::Window(modification)) => {
                    self.apply_window(modification)?;
                }
                Some(InputStreamEvent::ResetFrame) => return Err(StreamError::Reset),
                None => return Err(StreamError::Reset),
                _ => {}
            }
        }
    }

    async fn write_response(&mut self, response: Response) -> Result<(), StreamError> {
        self.drain_pending_events()?;
        let stream_id = self.headers.stream_id();
        let has_body = response.body.is_some();

        let pseudo = Pseudo::response(response.status);
        let mut headers = Headers::new(stream_id, pseudo, response.headers);
        if !has_body {
            headers.set_end_stream();
        }
        self.output.send_headers(&headers);

        if let Some(body) = response.body {
            let mut offset = 0;
            while offset < body.len() {
                self.wait_for_window().await?;
                let window = self.send_window.get() as usize;
                let chunk_len = window.min(body.len() - offset);
                let end_stream = offset + chunk_len == body.len();
                self.output.send_data(
                    stream_id,
                    end_stream,
                    body[offset..offset + chunk_len].to_vec(),
                );
                self.send_window.consume(chunk_len as i64);
                offset += chunk_len;
            }
            if body.is_empty() {
                self.output.send_data(stream_id, true, Vec::new());
            }
        }

        self.output.send_stream_done(stream_id);
        Ok(())
    }
}

pub async fn handle_stream<H: Handler>(mut stream: Stream, handler: H) {
    let res: Result<Option<Response>, StreamError> = match &stream.state {
        StreamState::HeadersOnly => handler.process_headers_only(&stream.headers).await,
        StreamState::StreamingBody => handler.process_stream(&mut stream).await,
        StreamState::AccumulatingBody { len } => {
            let len = *len;
            stream
                .output
                .send_window_update(stream.headers.stream_id(), len as u32);
            stream.output.send_window_update(StreamId::ZERO, len as u32);
            let mut acc = Vec::with_capacity(len);
            while let Ok(Some(data)) = stream.recv_data().await {
                acc.extend_from_slice(&data);
            }
            if len == acc.len() {
                handler.process_accumulated_body(&stream.headers, acc).await
            } else {
                Err(StreamError::ContentLengthMismatch)
            }
        }
    };
    match res {
        Ok(Some(response)) => {
            if let Err(err) = stream.write_response(response).await {
                stream.write_err(err);
            }
        }
        Ok(None) => {}
        Err(err) => stream.write_err(err),
    }
}
