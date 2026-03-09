use std::{fmt::Debug, mem};

use runtime_core::channels::Receiver;

use crate::{codec::frames::headers::Headers, types::Frame};
use crate::server::output::SharedOutput;

pub struct Stream {
    pub headers: Headers,
    pub output: SharedOutput,
    state: BodyState,
}

impl Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stream")
            .field("headers", &self.headers)
            .field("state", &self.state)
            .finish()
    }
}

enum BodyState {
    None,
    Receiving(Receiver<Frame>),
    Done,
}

impl Stream {
    pub fn new(headers: Headers, output: SharedOutput, receiver: Receiver<Frame>) -> Self {
        Stream {
            headers,
            output,
            state: BodyState::Receiving(receiver),
        }
    }

    pub fn headers_only(headers: Headers, output: SharedOutput) -> Self {
        Stream {
            headers,
            output,
            state: BodyState::None,
        }
    }

    /// Async iterator over body chunks. Returns None when the body is complete.
    pub async fn next(&mut self) -> Option<Vec<u8>> {
        match &self.state {
            BodyState::None => {
                self.state = BodyState::Done;
                None
            }
            BodyState::Done => None,
            BodyState::Receiving(_) => {
                // Take the receiver out temporarily
                let old = mem::replace(&mut self.state, BodyState::Done);
                let BodyState::Receiving(receiver) = old else {
                    unreachable!()
                };

                while let Some(frame) = receiver.recv().await {
                    match frame {
                        Frame::Data {
                            data, end_stream, ..
                        } => {
                            if end_stream {
                                // Body complete, stay Done
                            } else {
                                // Put receiver back for next call
                                self.state = BodyState::Receiving(receiver);
                            }
                            return Some(data);
                        }
                        Frame::RstStream { .. } => {
                            return None;
                        }
                        _ => {}
                    }
                }
                // Channel closed (sender dropped)
                None
            }
        }
    }
}

impl Debug for BodyState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Receiving(_) => write!(f, "Receiving"),
            Self::Done => write!(f, "Done"),
        }
    }
}

pub async fn handle_stream(mut stream: Stream) {
    use crate::codec::frames::headers::{Headers, Pseudo};
    use crate::codec::frames::stream_id::StreamId;
    use http::{HeaderMap, StatusCode, header};

    let stream_id = stream.headers.stream_id();

    while let Some(chunk) = stream.next().await {
        let len = chunk.len() as u32;
        stream.output.send_window_update(stream_id, len);
        stream.output.send_window_update(StreamId::ZERO, len);
    }

    // Send response
    let pseudo = Pseudo::response(StatusCode::OK);
    let mut fields = HeaderMap::new();
    fields.insert(header::CONTENT_TYPE, "text/plain".parse().unwrap());
    let response_headers = Headers::new(stream_id, pseudo, fields);
    stream.output.send_headers(&response_headers);

    let body = b"hello from h2 stream!".to_vec();
    stream.output.send_data(stream_id, true, body);
}
