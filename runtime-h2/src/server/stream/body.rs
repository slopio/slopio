use std::fmt::Debug;

use runtime_core::channels::Receiver;

use crate::server::stream::StreamError;
use crate::server::stream::events_in::{InputStreamEvent, WindowModification};
use crate::server::stream::window::SendWindow;

enum BodyState {
    Receiving,
    Done,
}

pub struct Body {
    state: BodyState,
    expected_len: Option<u64>,
    received_len: u64,
}

impl Body {
    pub fn done() -> Self {
        Self {
            state: BodyState::Done,
            expected_len: None,
            received_len: 0,
        }
    }

    pub fn receiving(expected_len: Option<u64>) -> Self {
        Self {
            state: BodyState::Receiving,
            expected_len,
            received_len: 0,
        }
    }

    /// Async iterator over body chunks. Returns None when the body is complete.
    pub async fn next(
        &mut self,
        receiver: &Receiver<InputStreamEvent>,
        send_window: &mut SendWindow,
    ) -> Result<Option<Vec<u8>>, StreamError> {
        match self.state {
            BodyState::Receiving => {
                while let Some(s) = receiver.recv().await {
                    match s {
                        InputStreamEvent::Header { headers } => {
                            if headers.is_end_headers() {
                                self.state = BodyState::Done;
                                return Ok(None);
                            } else {
                                unimplemented!()
                            }
                        }
                        InputStreamEvent::Data { end_stream, data } => {
                            self.received_len += data.len() as u64;
                            if end_stream {
                                if let Some(expected) = self.expected_len
                                    && self.received_len != expected
                                {
                                    return Err(StreamError::ContentLengthMismatch);
                                }
                                self.state = BodyState::Done;
                            }
                            return Ok(Some(data));
                        }
                        InputStreamEvent::Window(window_modification) => {
                            let ok = match window_modification {
                                WindowModification::Increment(increment) => {
                                    send_window.add_ok(increment)
                                }
                                WindowModification::InitialSizeChange { old, new } => {
                                    send_window.adjust_initial(old, new)
                                }
                            };
                            if !ok {
                                return Err(StreamError::WindowOverflow);
                            }
                            continue;
                        }
                        InputStreamEvent::ResetFrame => return Err(StreamError::Reset),
                    }
                }
                self.state = BodyState::Done;
                Ok(None)
            }
            BodyState::Done => Ok(None),
        }
    }
}

impl Debug for Body {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.state {
            BodyState::Receiving => write!(f, "Receiving"),
            BodyState::Done => write!(f, "Done"),
        }
    }
}
