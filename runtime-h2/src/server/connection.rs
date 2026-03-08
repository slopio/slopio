use std::collections::{HashMap, VecDeque};

use runtime_core::io_driver::operations::sockets::{
    SocketWrapper, multi_recv::MultiRecv, send::socket_send,
};
use runtime_streams::StreamExt;

use crate::{
    codec::frames::{FrameDecoder, stream_id::StreamId},
    server::{StreamSendingEnd, stream::new_stream},
    types::FrameError,
};

pub struct Socket {
    socket: SocketWrapper,
    recv: MultiRecv,
}

pub struct Connection {
    window: u32,
    socket: Socket,
    decoder: FrameDecoder,
    streams: HashMap<StreamId, StreamSendingEnd>,
    alive: bool,
}

enum WindowAction {
    Ok,
    StreamOverflow,
    GlobalOverflow,
}

impl Connection {
    #[must_use]
    pub fn new(socket: SocketWrapper) -> Self {
        Self {
            window: 65535,
            socket: {
                let recv = MultiRecv::new(socket.fd(), runtime_core::io_driver::BufGroup::Small);
                Socket { socket, recv }
            },
            decoder: FrameDecoder::new(),
            streams: HashMap::new(),
            alive: true,
        }
    }

    async fn error(&mut self) {}

    async fn send(&self, data: &[u8]) {
        let mut sent = 0;
        while sent < data.len() {
            let n = socket_send(&self.socket.socket, &data[sent..])
                .await
                .unwrap();
            sent += n;
        }
    }

    fn handle_data_window(data_len: u32, global_window: u32, stream_window: u32) -> WindowAction {
        if data_len > global_window {
            // Connection-level flow control violation → GOAWAY + close
            WindowAction::GlobalOverflow
        } else if data_len > stream_window {
            // Stream-level flow control violation → RST_STREAM on that stream
            WindowAction::StreamOverflow
        } else {
            WindowAction::Ok
        }
    }

    pub async fn run(&mut self) -> Result<(), FrameError> {
        while let Some(v) = self.socket.recv.next().await {
            self.decoder.decode(v.as_ref())?;
            while let Some(frame) = self.decoder.frames.pop_front() {
                match frame {
                    crate::types::Frame::Headers(headers) => {
                        self.streams
                            .entry(headers.stream_id())
                            .or_insert(new_stream(headers));
                    }
                    crate::types::Frame::Data {
                        stream_id,
                        data,
                        end_stream,
                    } => {
                        let stream = self
                            .streams
                            .get_mut(&stream_id)
                            .ok_or(FrameError::ProtocolError("no such stream"))?;
                        let data_len = data.len() as u32;
                        match Self::handle_data_window(data_len, self.window, stream.window) {
                            WindowAction::Ok => {
                                self.window -= data_len;
                                stream.window -= data_len;
                                stream.sender.send(crate::types::Frame::Data {
                                    stream_id,
                                    end_stream,
                                    data,
                                });
                            }
                            WindowAction::StreamOverflow => {
                                self.window -= data_len;
                                stream.sender.send(crate::types::Frame::RstStream {
                                    stream_id,
                                    error_code: 0x3, // FLOW_CONTROL_ERROR
                                });
                            }
                            WindowAction::GlobalOverflow => {
                                // FLOW_CONTROL_ERROR on the connection → GOAWAY
                                return Err(FrameError::ProtocolError(
                                    "connection flow control overflow",
                                ));
                            }
                        }
                    }
                    crate::types::Frame::RstStream { stream_id, .. } => self
                        .streams
                        .get(&stream_id)
                        .ok_or(FrameError::ProtocolError("no such stream"))?
                        .sender
                        .send(frame),
                    crate::types::Frame::Settings { ack, settings } => {
                        //TODO
                    }
                    crate::types::Frame::Ping { ack, opaque } => {
                        //TODO reply
                        println!("got a ping");
                    }
                    crate::types::Frame::GoAway { .. } => {
                        //todo handle stream
                        println!("go away");
                    }
                    crate::types::Frame::WindowUpdate {
                        stream_id,
                        increment,
                    } => {
                        if stream_id == StreamId::ZERO {
                            self.window += increment;
                        } else {
                            self.streams
                                .get_mut(&stream_id)
                                .ok_or(FrameError::ProtocolError("no such stream"))?
                                .window += increment;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
