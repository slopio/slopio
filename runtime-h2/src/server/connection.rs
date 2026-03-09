use std::collections::HashMap;

use runtime_core::channels::{Receiver, unbounded_channel};
use runtime_core::io_driver::operations::sockets::{SocketWrapper, multi_recv::MultiRecv};
use runtime_core::runtime::select::{SelectResult, select};
use runtime_core::runtime::spawn;
use runtime_streams::StreamExt;

use crate::server::output::{OutputFrame, SharedOutput, flush_output};
use crate::server::stream::{Stream, handle_stream};
use crate::{
    codec::frames::{FrameDecoder, stream_id::StreamId},
    server::StreamSendingEnd,
    types::{Frame, FrameError},
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
    max_stream_id: StreamId,
    output_receiver: Receiver<OutputFrame>,
    output: SharedOutput,
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
        let (output_sender, output_receiver) = unbounded_channel::<OutputFrame>();
        Self {
            window: 65535,
            socket: {
                let recv = MultiRecv::new(socket.fd(), runtime_core::io_driver::BufGroup::Small);
                Socket { socket, recv }
            },
            decoder: FrameDecoder::new(),
            streams: HashMap::new(),
            max_stream_id: StreamId::ZERO,
            output: SharedOutput::new(output_sender),
            output_receiver,
            alive: true,
        }
    }

    async fn send_output(&mut self, frames: Vec<OutputFrame>) -> std::io::Result<()> {
        flush_output(
            &self.socket.socket,
            frames,
            &mut self.window,
            &mut self.streams,
        )
        .await
    }

    fn handle_data_window(data_len: u32, global_window: u32, stream_window: u32) -> WindowAction {
        if data_len > global_window {
            WindowAction::GlobalOverflow
        } else if data_len > stream_window {
            WindowAction::StreamOverflow
        } else {
            WindowAction::Ok
        }
    }

    pub async fn run(&mut self) -> Result<(), FrameError> {
        let result = self.run_inner().await;
        if let Err(ref e) = result {
            let error_code = match e {
                FrameError::FrameSizeError(_) => 0x6, // FRAME_SIZE_ERROR
                FrameError::ProtocolError(_) => 0x1,  // PROTOCOL_ERROR
                FrameError::Hpack(_) => 0x9,          // COMPRESSION_ERROR
                FrameError::Io(_) => 0x0,             // NO_ERROR
            };
            self.output.send_goaway(StreamId::ZERO, error_code);
            if let Some(frames) = self.output_receiver.try_recv_all() {
                let _ = self.send_output(frames).await;
            }
        }
        result
    }

    async fn run_inner(&mut self) -> Result<(), FrameError> {
        loop {
            match select(self.socket.recv.next(), self.output_receiver.recv_all()).await {
                SelectResult::First(Some(v)) => {
                    self.process_incoming(v.as_ref())?;
                    if !self.alive {
                        break;
                    }
                    // Drain pending output before going back to select
                    if let Some(frames) = self.output_receiver.try_recv_all() {
                        self.send_output(frames).await?;
                    }
                }
                SelectResult::First(None) => {
                    break;
                }
                SelectResult::Second(Some(frames)) => {
                    self.send_output(frames).await?;
                }
                SelectResult::Second(None) => {
                    break;
                }
            }
        }
        Ok(())
    }

    fn process_incoming(&mut self, data: &[u8]) -> Result<(), FrameError> {
        self.decoder.decode(data)?;
        while let Some(frame) = self.decoder.frames.pop_front() {
            match frame {
                Frame::Headers(headers) => {
                    let sid = headers.stream_id();
                    if sid <= self.max_stream_id {
                        if let Some(stream) = self.streams.get(&sid) {
                            // Trailers on an open stream
                            stream.sender.send(Frame::Headers(headers));
                            self.streams.remove(&sid);
                        } else {
                            // Stream already closed — connection error
                            self.output.send_rst_stream(sid, 0x5);
                            self.output.send_goaway(self.max_stream_id, 0x5);
                            self.alive = false;
                            return Ok(());
                        }
                        continue;
                    }
                    self.max_stream_id = sid;
                    let end_stream = headers.is_end_stream();
                    let output = self.output.clone();
                    if end_stream {
                        let stream = Stream::headers_only(headers, output);
                        spawn(handle_stream(stream));
                    } else {
                        self.streams.entry(sid).or_insert_with(|| {
                            let (sender, receiver) = unbounded_channel();
                            let stream = Stream::new(headers, output, receiver);
                            spawn(handle_stream(stream));
                            StreamSendingEnd {
                                window: 65535,
                                sender,
                            }
                        });
                    }
                }
                Frame::Data {
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
                            stream.sender.send(Frame::Data {
                                stream_id,
                                end_stream,
                                data,
                            });
                            if end_stream {
                                self.streams.remove(&stream_id);
                            }
                        }
                        WindowAction::StreamOverflow => {
                            self.window -= data_len;
                            stream.sender.send(Frame::RstStream {
                                stream_id,
                                error_code: 0x3, // FLOW_CONTROL_ERROR
                            });
                            self.streams.remove(&stream_id);
                        }
                        WindowAction::GlobalOverflow => {
                            return Err(FrameError::ProtocolError(
                                "connection flow control overflow",
                            ));
                        }
                    }
                }
                Frame::RstStream { stream_id, .. } => {
                    if let Some(stream) = self.streams.remove(&stream_id) {
                        stream.sender.send(frame);
                    } else if stream_id > self.max_stream_id {
                        return Err(FrameError::ProtocolError("RST_STREAM on idle stream"));
                    }
                }
                Frame::Settings { ack, settings } => {
                    if !ack {
                        self.output.send_settings_ack();
                    }
                }
                Frame::Ping { ack, opaque } => {
                    if !ack {
                        self.output.send_ping_ack(&opaque);
                    }
                }
                Frame::GoAway { .. } => {
                    self.alive = false;
                    return Ok(());
                }
                Frame::WindowUpdate {
                    stream_id,
                    increment,
                } => {
                    if stream_id == StreamId::ZERO {
                        self.window += increment;
                    } else if let Some(stream) = self.streams.get_mut(&stream_id) {
                        stream.window += increment;
                    } else if stream_id > self.max_stream_id {
                        return Err(FrameError::ProtocolError("WINDOW_UPDATE on idle stream"));
                    }
                }
            }
        }
        Ok(())
    }
}
