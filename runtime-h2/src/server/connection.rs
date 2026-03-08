use std::collections::HashMap;
use std::rc::Rc;

use runtime_core::channels::{Receiver, Sender, unbounded_channel};
use runtime_core::io_driver::operations::sockets::{
    SocketWrapper, multi_recv::MultiRecv, send::socket_send,
};
use runtime_core::runtime::select::{SelectResult, select};
use runtime_streams::StreamExt;

use crate::{
    codec::encoder::FrameEncoder,
    codec::frames::{FrameDecoder, stream_id::StreamId},
    server::{StreamSendingEnd, stream::new_stream},
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
    encoder: FrameEncoder,
    streams: HashMap<StreamId, StreamSendingEnd>,
    output_sender: Rc<Sender<Frame>>,
    output_receiver: Receiver<Frame>,
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
        let (output_sender, output_receiver) = unbounded_channel::<Frame>();
        Self {
            window: 65535,
            socket: {
                let recv = MultiRecv::new(socket.fd(), runtime_core::io_driver::BufGroup::Small);
                Socket { socket, recv }
            },
            decoder: FrameDecoder::new(),
            encoder: FrameEncoder::new(),
            streams: HashMap::new(),
            output_sender: Rc::new(output_sender),
            output_receiver,
            alive: true,
        }
    }

    async fn send_output(&mut self, frames: Vec<Frame>) {
        for frame in &frames {
            self.encoder.encode(frame);
        }
        let buf = self.encoder.take();
        if !buf.is_empty() {
            let mut sent = 0;
            while sent < buf.len() {
                let n = socket_send(&self.socket.socket, &buf[sent..])
                    .await
                    .unwrap();
                sent += n;
            }
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
        loop {
            match select(self.socket.recv.next(), self.output_receiver.recv_all()).await {
                SelectResult::First(Some(v)) => {
                    self.process_incoming(v.as_ref())?;
                    if !self.alive {
                        break;
                    }
                }
                SelectResult::First(None) => break,
                SelectResult::Second(Some(frames)) => {
                    self.send_output(frames).await;
                }
                SelectResult::Second(None) => break,
            }
        }
        Ok(())
    }

    fn process_incoming(&mut self, data: &[u8]) -> Result<(), FrameError> {
        self.decoder.decode(data)?;
        while let Some(frame) = self.decoder.frames.pop_front() {
            match frame {
                Frame::Headers(headers) => {
                    let output = Rc::clone(&self.output_sender);
                    self.streams
                        .entry(headers.stream_id())
                        .or_insert(new_stream(headers, output));
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
                        }
                        WindowAction::StreamOverflow => {
                            self.window -= data_len;
                            stream.sender.send(Frame::RstStream {
                                stream_id,
                                error_code: 0x3, // FLOW_CONTROL_ERROR
                            });
                        }
                        WindowAction::GlobalOverflow => {
                            return Err(FrameError::ProtocolError(
                                "connection flow control overflow",
                            ));
                        }
                    }
                }
                Frame::RstStream { stream_id, .. } => self
                    .streams
                    .get(&stream_id)
                    .ok_or(FrameError::ProtocolError("no such stream"))?
                    .sender
                    .send(frame),
                Frame::Settings { ack, settings } => {
                    //TODO
                }
                Frame::Ping { ack, opaque } => {
                    //TODO reply
                    println!("got a ping");
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
                    } else {
                        self.streams
                            .get_mut(&stream_id)
                            .ok_or(FrameError::ProtocolError("no such stream"))?
                            .window += increment;
                    }
                }
            }
        }
        Ok(())
    }
}
