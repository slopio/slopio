use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::rc::Rc;

use bytes::BytesMut;
use http::header::HeaderName;
use runtime_core::channels::{Receiver, Sender, unbounded_channel};
use runtime_core::io_driver::operations::sockets::{
    SocketWrapper, multi_recv::MultiRecv, send::socket_send,
    send_vectored::socket_send_vectored,
};
use runtime_core::runtime::select::{SelectResult, select};
use runtime_core::runtime::spawn;
use runtime_streams::StreamExt;

use crate::codec::frames::headers::Headers;
use crate::hpack::{self, Header};
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
    output_receiver: Receiver<OutputFrame>,
    output: SharedOutput,
    alive: bool,
}

pub enum OutputFrame {
    Encoded(Vec<u8>),
    WindowUpdate {
        stream_id: StreamId,
        increment: u32,
        frame: Vec<u8>,
    },
}

const FRAME_HEADER_LEN: usize = 9;
const END_STREAM: u8 = 0x1;
const END_HEADERS: u8 = 0x4;

struct SharedOutputInner {
    hpack: hpack::Encoder,
    hpack_buf: BytesMut,
    sender: Sender<OutputFrame>,
}

pub struct SharedOutput(Rc<UnsafeCell<SharedOutputInner>>);

impl Clone for SharedOutput {
    fn clone(&self) -> Self {
        SharedOutput(Rc::clone(&self.0))
    }
}

impl SharedOutput {
    fn new(sender: Sender<OutputFrame>) -> Self {
        SharedOutput(Rc::new(UnsafeCell::new(SharedOutputInner {
            hpack: hpack::Encoder::default(),
            hpack_buf: BytesMut::with_capacity(1024),
            sender,
        })))
    }

    fn write_frame_header(
        buf: &mut Vec<u8>,
        length: u32,
        frame_type: u8,
        flags: u8,
        stream_id: StreamId,
    ) {
        let len_bytes = length.to_be_bytes();
        buf.extend_from_slice(&len_bytes[1..4]);
        buf.push(frame_type);
        buf.push(flags);
        buf.extend_from_slice(&u32::from(stream_id).to_be_bytes());
    }

    pub fn send_headers(&self, headers: &Headers) {
        let inner = unsafe { &mut *self.0.get() };
        inner.hpack_buf.clear();

        let pseudo = headers.pseudo();
        let mut hpack_headers: Vec<Header<Option<HeaderName>>> = Vec::new();

        if let Some(ref status) = pseudo.status {
            hpack_headers.push(Header::Status(*status));
        }
        if let Some(ref method) = pseudo.method {
            hpack_headers.push(Header::Method(method.clone()));
        }
        if let Some(ref scheme) = pseudo.scheme {
            hpack_headers.push(Header::Scheme(scheme.clone()));
        }
        if let Some(ref authority) = pseudo.authority {
            hpack_headers.push(Header::Authority(authority.clone()));
        }
        if let Some(ref path) = pseudo.path {
            hpack_headers.push(Header::Path(path.clone()));
        }

        for (name, value) in headers.fields() {
            hpack_headers.push(Header::Field {
                name: Some(name.clone()),
                value: value.clone(),
            });
        }

        inner.hpack.encode(hpack_headers, &mut inner.hpack_buf);

        let mut flags = END_HEADERS;
        if headers.is_end_stream() {
            flags |= END_STREAM;
        }

        let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + inner.hpack_buf.len());
        Self::write_frame_header(
            &mut buf,
            inner.hpack_buf.len() as u32,
            0x1,
            flags,
            headers.stream_id(),
        );
        buf.extend_from_slice(&inner.hpack_buf);

        inner.sender.send(OutputFrame::Encoded(buf));
    }

    pub fn send_data(&self, stream_id: StreamId, end_stream: bool, data: Vec<u8>) {
        let flags = if end_stream { END_STREAM } else { 0 };
        let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + data.len());
        Self::write_frame_header(&mut buf, data.len() as u32, 0x0, flags, stream_id);
        buf.extend_from_slice(&data);
        let inner = unsafe { &mut *self.0.get() };
        inner.sender.send(OutputFrame::Encoded(buf));
    }

    pub fn send_window_update(&self, stream_id: StreamId, increment: u32) {
        //TODO update window here ?
        let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + 4);
        Self::write_frame_header(&mut frame, 4, 0x8, 0, stream_id);
        frame.extend_from_slice(&increment.to_be_bytes());
        let inner = unsafe { &mut *self.0.get() };
        inner.sender.send(OutputFrame::WindowUpdate {
            stream_id,
            increment,
            frame,
        });
    }
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
            output: SharedOutput::new(output_sender),
            output_receiver,
            alive: true,
        }
    }

    async fn send_output(&mut self, frames: Vec<OutputFrame>) {
        let mut bufs: Vec<Vec<u8>> = Vec::with_capacity(frames.len());
        for frame in frames {
            match frame {
                OutputFrame::Encoded(data) => {
                    bufs.push(data);
                }
                OutputFrame::WindowUpdate {
                    stream_id,
                    increment,
                    frame: data,
                } => {
                    if stream_id == StreamId::ZERO {
                        self.window += increment;
                    } else if let Some(stream) = self.streams.get_mut(&stream_id) {
                        stream.window += increment;
                    }
                    bufs.push(data);
                }
            }
        }
        if !bufs.is_empty() {
            let slices: Vec<std::io::IoSlice<'_>> = bufs.iter()
                .map(|b| std::io::IoSlice::new(b))
                .collect();
            let total: usize = bufs.iter().map(|b| b.len()).sum();
            let mut sent = 0;
            while sent < total {
                let n = socket_send_vectored(&self.socket.socket, &slices)
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
                    // Drain pending output before going back to select
                    if let Some(frames) = self.output_receiver.try_recv_all() {
                        self.send_output(frames).await;
                    }
                }
                SelectResult::First(None) => {
                    break;
                }
                SelectResult::Second(Some(frames)) => {
                    self.send_output(frames).await;
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
                    }
                }
                Frame::Settings { ack, settings } => {
                    //TODO
                }
                Frame::Ping { ack, opaque } => {
                    //TODO reply
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
                    }
                }
            }
        }
        Ok(())
    }
}
