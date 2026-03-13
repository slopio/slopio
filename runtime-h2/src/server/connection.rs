use std::collections::HashMap;

use runtime_core::channels::{Receiver, unbounded_channel};
use runtime_core::io_driver::operations::sockets::{SocketWrapper, multi_recv::MultiRecv};
use runtime_core::runtime::select::{SelectResult, select};
use runtime_core::runtime::spawn;
use runtime_streams::StreamExt;

use crate::codec::frames::headers::Headers;
use crate::server::StreamMapState;
use std::io::IoSlice;
use runtime_core::io_driver::operations::sockets::send_vectored::socket_send_vectored;
use crate::server::output::{OutputFrame, SharedOutput};
use crate::server::stream::events_in::{InputStreamEvent, WindowModification};
use crate::server::stream::{Handler, Stream, handle_stream};
use crate::{
    codec::frames::{FrameDecoder, stream_id::StreamId},
    server::{MAX_CONCURRENT_STREAMS, StreamState},
    types::{
        COMPRESSION_ERROR, FLOW_CONTROL_ERROR, FRAME_SIZE_ERROR, Frame, FrameError, NO_ERROR,
        PROTOCOL_ERROR, REFUSED_STREAM, STREAM_CLOSED, Setting,
    },
};

pub const DEFAULT_WINDOW_SIZE: u32 = 65535;

enum ConnectionState {
    Alive,
    Draining,
    Closed,
}

pub struct Socket {
    socket: SocketWrapper,
    recv: MultiRecv,
}

struct PeerSettings {
    header_table_size: u32,
    enable_push: bool,
    max_concurrent_streams: u32,
    initial_window_size: u32,
    max_frame_size: u32,
    max_header_list_size: u32,
}

impl Default for PeerSettings {
    fn default() -> Self {
        Self {
            header_table_size: 4096,
            enable_push: true,
            max_concurrent_streams: u32::MAX,
            initial_window_size: DEFAULT_WINDOW_SIZE,
            max_frame_size: 16_384,
            max_header_list_size: u32::MAX,
        }
    }
}

pub struct Connection<H> {
    recv_window: u32,
    send_window: u32,
    socket: Socket,
    decoder: FrameDecoder,
    streams: HashMap<StreamId, StreamMapState>,
    max_stream_id: StreamId,
    active_streams: u32,
    output_receiver: Receiver<OutputFrame>,
    output: SharedOutput,
    state: ConnectionState,
    peer_settings: PeerSettings,
    handler: H,
}

fn validate_request_headers(headers: &Headers) -> Result<(), FrameError> {
    let pseudo = headers.pseudo();
    if pseudo.status.is_some() {
        return Err(FrameError::ProtocolError(
            "response pseudo-header in request",
        ));
    }
    if pseudo.method.is_none() {
        return Err(FrameError::ProtocolError("missing :method"));
    }
    if pseudo.scheme.is_none() {
        return Err(FrameError::ProtocolError("missing :scheme"));
    }
    match &pseudo.path {
        None => return Err(FrameError::ProtocolError("missing :path")),
        Some(p) if p.as_ref().is_empty() => {
            return Err(FrameError::ProtocolError("empty :path"));
        }
        _ => {}
    }
    Ok(())
}

fn validate_frame(frame: &Frame) -> Result<(), FrameError> {
    match frame {
        Frame::Headers(headers) => {
            if let Some(dep) = headers.stream_dep()
                && dep.dependency_id == headers.stream_id()
            {
                return Err(FrameError::ProtocolError("stream depends on itself"));
            }
        }
        Frame::Settings { ack, settings } => {
            if !ack {
                for setting in settings {
                    match setting {
                        Setting::EnablePush(v) if *v > 1 => {
                            return Err(FrameError::ProtocolError(
                                "SETTINGS_ENABLE_PUSH must be 0 or 1",
                            ));
                        }
                        Setting::InitialWindowSize(v) if *v > 0x7FFF_FFFF => {
                            return Err(FrameError::FlowControlError(
                                "SETTINGS_INITIAL_WINDOW_SIZE exceeds maximum",
                            ));
                        }
                        Setting::MaxFrameSize(v) if *v < 16_384 || *v > 16_777_215 => {
                            return Err(FrameError::ProtocolError(
                                "SETTINGS_MAX_FRAME_SIZE out of range",
                            ));
                        }
                        _ => {}
                    }
                }
            }
        }
        Frame::WindowUpdate { increment, .. } if *increment == 0 => {
            return Err(FrameError::ProtocolError("WINDOW_UPDATE with 0 increment"));
        }
        _ => {}
    }
    Ok(())
}

impl<H: Handler + Clone + 'static> Connection<H> {
    #[must_use]
    pub fn new(socket: SocketWrapper, handler: H) -> Self {
        let (output_sender, output_receiver) = unbounded_channel::<OutputFrame>();
        Self {
            recv_window: DEFAULT_WINDOW_SIZE,
            send_window: DEFAULT_WINDOW_SIZE,
            socket: {
                let recv = MultiRecv::new(socket.fd(), runtime_core::io_driver::BufGroup::Small);
                Socket { socket, recv }
            },
            decoder: FrameDecoder::new(),
            streams: HashMap::new(),
            max_stream_id: StreamId::ZERO,
            active_streams: 0,
            output: SharedOutput::new(output_sender),
            output_receiver,
            state: ConnectionState::Alive,
            peer_settings: PeerSettings::default(),
            handler,
        }
    }

    async fn send_output(&mut self, frames: Vec<OutputFrame>) -> std::io::Result<()> {
        let mut bufs: Vec<Vec<u8>> = Vec::with_capacity(frames.len());
        for frame in frames {
            match frame {
                OutputFrame::StreamDone(stream_id) => {
                    self.streams.remove(&stream_id);
                    match self.state {
                        ConnectionState::Alive => {
                            self.active_streams -= 1;
                        }
                        ConnectionState::Draining => {
                            self.active_streams -= 1;
                            if self.active_streams == 0 {
                                self.state = ConnectionState::Closed;
                            }
                        }
                        ConnectionState::Closed => {}
                    }
                }
                OutputFrame::WindowUpdate {
                    stream_id,
                    increment,
                    frame: data,
                } if stream_id.is_zero() => {
                    self.recv_window += increment;
                    bufs.push(data);
                }
                OutputFrame::WindowUpdate { frame: data, .. } => {
                    bufs.push(data);
                }
                OutputFrame::EncodedData(data) => {
                    self.send_window -= (data.len() - 9) as u32;
                    bufs.push(data);
                }
                OutputFrame::EncodedHeaders(data) => {
                    bufs.push(data);
                }
            }
        }
        if bufs.is_empty() {
            return Ok(());
        }
        let total: usize = bufs.iter().map(|b| b.len()).sum();
        let mut sent = 0;
        while sent < total {
            let mut offset = sent;
            let mut slices = Vec::new();
            for buf in &bufs {
                if offset >= buf.len() {
                    offset -= buf.len();
                    continue;
                }
                slices.push(IoSlice::new(&buf[offset..]));
                offset = 0;
            }
            let n = socket_send_vectored(&self.socket.socket, &slices).await?;
            sent += n;
        }
        Ok(())
    }

    fn start_closing(&mut self) {
        if self.active_streams == 0 {
            self.state = ConnectionState::Closed
        } else {
            self.state = ConnectionState::Draining
        }
    }

    pub async fn run(&mut self) -> Result<(), FrameError> {
        let result = self.run_inner().await;
        if let Err(ref e) = result {
            let error_code = match e {
                FrameError::FrameSizeError(_) => FRAME_SIZE_ERROR,
                FrameError::ProtocolError(_) => PROTOCOL_ERROR,
                FrameError::FlowControlError(_) => FLOW_CONTROL_ERROR,
                FrameError::Hpack(_) => COMPRESSION_ERROR,
                FrameError::Io(_) => NO_ERROR,
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
            match &self.state {
                ConnectionState::Alive => {
                    match select(self.socket.recv.next(), self.output_receiver.recv_all()).await {
                        SelectResult::First(Some(v)) => {
                            self.process_incoming(v.as_ref())?;
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
                ConnectionState::Closed => {
                    break;
                }
                ConnectionState::Draining => {
                    if let Some(frames) = self.output_receiver.recv_all().await {
                        self.send_output(frames).await?;
                    }
                }
            }
        }
        Ok(())
    }

    fn handle_headers_frame(&mut self, headers: Headers) -> Result<(), FrameError> {
        let end_of_stream = headers.is_end_stream();
        match self.streams.get_mut(&headers.stream_id()) {
            Some(stream) => match stream.state {
                StreamState::Open => {
                    if !end_of_stream {
                        return Err(FrameError::ProtocolError(
                            "trailing HEADERS without END_STREAM",
                        ));
                    }
                    stream.sender.send(InputStreamEvent::Header { headers });
                    stream.state = StreamState::HalfClosedRemote;
                    Ok(())
                }
                StreamState::HalfClosedRemote | StreamState::Closed => {
                    Err(FrameError::ProtocolError("closed stream"))
                }
            },
            None => {
                let stream_id = headers.stream_id();
                if !stream_id.is_client_initiated() {
                    return Err(FrameError::ProtocolError("even-numbered stream identifier"));
                }
                validate_request_headers(&headers)?;
                if stream_id <= self.max_stream_id {
                    self.output.send_rst_stream(stream_id, STREAM_CLOSED);
                    self.output.send_goaway(self.max_stream_id, STREAM_CLOSED);
                    self.start_closing();
                    return Ok(());
                }
                self.max_stream_id = stream_id;

                if self.active_streams >= MAX_CONCURRENT_STREAMS {
                    self.output.send_rst_stream(stream_id, REFUSED_STREAM);
                    return Ok(());
                }

                let (sender, receiver) = unbounded_channel();
                self.streams.insert(
                    stream_id,
                    StreamMapState {
                        sender,
                        state: if end_of_stream {
                            StreamState::HalfClosedRemote
                        } else {
                            StreamState::Open
                        },
                    },
                );

                self.active_streams += 1;
                let handler = self.handler.clone();
                spawn(handle_stream(
                    Stream::new(
                        headers,
                        self.output.clone(),
                        receiver,
                        self.peer_settings.initial_window_size,
                    ),
                    handler,
                ));

                Ok(())
            }
        }
    }

    fn handle_data_frame(
        &mut self,
        stream_id: StreamId,
        data: Vec<u8>,
        end_stream: bool,
    ) -> Result<(), FrameError> {
        let stream = self
            .streams
            .get_mut(&stream_id)
            .ok_or(FrameError::ProtocolError("DATA on unknown stream"))?;
        match stream.state {
            StreamState::Open => {
                if data.len() as u32 > self.recv_window {
                    return Err(FrameError::FlowControlError(
                        "connection flow control overflow",
                    ));
                }
                self.recv_window -= data.len() as u32;

                if let Some(f) = InputStreamEvent::from_frame(Frame::Data {
                    stream_id,
                    end_stream,
                    data,
                }) {
                    stream.sender.send(f)
                }

                if end_stream {
                    stream.state = StreamState::Closed;
                }
            }
            StreamState::HalfClosedRemote | StreamState::Closed => {
                //TODO can do both at the same time.
                self.output.send_rst_stream(stream_id, STREAM_CLOSED);
                self.output.send_goaway(self.max_stream_id, STREAM_CLOSED);
                self.start_closing();
                return Ok(());
            }
        }
        Ok(())
    }

    fn handle_reset_stream_frame(&mut self, stream_id: StreamId) -> Result<(), FrameError> {
        let stream = self
            .streams
            .get_mut(&stream_id)
            .ok_or(FrameError::ProtocolError("RST_STREAM on unknown stream"))?;
        stream.sender.purge(); //we don't need the other messages anymore
        stream.sender.send(InputStreamEvent::ResetFrame);
        stream.state = StreamState::Closed;
        Ok(())
    }

    fn handle_window_update(
        &mut self,
        stream_id: StreamId,
        increment: u32,
    ) -> Result<(), FrameError> {
        if stream_id.is_zero() {
            self.send_window = self
                .send_window
                .checked_add(increment)
                .ok_or(FrameError::FlowControlError("connection window overflow"))?;
            return Ok(());
        }
        let stream = self
            .streams
            .get_mut(&stream_id)
            .ok_or(FrameError::ProtocolError("WINDOW_UPDATE on unknown stream"))?;
        match stream.state {
            StreamState::Open | StreamState::HalfClosedRemote => {
                stream
                    .sender
                    .send(InputStreamEvent::Window(WindowModification::Increment(
                        increment,
                    )));
            }
            StreamState::Closed => {}
        }
        Ok(())
    }

    fn handle_settings(&mut self, settings: &[Setting]) {
        for setting in settings {
            match *setting {
                Setting::HeaderTableSize(v) => {
                    self.peer_settings.header_table_size = v;
                }
                Setting::EnablePush(v) => {
                    self.peer_settings.enable_push = v != 0;
                }
                Setting::MaxConcurrentStreams(v) => {
                    self.peer_settings.max_concurrent_streams = v;
                }
                Setting::InitialWindowSize(v) => {
                    let old = self.peer_settings.initial_window_size;
                    self.peer_settings.initial_window_size = v;
                    if old != v {
                        for stream in self.streams.values() {
                            stream.sender.send(InputStreamEvent::Window(
                                WindowModification::InitialSizeChange { old, new: v },
                            ));
                        }
                    }
                }
                Setting::MaxFrameSize(v) => {
                    self.peer_settings.max_frame_size = v;
                }
                Setting::MaxHeaderListSize(v) => {
                    self.peer_settings.max_header_list_size = v;
                }
                Setting::Unknown { .. } => {}
            }
        }
        self.output.send_settings_ack();
    }

    fn process_incoming(&mut self, data: &[u8]) -> Result<(), FrameError> {
        self.decoder.decode(data)?;
        while let Some(frame) = self.decoder.frames.pop_front() {
            validate_frame(&frame)?;
            match frame {
                Frame::Headers(headers) => {
                    self.handle_headers_frame(headers)?;
                }
                Frame::Data {
                    stream_id,
                    data,
                    end_stream,
                } => {
                    self.handle_data_frame(stream_id, data, end_stream)?;
                }
                Frame::RstStream { stream_id, .. } => self.handle_reset_stream_frame(stream_id)?,
                Frame::Ping { ack, opaque } => {
                    if !ack {
                        self.output.send_ping_ack(&opaque);
                    }
                }
                Frame::GoAway { .. } => {
                    self.start_closing();
                    return Ok(());
                }
                Frame::WindowUpdate {
                    stream_id,
                    increment,
                } => {
                    self.handle_window_update(stream_id, increment)?;
                }
                Frame::Settings { ack, settings } => {
                    if !ack {
                        self.handle_settings(&settings);
                    }
                }
            }
        }
        Ok(())
    }
}
