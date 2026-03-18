pub mod grpc;
#[cfg(feature = "tracing")]
pub mod otlp_collector;

use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::rc::Rc;

use http::{HeaderMap, Method, StatusCode};
use runtime_core::channels::{Receiver, Sender, unbounded_channel};
use runtime_core::io_driver::operations::sockets::{
    SocketWrapper, connect_tcp, multi_recv::MultiRecv, send::socket_send,
};
use runtime_core::runtime::select::{SelectResult, select};
use runtime_core::runtime::spawn;
use runtime_core::runtime::time::Sleep;
use runtime_streams::StreamExt;
use std::time::Duration;

use crate::codec::frames::FrameDecoder;
use crate::codec::frames::headers::{Headers, Pseudo};
use crate::codec::frames::stream_id::StreamId;
use crate::hpack::header::BytesStr;
use crate::server::output::{OutputFrame, SharedOutput};
use crate::types::{Frame, FrameError, Setting};

const CLIENT_PREFACE: &[u8] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
const DEFAULT_WINDOW_SIZE: u32 = 65535;

pub enum Body {
    /// Raw data — H2 layer prepends the 9-byte frame header (copies).
    Raw(Vec<u8>),
    /// First 9 bytes are already reserved for the H2 frame header (zero copy).
    Preframed(Vec<u8>),
}

impl Body {
    fn is_empty(&self) -> bool {
        match self {
            Body::Raw(d) => d.is_empty(),
            Body::Preframed(d) => d.len() <= 9,
        }
    }
}

// --- Shared stream map ---

struct SharedStreamsInner {
    streams: HashMap<StreamId, Sender<StreamEvent>>,
}

struct SharedStreams(Rc<UnsafeCell<SharedStreamsInner>>);

impl Clone for SharedStreams {
    fn clone(&self) -> Self {
        SharedStreams(Rc::clone(&self.0))
    }
}

impl SharedStreams {
    fn new() -> Self {
        SharedStreams(Rc::new(UnsafeCell::new(SharedStreamsInner {
            streams: HashMap::new(),
        })))
    }

    fn insert(&self, id: StreamId, sender: Sender<StreamEvent>) {
        let inner = unsafe { &mut *self.0.get() };
        inner.streams.insert(id, sender);
    }

    fn dispatch(&self, id: &StreamId, event: StreamEvent) {
        let inner = unsafe { &*self.0.get() };
        if let Some(sender) = inner.streams.get(id) {
            sender.send(event);
        }
    }

    fn remove(&self, id: &StreamId) {
        let inner = unsafe { &mut *self.0.get() };
        inner.streams.remove(id);
    }

    fn clear(&self) {
        let inner = unsafe { &mut *self.0.get() };
        inner.streams.clear();
    }
}

// --- Stream events ---

enum StreamEvent {
    Headers(Box<Headers>),
    Data { data: Vec<u8>, end_stream: bool },
    Reset(u32),
    WindowIncrement(u32),
}

// --- Public types ---

pub struct H2Client {
    dns_cache: HashMap<String, IpAddr>,
}

pub struct H2Connection {
    authority: String,
    output: SharedOutput,
    streams: SharedStreams,
    next_stream_id: StreamId,
}

pub struct ClientStream {
    stream_id: StreamId,
    output: SharedOutput,
    streams: SharedStreams,
    receiver: Receiver<StreamEvent>,
    send_window: i64,
}

pub struct Response {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Vec<u8>,
}

// --- H2Client ---

impl H2Client {
    pub fn new() -> Self {
        Self {
            dns_cache: HashMap::new(),
        }
    }

    pub async fn connect(&mut self, host: &str, port: u16) -> Result<H2Connection, FrameError> {
        let addr = self.resolve(host, port)?;
        let authority = if port == 443 {
            host.to_string()
        } else {
            format!("{}:{}", host, port)
        };
        H2Connection::connect(addr, authority).await
    }

    fn resolve(&mut self, host: &str, port: u16) -> Result<SocketAddr, FrameError> {
        if let Some(&ip) = self.dns_cache.get(host) {
            return Ok(SocketAddr::new(ip, port));
        }
        let addr = format!("{}:{}", host, port)
            .to_socket_addrs()
            .map_err(FrameError::Io)?
            .next()
            .ok_or(FrameError::ProtocolError("DNS resolution failed"))?;
        self.dns_cache.insert(host.to_string(), addr.ip());
        Ok(addr)
    }
}

impl Default for H2Client {
    fn default() -> Self {
        Self::new()
    }
}

// --- H2Connection ---

impl H2Connection {
    pub async fn connect(addr: SocketAddr, authority: String) -> Result<Self, FrameError> {
        let socket = connect_tcp(addr).map_err(FrameError::Io)?;

        send_all(&socket, CLIENT_PREFACE)
            .await
            .map_err(FrameError::Io)?;

        let settings_frame: &[u8] = &[
            0, 0, 0,    // length = 0
            0x04, // type = SETTINGS
            0x00, // flags = 0
            0, 0, 0, 0, // stream_id = 0
        ];
        send_all(&socket, settings_frame)
            .await
            .map_err(FrameError::Io)?;

        let (output_sender, output_receiver) = unbounded_channel::<OutputFrame>();
        let output = SharedOutput::new(output_sender, DEFAULT_WINDOW_SIZE);
        let streams = SharedStreams::new();
        let recv = MultiRecv::new(socket.fd(), runtime_core::io_driver::BufGroup::Small);

        let mut conn = ClientConnection {
            recv_window: DEFAULT_WINDOW_SIZE,
            socket: Socket { socket, recv },
            decoder: FrameDecoder::new(),
            streams: streams.clone(),
            output: output.clone(),
            output_receiver,
            peer_settings: PeerSettings::default(),
        };
        let streams_for_task = streams.clone();
        spawn(async move {
            if let Err(e) = conn.run().await {
                eprintln!("h2 connection error: {e:?}");
            }
            streams_for_task.clear();
        });

        let ping_output = output.clone();
        spawn(async move {
            let mut seq: u64 = 0;
            loop {
                Sleep::during(Duration::from_secs(30)).await;
                ping_output.send_ping(&seq.to_be_bytes());
                seq += 1;
            }
        });

        Ok(Self {
            authority,
            output,
            streams,
            next_stream_id: StreamId::from(1u32),
        })
    }

    pub fn request(
        &mut self,
        method: Method,
        path: &str,
        headers: HeaderMap,
        body: Option<Body>,
    ) -> Result<ClientStream, FrameError> {
        let stream_id = self.next_stream_id;
        self.next_stream_id = stream_id
            .next_id()
            .map_err(|_| FrameError::ProtocolError("stream ID exhausted"))?;

        let (sender, receiver) = unbounded_channel();
        self.streams.insert(stream_id, sender);

        let pseudo = Pseudo {
            method: Some(method),
            scheme: Some(BytesStr::from("http")),
            authority: Some(BytesStr::from(&self.authority)),
            path: Some(BytesStr::from(path)),
            protocol: None,
            status: None,
        };
        let has_body = body.as_ref().is_some_and(|b| !b.is_empty());
        let mut req_headers = Headers::new(stream_id, pseudo, headers);
        if !has_body {
            req_headers.set_end_stream();
        }
        self.output.send_headers(&req_headers);

        if let Some(body) = body
            && !body.is_empty()
        {
            match body {
                Body::Raw(data) => self.output.send_data(stream_id, true, data),
                Body::Preframed(data) => self.output.send_data_preframed(stream_id, true, data),
            }
        }

        Ok(ClientStream {
            stream_id,
            output: self.output.clone(),
            streams: self.streams.clone(),
            receiver,
            send_window: DEFAULT_WINDOW_SIZE as i64,
        })
    }
}

// --- ClientStream ---

impl ClientStream {
    fn available_window(&self) -> i64 {
        self.send_window.min(self.output.send_window())
    }

    async fn wait_for_window(&mut self) -> Result<(), FrameError> {
        loop {
            if self.available_window() > 0 {
                return Ok(());
            }
            match self.receiver.recv().await {
                Some(StreamEvent::WindowIncrement(inc)) => {
                    self.send_window = self
                        .send_window
                        .checked_add(inc as i64)
                        .filter(|&v| v <= 0x7FFF_FFFF)
                        .ok_or(FrameError::FlowControlError("stream send window overflow"))?;
                }
                Some(StreamEvent::Reset(_)) => {
                    self.streams.remove(&self.stream_id);
                    return Err(FrameError::ProtocolError("stream reset by server"));
                }
                None => {
                    return Err(FrameError::ProtocolError("connection closed"));
                }
                _ => {}
            }
        }
    }

    pub async fn send_data(&mut self, data: Vec<u8>, end_stream: bool) -> Result<(), FrameError> {
        let mut offset = 0;
        while offset < data.len() {
            self.wait_for_window().await?;
            let window = self.available_window() as usize;
            let chunk_len = window.min(data.len() - offset);
            let is_last = offset + chunk_len == data.len();
            self.output.send_data(
                self.stream_id,
                end_stream && is_last,
                data[offset..offset + chunk_len].to_vec(),
            );
            self.send_window -= chunk_len as i64;
            offset += chunk_len;
        }
        if data.is_empty() && end_stream {
            self.output.send_data(self.stream_id, true, Vec::new());
        }
        Ok(())
    }

    pub async fn recv_response(mut self) -> Result<Response, FrameError> {
        let mut status = None;
        let mut resp_headers = HeaderMap::new();
        let mut body = Vec::new();

        loop {
            match self.receiver.recv().await {
                Some(StreamEvent::Headers(headers)) => {
                    let (pseudo, fields) = headers.into_parts();
                    if let Some(s) = pseudo.status {
                        status = Some(s);
                    }
                    resp_headers = fields;
                }
                Some(StreamEvent::Data { data, end_stream }) => {
                    body.extend_from_slice(&data);
                    if end_stream {
                        break;
                    }
                }
                Some(StreamEvent::Reset(_)) => {
                    self.streams.remove(&self.stream_id);
                    return Err(FrameError::ProtocolError("stream reset by server"));
                }
                Some(StreamEvent::WindowIncrement(inc)) => {
                    self.send_window = self
                        .send_window
                        .checked_add(inc as i64)
                        .filter(|&v| v <= 0x7FFF_FFFF)
                        .ok_or(FrameError::FlowControlError("stream send window overflow"))?;
                }
                None => break,
            }
        }

        self.streams.remove(&self.stream_id);

        let status = status.ok_or(FrameError::ProtocolError("no status in response"))?;
        Ok(Response {
            status,
            headers: resp_headers,
            body,
        })
    }
}

// --- Connection task (background) ---

struct Socket {
    socket: SocketWrapper,
    recv: MultiRecv,
}

struct PeerSettings {
    initial_window_size: u32,
    max_frame_size: u32,
    max_concurrent_streams: u32,
}

impl Default for PeerSettings {
    fn default() -> Self {
        Self {
            initial_window_size: DEFAULT_WINDOW_SIZE,
            max_frame_size: 16_384,
            max_concurrent_streams: u32::MAX,
        }
    }
}

struct ClientConnection {
    recv_window: u32,
    socket: Socket,
    decoder: FrameDecoder,
    streams: SharedStreams,
    output: SharedOutput,
    output_receiver: Receiver<OutputFrame>,
    peer_settings: PeerSettings,
}

impl ClientConnection {
    async fn run(&mut self) -> Result<(), FrameError> {
        loop {
            match select(self.socket.recv.next(), self.output_receiver.recv_all()).await {
                SelectResult::First(Some(data)) => {
                    self.process_incoming(data.as_ref())?;
                    if let Some(frames) = self.output_receiver.try_recv_all() {
                        self.flush_frames(frames).await.map_err(FrameError::Io)?;
                    }
                }
                SelectResult::First(None) => break,
                SelectResult::Second(Some(frames)) => {
                    self.flush_frames(frames).await.map_err(FrameError::Io)?;
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
                Frame::Headers(headers) => self.handle_headers(headers),
                Frame::Data {
                    stream_id,
                    data,
                    end_stream,
                } => self.handle_data(stream_id, data, end_stream)?,
                Frame::WindowUpdate {
                    stream_id,
                    increment,
                } => self.handle_window_update(stream_id, increment)?,
                Frame::Settings { ack, settings } => {
                    if !ack {
                        self.handle_settings(&settings);
                    }
                }
                Frame::Ping { ack, opaque } => {
                    if !ack {
                        self.output.send_ping_ack(&opaque);
                    }
                }
                Frame::RstStream {
                    stream_id,
                    error_code,
                } => self.handle_rst_stream(stream_id, error_code),
                Frame::GoAway { .. } => {
                    return Err(FrameError::ProtocolError("server sent GOAWAY"));
                }
            }
        }
        Ok(())
    }

    fn handle_headers(&self, headers: Box<Headers>) {
        let stream_id = headers.stream_id();
        let end_stream = headers.is_end_stream();
        self.streams
            .dispatch(&stream_id, StreamEvent::Headers(headers));
        if end_stream {
            self.streams.dispatch(
                &stream_id,
                StreamEvent::Data {
                    data: Vec::new(),
                    end_stream: true,
                },
            );
        }
    }

    fn handle_data(
        &mut self,
        stream_id: StreamId,
        data: Vec<u8>,
        end_stream: bool,
    ) -> Result<(), FrameError> {
        if data.len() as u32 > self.recv_window {
            return Err(FrameError::FlowControlError("connection window overflow"));
        }
        self.recv_window -= data.len() as u32;

        self.streams
            .dispatch(&stream_id, StreamEvent::Data { data, end_stream });

        if self.recv_window < DEFAULT_WINDOW_SIZE / 2 {
            let increment = DEFAULT_WINDOW_SIZE - self.recv_window;
            self.output.send_window_update(StreamId::ZERO, increment);
            self.recv_window += increment;
        }
        Ok(())
    }

    fn handle_window_update(
        &mut self,
        stream_id: StreamId,
        increment: u32,
    ) -> Result<(), FrameError> {
        if stream_id.is_zero() {
            self.output.add_send_window(increment)?;
        }
        Ok(())
    }

    fn handle_rst_stream(&mut self, stream_id: StreamId, error_code: u32) {
        self.streams
            .dispatch(&stream_id, StreamEvent::Reset(error_code));
        self.streams.remove(&stream_id);
    }

    fn handle_settings(&mut self, settings: &[Setting]) {
        for setting in settings {
            match *setting {
                Setting::InitialWindowSize(v) => self.peer_settings.initial_window_size = v,
                Setting::MaxFrameSize(v) => self.peer_settings.max_frame_size = v,
                Setting::MaxConcurrentStreams(v) => self.peer_settings.max_concurrent_streams = v,
                _ => {}
            }
        }
        self.output.send_settings_ack();
    }

    async fn flush_frames(&mut self, frames: Vec<OutputFrame>) -> std::io::Result<()> {
        for frame in frames {
            let buf = match frame {
                OutputFrame::EncodedHeaders(data) | OutputFrame::EncodedData(data) => data,
                OutputFrame::WindowUpdate { frame: data, .. } => data,
                OutputFrame::StreamDone(_) => continue,
            };
            send_all(&self.socket.socket, &buf).await?;
        }
        Ok(())
    }
}

async fn send_all(socket: &SocketWrapper, data: &[u8]) -> std::io::Result<()> {
    let mut sent = 0;
    while sent < data.len() {
        let n = socket_send(socket, &data[sent..]).await?;
        sent += n;
    }
    Ok(())
}
