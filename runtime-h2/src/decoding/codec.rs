use std::collections::HashMap;

use bytes::BytesMut;
use runtime_core::{
    channels::Sender,
    io_driver::operations::sockets::{SocketWrapper, recv::socket_recv_init},
};

use crate::h2_types::frame::{self, Frame, HEADER_LEN, Head, Kind, StreamId};
use crate::h2_types::hpack;

const DEFAULT_SETTINGS_HEADER_TABLE_SIZE: usize = 4096;
const DEFAULT_MAX_HEADER_LIST_SIZE: usize = 16 << 20;
const DEFAULT_MAX_FRAME_SIZE: usize = frame::DEFAULT_MAX_FRAME_SIZE as usize;
const READ_BUF_SIZE: usize = 32768;

struct Partial {
    frame: Continuable,
    buf: BytesMut,
    continuation_frames_count: usize,
}

enum Continuable {
    Headers(frame::Headers),
    PushPromise(frame::PushPromise),
}

impl Continuable {
    fn stream_id(&self) -> StreamId {
        match self {
            Continuable::Headers(h) => h.stream_id(),
            Continuable::PushPromise(p) => p.stream_id(),
        }
    }

    fn is_over_size(&self) -> bool {
        match self {
            Continuable::Headers(h) => h.is_over_size(),
            Continuable::PushPromise(p) => p.is_over_size(),
        }
    }

    fn load_hpack(
        &mut self,
        src: &mut BytesMut,
        max_header_list_size: usize,
        decoder: &mut hpack::Decoder,
    ) -> Result<(), frame::Error> {
        match self {
            Continuable::Headers(h) => h.load_hpack(src, max_header_list_size, decoder),
            Continuable::PushPromise(p) => p.load_hpack(src, max_header_list_size, decoder),
        }
    }
}

impl<T> From<Continuable> for Frame<T> {
    fn from(cont: Continuable) -> Self {
        match cont {
            Continuable::Headers(mut h) => {
                h.set_end_headers();
                h.into()
            }
            Continuable::PushPromise(mut p) => {
                p.set_end_headers();
                p.into()
            }
        }
    }
}

#[derive(Debug)]
pub enum DecodeError {
    Io(std::io::Error),
    Connection(frame::Reason),
    Stream(StreamId, frame::Reason),
}

/// Connection-level events produced by the frame reader.
pub enum ConnectionEvent {
    Settings(frame::Settings),
    Ping(frame::Ping),
    GoAway(frame::GoAway),
    WindowUpdate(u32), // connection-level window increment
}

/// Stateful frame decoder that works on a BytesMut buffer.
/// No I/O — just feed bytes and pull frames.
pub struct FrameDecoder {
    pub buf: BytesMut,
    hpack: hpack::Decoder,
    max_header_list_size: usize,
    max_frame_size: usize,
    max_continuation_frames: usize,
    partial: Option<Partial>,
}

impl FrameDecoder {
    pub fn new() -> Self {
        let max_header_list_size = DEFAULT_MAX_HEADER_LIST_SIZE;
        let max_frame_size = DEFAULT_MAX_FRAME_SIZE;
        FrameDecoder {
            buf: BytesMut::with_capacity(READ_BUF_SIZE),
            hpack: hpack::Decoder::new(DEFAULT_SETTINGS_HEADER_TABLE_SIZE),
            max_header_list_size,
            max_frame_size,
            max_continuation_frames: calc_max_continuation_frames(
                max_header_list_size,
                max_frame_size,
            ),
            partial: None,
        }
    }

    fn peek_payload_len(&self) -> Option<usize> {
        if self.buf.len() < HEADER_LEN {
            return None;
        }
        Some(
            ((self.buf[0] as usize) << 16) | ((self.buf[1] as usize) << 8) | (self.buf[2] as usize),
        )
    }

    /// Try to decode a complete frame from the buffer.
    /// Returns Ok(None) if more data is needed.
    pub fn decode(&mut self) -> Result<Option<Frame>, DecodeError> {
        let payload_len = match self.peek_payload_len() {
            Some(len) => len,
            None => return Ok(None),
        };

        if payload_len > self.max_frame_size {
            return Err(DecodeError::Connection(frame::Reason::FRAME_SIZE_ERROR));
        }

        let total = HEADER_LEN + payload_len;
        if self.buf.len() < total {
            return Ok(None);
        }

        let frame_data = self.buf.split_to(total);
        let head = Head::parse(&frame_data);
        let payload = frame_data.freeze().slice(HEADER_LEN..);

        if self.partial.is_some() && head.kind() != Kind::Continuation {
            return Err(DecodeError::Connection(frame::Reason::PROTOCOL_ERROR));
        }

        match head.kind() {
            Kind::Data => frame::Data::load(head, payload.clone())
                .map(|f| Some(f.into()))
                .map_err(|_| DecodeError::Connection(frame::Reason::PROTOCOL_ERROR)),

            Kind::Headers => self.decode_headers(head, BytesMut::from(payload.as_ref())),

            Kind::PushPromise => self.decode_push_promise(head, BytesMut::from(payload.as_ref())),

            Kind::Settings => frame::Settings::load(head, &payload)
                .map(|f| Some(f.into()))
                .map_err(|_| DecodeError::Connection(frame::Reason::PROTOCOL_ERROR)),

            Kind::Ping => frame::Ping::load(head, &payload)
                .map(|f| Some(f.into()))
                .map_err(|_| DecodeError::Connection(frame::Reason::PROTOCOL_ERROR)),

            Kind::WindowUpdate => frame::WindowUpdate::load(head, &payload)
                .map(|f| Some(f.into()))
                .map_err(|_| DecodeError::Connection(frame::Reason::PROTOCOL_ERROR)),

            Kind::Reset => frame::Reset::load(head, &payload)
                .map(|f| Some(f.into()))
                .map_err(|_| DecodeError::Connection(frame::Reason::PROTOCOL_ERROR)),

            Kind::GoAway => frame::GoAway::load(&payload)
                .map(|f| Some(f.into()))
                .map_err(|_| DecodeError::Connection(frame::Reason::PROTOCOL_ERROR)),

            Kind::Priority => {
                if head.stream_id() == 0 {
                    return Err(DecodeError::Connection(frame::Reason::PROTOCOL_ERROR));
                }
                match frame::Priority::load(head, &payload) {
                    Ok(f) => Ok(Some(f.into())),
                    Err(frame::Error::InvalidDependencyId) => Err(DecodeError::Stream(
                        head.stream_id(),
                        frame::Reason::PROTOCOL_ERROR,
                    )),
                    Err(_) => Err(DecodeError::Connection(frame::Reason::PROTOCOL_ERROR)),
                }
            }

            Kind::Continuation => self.decode_continuation(head, &payload),

            Kind::Unknown => Ok(None),
        }
    }

    fn decode_headers(&mut self, head: Head, src: BytesMut) -> Result<Option<Frame>, DecodeError> {
        match frame::Headers::load(head, src) {
            Ok((mut frame, mut hdr_payload)) => {
                let is_end = frame.is_end_headers();
                match frame.load_hpack(&mut hdr_payload, self.max_header_list_size, &mut self.hpack)
                {
                    Ok(_) => {}
                    Err(frame::Error::Hpack(hpack::DecoderError::NeedMore(_))) if !is_end => {}
                    Err(frame::Error::MalformedMessage) => {
                        return Err(DecodeError::Stream(
                            head.stream_id(),
                            frame::Reason::PROTOCOL_ERROR,
                        ));
                    }
                    Err(_) => {
                        return Err(DecodeError::Connection(frame::Reason::PROTOCOL_ERROR));
                    }
                }
                if is_end {
                    Ok(Some(frame.into()))
                } else {
                    self.partial = Some(Partial {
                        frame: Continuable::Headers(frame),
                        buf: hdr_payload,
                        continuation_frames_count: 0,
                    });
                    Ok(None)
                }
            }
            Err(frame::Error::InvalidDependencyId) => Err(DecodeError::Stream(
                head.stream_id(),
                frame::Reason::PROTOCOL_ERROR,
            )),
            Err(_) => Err(DecodeError::Connection(frame::Reason::PROTOCOL_ERROR)),
        }
    }

    fn decode_push_promise(
        &mut self,
        head: Head,
        src: BytesMut,
    ) -> Result<Option<Frame>, DecodeError> {
        match frame::PushPromise::load(head, src) {
            Ok((mut frame, mut hdr_payload)) => {
                let is_end = frame.is_end_headers();
                match frame.load_hpack(&mut hdr_payload, self.max_header_list_size, &mut self.hpack)
                {
                    Ok(_) => {}
                    Err(frame::Error::Hpack(hpack::DecoderError::NeedMore(_))) if !is_end => {}
                    Err(frame::Error::MalformedMessage) => {
                        return Err(DecodeError::Stream(
                            head.stream_id(),
                            frame::Reason::PROTOCOL_ERROR,
                        ));
                    }
                    Err(_) => {
                        return Err(DecodeError::Connection(frame::Reason::PROTOCOL_ERROR));
                    }
                }
                if is_end {
                    Ok(Some(frame.into()))
                } else {
                    self.partial = Some(Partial {
                        frame: Continuable::PushPromise(frame),
                        buf: hdr_payload,
                        continuation_frames_count: 0,
                    });
                    Ok(None)
                }
            }
            Err(frame::Error::InvalidDependencyId) => Err(DecodeError::Stream(
                head.stream_id(),
                frame::Reason::PROTOCOL_ERROR,
            )),
            Err(_) => Err(DecodeError::Connection(frame::Reason::PROTOCOL_ERROR)),
        }
    }

    fn decode_continuation(
        &mut self,
        head: Head,
        payload: &[u8],
    ) -> Result<Option<Frame>, DecodeError> {
        let is_end_headers = (head.flag() & 0x4) == 0x4;

        let mut partial = match self.partial.take() {
            Some(p) => p,
            None => return Err(DecodeError::Connection(frame::Reason::PROTOCOL_ERROR)),
        };

        if partial.frame.stream_id() != head.stream_id() {
            return Err(DecodeError::Connection(frame::Reason::PROTOCOL_ERROR));
        }

        if is_end_headers {
            partial.continuation_frames_count = 0;
        } else {
            let cnt = partial.continuation_frames_count + 1;
            if cnt > self.max_continuation_frames {
                return Err(DecodeError::Connection(frame::Reason::ENHANCE_YOUR_CALM));
            }
            partial.continuation_frames_count = cnt;
        }

        if partial.buf.is_empty() {
            partial.buf = BytesMut::from(payload);
        } else {
            if partial.frame.is_over_size()
                && partial.buf.len() + payload.len() > self.max_header_list_size
            {
                return Err(DecodeError::Connection(frame::Reason::COMPRESSION_ERROR));
            }
            partial.buf.extend_from_slice(payload);
        }

        match partial
            .frame
            .load_hpack(&mut partial.buf, self.max_header_list_size, &mut self.hpack)
        {
            Ok(_) => {}
            Err(frame::Error::Hpack(hpack::DecoderError::NeedMore(_))) if !is_end_headers => {}
            Err(frame::Error::MalformedMessage) => {
                return Err(DecodeError::Stream(
                    head.stream_id(),
                    frame::Reason::PROTOCOL_ERROR,
                ));
            }
            Err(_) => {
                return Err(DecodeError::Connection(frame::Reason::PROTOCOL_ERROR));
            }
        }

        if is_end_headers {
            Ok(Some(partial.frame.into()))
        } else {
            self.partial = Some(partial);
            Ok(None)
        }
    }
}

/// Full connection reader: FrameDecoder + socket I/O + stream dispatch.
pub struct ConnectionFrameReader {
    decoder: FrameDecoder,
    socket: SocketWrapper,
    streams: HashMap<StreamId, Sender<Frame>>,
    conn_events: Sender<ConnectionEvent>,
}

impl ConnectionFrameReader {
    pub fn new(socket: SocketWrapper, conn_events: Sender<ConnectionEvent>) -> Self {
        ConnectionFrameReader {
            decoder: FrameDecoder::new(),
            socket,
            streams: HashMap::new(),
            conn_events,
        }
    }

    pub fn register_stream(&mut self, id: StreamId, sender: Sender<Frame>) {
        self.streams.insert(id, sender);
    }

    pub fn unregister_stream(&mut self, id: &StreamId) {
        self.streams.remove(id);
    }

    async fn fill(&mut self) -> Result<(), DecodeError> {
        self.decoder.buf.reserve(READ_BUF_SIZE);
        let spare = self.decoder.buf.spare_capacity_mut();
        let n = socket_recv_init(
            &self.socket,
            unsafe { &mut *(spare as *mut [std::mem::MaybeUninit<u8>] as *mut [u8]) },
            spare.len(),
        )
        .await
        .map_err(DecodeError::Io)?;

        if n == 0 {
            return Err(DecodeError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed",
            )));
        }

        unsafe { self.decoder.buf.set_len(self.decoder.buf.len() + n) };
        Ok(())
    }

    /// Read and decode a single frame from the socket.
    pub async fn next_frame(&mut self) -> Result<Frame, DecodeError> {
        loop {
            match self.decoder.decode() {
                Ok(Some(frame)) => return Ok(frame),
                Ok(None) => self.fill().await?,
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn process(&mut self) -> Result<(), DecodeError> {
        loop {
            let frame = match self.decoder.decode() {
                Ok(Some(frame)) => frame,
                Ok(None) => {
                    self.fill().await?;
                    continue;
                }
                Err(e) => return Err(e),
            };

            match frame {
                Frame::Settings(s) => {
                    self.conn_events.send(ConnectionEvent::Settings(s));
                }
                Frame::Ping(p) => {
                    self.conn_events.send(ConnectionEvent::Ping(p));
                }
                Frame::GoAway(ga) => {
                    self.conn_events.send(ConnectionEvent::GoAway(ga));
                    return Ok(()); // peer is shutting down, stop reading
                }
                Frame::WindowUpdate(wu) if wu.stream_id().is_zero() => {
                    self.conn_events
                        .send(ConnectionEvent::WindowUpdate(wu.size_increment()));
                }
                // Stream-level frames: dispatch to the registered stream channel
                frame => {
                    let end_stream = match &frame {
                        Frame::Data(d) if d.is_end_stream() => Some(d.stream_id()),
                        Frame::Headers(h) if h.is_end_stream() => Some(h.stream_id()),
                        _ => None,
                    };
                    if let Some(id) = frame.stream_id() {
                        if let Some(sender) = self.streams.get(&id) {
                            sender.send(frame);
                            if let Some(sid) = end_stream {
                                sender.send(Frame::EndStream(sid));
                            }
                        }
                        // else: frame for unknown stream — could send RST_STREAM
                    }
                }
            }
        }
    }
}

fn calc_max_continuation_frames(header_max: usize, frame_max: usize) -> usize {
    let min_frames = (header_max / frame_max).max(1);
    let padding = min_frames >> 2;
    min_frames.saturating_add(padding).max(5)
}
