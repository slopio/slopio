use bytes::BytesMut;
use http::header::HeaderName;

use crate::codec::frames::headers::Headers;
use crate::codec::frames::stream_id::StreamId;
use crate::hpack::{self, Header};
use crate::types::Frame;

// Frame types
const DATA: u8 = 0x0;
const HEADERS: u8 = 0x1;
const RST_STREAM: u8 = 0x3;
const SETTINGS: u8 = 0x4;
const PING: u8 = 0x6;
const GOAWAY: u8 = 0x7;
const WINDOW_UPDATE: u8 = 0x8;

// Flags
const END_STREAM: u8 = 0x1;
const END_HEADERS: u8 = 0x4;
const ACK: u8 = 0x1;

pub struct FrameEncoder {
    hpack: hpack::Encoder,
    buf: BytesMut,
    hpack_buf: BytesMut,
}

impl FrameEncoder {
    pub fn new() -> Self {
        Self {
            hpack: hpack::Encoder::default(),
            buf: BytesMut::with_capacity(4096),
            hpack_buf: BytesMut::with_capacity(1024),
        }
    }

    pub fn encode(&mut self, frame: &Frame) {
        match frame {
            Frame::Headers(headers) => self.encode_headers(headers),
            Frame::Data {
                stream_id,
                end_stream,
                data,
            } => self.encode_data(*stream_id, *end_stream, data),
            Frame::RstStream {
                stream_id,
                error_code,
            } => self.encode_rst_stream(*stream_id, *error_code),
            Frame::Settings { ack, settings } => self.encode_settings(*ack, settings),
            Frame::Ping { ack, opaque } => self.encode_ping(*ack, opaque),
            Frame::GoAway {
                last_stream_id,
                error_code,
                debug_data,
            } => self.encode_goaway(*last_stream_id, *error_code, debug_data),
            Frame::WindowUpdate {
                stream_id,
                increment,
            } => self.encode_window_update(*stream_id, *increment),
        }
    }

    pub fn take(&mut self) -> BytesMut {
        self.buf.split()
    }

    fn write_frame_header(&mut self, length: u32, frame_type: u8, flags: u8, stream_id: StreamId) {
        let len_bytes = length.to_be_bytes();
        self.buf.extend_from_slice(&len_bytes[1..4]); // 3 bytes
        self.buf.extend_from_slice(&[frame_type, flags]);
        self.buf
            .extend_from_slice(&u32::from(stream_id).to_be_bytes());
    }

    fn encode_headers(&mut self, headers: &Headers) {
        self.hpack_buf.clear();

        // Encode pseudo headers
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

        // Encode regular headers
        for (name, value) in headers.fields() {
            hpack_headers.push(Header::Field {
                name: Some(name.clone()),
                value: value.clone(),
            });
        }

        self.hpack.encode(hpack_headers, &mut self.hpack_buf);

        let mut flags = END_HEADERS;
        if headers.is_end_stream() {
            flags |= END_STREAM;
        }

        self.write_frame_header(
            self.hpack_buf.len() as u32,
            HEADERS,
            flags,
            headers.stream_id(),
        );
        self.buf.extend_from_slice(&self.hpack_buf);
    }

    fn encode_data(&mut self, stream_id: StreamId, end_stream: bool, data: &[u8]) {
        let flags = if end_stream { END_STREAM } else { 0 };
        self.write_frame_header(data.len() as u32, DATA, flags, stream_id);
        self.buf.extend_from_slice(data);
    }

    fn encode_rst_stream(&mut self, stream_id: StreamId, error_code: u32) {
        self.write_frame_header(4, RST_STREAM, 0, stream_id);
        self.buf.extend_from_slice(&error_code.to_be_bytes());
    }

    fn encode_settings(&mut self, ack: bool, settings: &[crate::types::Setting]) {
        let flags = if ack { ACK } else { 0 };
        let payload_len = settings.len() as u32 * 6;
        self.write_frame_header(payload_len, SETTINGS, flags, StreamId::ZERO);
        for setting in settings {
            let (id, value) = match *setting {
                crate::types::Setting::HeaderTableSize(v) => (0x1, v),
                crate::types::Setting::EnablePush(v) => (0x2, v),
                crate::types::Setting::MaxConcurrentStreams(v) => (0x3, v),
                crate::types::Setting::InitialWindowSize(v) => (0x4, v),
                crate::types::Setting::MaxFrameSize(v) => (0x5, v),
                crate::types::Setting::MaxHeaderListSize(v) => (0x6, v),
                crate::types::Setting::Unknown { id, value } => (id, value),
            };
            self.buf.extend_from_slice(&id.to_be_bytes());
            self.buf.extend_from_slice(&value.to_be_bytes());
        }
    }

    fn encode_ping(&mut self, ack: bool, opaque: &[u8; 8]) {
        let flags = if ack { ACK } else { 0 };
        self.write_frame_header(8, PING, flags, StreamId::ZERO);
        self.buf.extend_from_slice(opaque);
    }

    fn encode_goaway(&mut self, last_stream_id: StreamId, error_code: u32, debug_data: &[u8]) {
        let len = 8 + debug_data.len() as u32;
        self.write_frame_header(len, GOAWAY, 0, StreamId::ZERO);
        self.buf
            .extend_from_slice(&u32::from(last_stream_id).to_be_bytes());
        self.buf.extend_from_slice(&error_code.to_be_bytes());
        self.buf.extend_from_slice(debug_data);
    }

    fn encode_window_update(&mut self, stream_id: StreamId, increment: u32) {
        self.write_frame_header(4, WINDOW_UPDATE, 0, stream_id);
        self.buf.extend_from_slice(&increment.to_be_bytes());
    }
}

impl Default for FrameEncoder {
    fn default() -> Self {
        Self::new()
    }
}
