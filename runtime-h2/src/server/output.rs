use std::cell::UnsafeCell;
use std::rc::Rc;

use bytes::BytesMut;
use http::header::HeaderName;
use runtime_core::channels::Sender;

use crate::codec::frames::headers::Headers;
use crate::codec::frames::stream_id::StreamId;
use crate::hpack::{self, Header};
use crate::types::FrameError;

const FRAME_HEADER_LEN: usize = 9;
const END_STREAM: u8 = 0x1;
const END_HEADERS: u8 = 0x4;

pub enum OutputFrame {
    EncodedHeaders(Vec<u8>),
    EncodedData(Vec<u8>),
    WindowUpdate {
        stream_id: StreamId,
        increment: u32,
        frame: Vec<u8>,
    },
    StreamDone(StreamId),
}

struct SharedOutputInner {
    hpack: hpack::Encoder,
    hpack_buf: BytesMut,
    sender: Sender<OutputFrame>,
    send_window: i64,
}

pub struct SharedOutput(Rc<UnsafeCell<SharedOutputInner>>);

impl Clone for SharedOutput {
    fn clone(&self) -> Self {
        SharedOutput(Rc::clone(&self.0))
    }
}

impl SharedOutput {
    pub fn new(sender: Sender<OutputFrame>, initial_window_size: u32) -> Self {
        SharedOutput(Rc::new(UnsafeCell::new(SharedOutputInner {
            hpack: hpack::Encoder::default(),
            hpack_buf: BytesMut::with_capacity(1024),
            sender,
            send_window: initial_window_size as i64,
        })))
    }

    pub fn send_window(&self) -> i64 {
        let inner = unsafe { &*self.0.get() };
        inner.send_window
    }

    pub fn add_send_window(&self, increment: u32) -> Result<(), FrameError> {
        let inner = unsafe { &mut *self.0.get() };
        match inner.send_window.checked_add(increment as i64) {
            Some(v) if v <= 0x7FFF_FFFF => {
                inner.send_window = v;
                Ok(())
            }
            _ => Err(FrameError::FlowControlError(
                "connection send window overflow",
            )),
        }
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

        inner.sender.send(OutputFrame::EncodedHeaders(buf));
    }

    pub fn send_data(&self, stream_id: StreamId, end_stream: bool, data: Vec<u8>) {
        let inner = unsafe { &mut *self.0.get() };
        inner.send_window -= data.len() as i64;
        let flags = if end_stream { END_STREAM } else { 0 };
        let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + data.len());
        Self::write_frame_header(&mut buf, data.len() as u32, 0x0, flags, stream_id);
        buf.extend_from_slice(&data);
        inner.sender.send(OutputFrame::EncodedData(buf));
    }

    pub fn send_window_update(&self, stream_id: StreamId, increment: u32) {
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

    pub fn send_rst_stream(&self, stream_id: StreamId, error_code: u32) {
        let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + 4);
        Self::write_frame_header(&mut buf, 4, 0x03, 0, stream_id);
        buf.extend_from_slice(&error_code.to_be_bytes());
        let inner = unsafe { &mut *self.0.get() };
        inner.sender.send(OutputFrame::EncodedHeaders(buf));
    }

    pub fn send_goaway(&self, last_stream_id: StreamId, error_code: u32) {
        let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + 8);
        Self::write_frame_header(&mut buf, 8, 0x07, 0, StreamId::ZERO);
        buf.extend_from_slice(&u32::from(last_stream_id).to_be_bytes());
        buf.extend_from_slice(&error_code.to_be_bytes());
        let inner = unsafe { &mut *self.0.get() };
        inner.sender.send(OutputFrame::EncodedHeaders(buf));
    }

    pub fn send_settings_ack(&self) {
        let mut buf = Vec::with_capacity(FRAME_HEADER_LEN);
        Self::write_frame_header(&mut buf, 0, 0x04, 0x01, StreamId::ZERO); // type=SETTINGS, flags=ACK
        let inner = unsafe { &mut *self.0.get() };
        inner.sender.send(OutputFrame::EncodedHeaders(buf));
    }

    pub fn send_stream_done(&self, stream_id: StreamId) {
        let inner = unsafe { &mut *self.0.get() };
        inner.sender.send(OutputFrame::StreamDone(stream_id));
    }

    pub fn send_ping_ack(&self, opaque: &[u8; 8]) {
        let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + 8);
        Self::write_frame_header(&mut buf, 8, 0x06, 0x01, StreamId::ZERO); // type=PING, flags=ACK
        buf.extend_from_slice(opaque);
        let inner = unsafe { &mut *self.0.get() };
        inner.sender.send(OutputFrame::EncodedHeaders(buf));
    }

    pub fn send_ping(&self, opaque: &[u8; 8]) {
        let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + 8);
        Self::write_frame_header(&mut buf, 8, 0x06, 0x00, StreamId::ZERO); // type=PING, flags=0
        buf.extend_from_slice(opaque);
        let inner = unsafe { &mut *self.0.get() };
        inner.sender.send(OutputFrame::EncodedHeaders(buf));
    }
}
