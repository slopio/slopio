use std::collections::VecDeque;

use bytes::BytesMut;
use zerocopy::{FromBytes, Immutable, KnownLayout, network_endian::U32};

use crate::{
    codec::frames::{
        head::{Head, Kind},
        headers::Headers,
        stream_id::StreamId,
    },
    hpack,
    types::{Frame, FrameError, Setting},
};

mod head;
pub mod headers;
pub mod stream_id;

const FRAME_HEADER_SIZE: usize = 9;
const DEFAULT_MAX_FRAME_SIZE: u32 = 16_384;

#[derive(FromBytes, KnownLayout, Immutable)]
#[repr(C)]
struct RawFrameHeader {
    length: [u8; 3],
    kind: u8,
    flags: u8,
    stream_id: U32,
}

impl RawFrameHeader {
    fn length(&self) -> u32 {
        (self.length[0] as u32) << 16 | (self.length[1] as u32) << 8 | (self.length[2] as u32)
    }

    fn stream_id(&self) -> u32 {
        self.stream_id.get() & 0x7FFF_FFFF
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    Data,
    Headers,
    Priority,
    RstStream,
    Settings,
    PushPromise,
    Ping,
    GoAway,
    WindowUpdate,
    Continuation,
    Unknown(u8),
}

impl From<u8> for FrameKind {
    fn from(value: u8) -> Self {
        match value {
            0x0 => FrameKind::Data,
            0x1 => FrameKind::Headers,
            0x2 => FrameKind::Priority,
            0x3 => FrameKind::RstStream,
            0x4 => FrameKind::Settings,
            0x5 => FrameKind::PushPromise,
            0x6 => FrameKind::Ping,
            0x7 => FrameKind::GoAway,
            0x8 => FrameKind::WindowUpdate,
            0x9 => FrameKind::Continuation,
            other => FrameKind::Unknown(other),
        }
    }
}

fn parse_settings(payload: &[u8]) -> Result<Vec<Setting>, FrameError> {
    if !payload.len().is_multiple_of(6) {
        return Err(FrameError::FrameSizeError(payload.len() as u32));
    }
    let mut settings = Vec::with_capacity(payload.len() / 6);
    let mut pos = 0;
    while pos < payload.len() {
        let id = u16::from_be_bytes(payload[pos..pos + 2].try_into().unwrap());
        let value = u32::from_be_bytes(payload[pos + 2..pos + 6].try_into().unwrap());
        let setting = match id {
            0x1 => Setting::HeaderTableSize(value),
            0x2 => Setting::EnablePush(value),
            0x3 => Setting::MaxConcurrentStreams(value),
            0x4 => Setting::InitialWindowSize(value),
            0x5 => Setting::MaxFrameSize(value),
            0x6 => Setting::MaxHeaderListSize(value),
            _ => Setting::Unknown { id, value },
        };
        settings.push(setting);
        pos += 6;
    }
    Ok(settings)
}

/// State for accumulating a header block across HEADERS + CONTINUATION frames.
struct HeaderBlockState {
    headers: Headers,
    buf: BytesMut,
}

pub struct FrameDecoder {
    hpack: hpack::Decoder,
    max_frame_size: u32,
    max_header_list_size: usize,
    pub frames: VecDeque<Frame>,
    remaining: Vec<u8>,
    /// In-progress header block waiting for END_HEADERS
    header_block: Option<HeaderBlockState>,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self {
            hpack: hpack::Decoder::default(),
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            max_header_list_size: usize::MAX,
            frames: VecDeque::new(),
            remaining: Vec::new(),
            header_block: None,
        }
    }

    pub fn decode(&mut self, data: &[u8]) -> Result<(), FrameError> {
        let mut pos = 0;

        if !self.remaining.is_empty() {
            pos = self.handle_reassembly(data)?;
        }

        // Decode complete frames directly from the rest of data
        let consumed = self.decode_buf(&data[pos..])?;
        let leftover = &data[pos + consumed..];
        if !leftover.is_empty() {
            self.remaining.extend_from_slice(leftover);
        }

        Ok(())
    }

    /// Complete a partial frame using bytes from `data`.
    /// Returns how many bytes from `data` were consumed.
    fn handle_reassembly(&mut self, data: &[u8]) -> Result<usize, FrameError> {
        let mut pos = 0;

        // Step 1: ensure we have a full 9-byte header in remaining
        if self.remaining.len() < FRAME_HEADER_SIZE {
            let need = FRAME_HEADER_SIZE - self.remaining.len();
            let copy = data.len().min(need);
            self.remaining.extend_from_slice(&data[..copy]);
            pos += copy;

            if self.remaining.len() < FRAME_HEADER_SIZE {
                // Still don't have a full header
                return Ok(pos);
            }
        }

        // Step 2: parse the header
        let (raw, _) = RawFrameHeader::ref_from_prefix(&self.remaining).unwrap();
        let length = raw.length();
        let kind = FrameKind::from(raw.kind);
        let flags = raw.flags;
        let stream_id = raw.stream_id();

        if length > self.max_frame_size {
            return Err(FrameError::FrameSizeError(length));
        }

        let total = FRAME_HEADER_SIZE + length as usize;

        // If we're accumulating a header block, only CONTINUATION is valid
        if self.header_block.is_some() && kind != FrameKind::Continuation {
            return Err(FrameError::ProtocolError(
                "expected CONTINUATION frame while header block is incomplete",
            ));
        }

        // Step 3: for constant-size frames, copy just enough to complete the frame
        match kind {
            FrameKind::RstStream
            | FrameKind::Ping
            | FrameKind::WindowUpdate
            | FrameKind::Priority => {
                let need = total - self.remaining.len();
                let available = data.len() - pos;
                if available < need {
                    self.remaining.extend_from_slice(&data[pos..]);
                    return Ok(data.len());
                }
                self.remaining.extend_from_slice(&data[pos..pos + need]);
                pos += need;

                let payload = &self.remaining[FRAME_HEADER_SIZE..total];
                if let Some(frame) =
                    self.decode_constant_len_frame(kind, flags, stream_id, length, payload)?
                {
                    self.frames.push_back(frame);
                }
                self.remaining.clear();
            }
            _ => {
                let payload_in_remaining = self.remaining.len() - FRAME_HEADER_SIZE;
                let need_from_data = length as usize - payload_in_remaining;
                let available = data.len() - pos;

                if available < need_from_data {
                    // Not enough data yet, copy what we can into remaining
                    self.remaining.extend_from_slice(&data[pos..]);
                    return Ok(data.len());
                }

                let mut payload = BytesMut::with_capacity(length as usize);
                payload.extend_from_slice(&self.remaining[FRAME_HEADER_SIZE..]);
                payload.extend_from_slice(&data[pos..pos + need_from_data]);
                pos += need_from_data;

                self.handle_variable_frame(kind, flags, stream_id, length, payload)?;
                self.remaining.clear();
            }
        }

        Ok(pos)
    }

    /// Decode as many complete frames as possible from `buf`.
    /// Returns the number of bytes consumed.
    fn decode_buf(&mut self, buf: &[u8]) -> Result<usize, FrameError> {
        let mut offset = 0;

        loop {
            let remaining = &buf[offset..];

            if remaining.len() < FRAME_HEADER_SIZE {
                break;
            }

            let (raw, _) = RawFrameHeader::ref_from_prefix(remaining).unwrap();
            let length = raw.length();
            let kind = FrameKind::from(raw.kind);
            let flags = raw.flags;
            let stream_id = raw.stream_id();

            if length > self.max_frame_size {
                return Err(FrameError::FrameSizeError(length));
            }

            let total = FRAME_HEADER_SIZE + length as usize;
            if remaining.len() < total {
                break;
            }

            let payload = &remaining[FRAME_HEADER_SIZE..total];
            self.decode_frame(kind, flags, stream_id, length, payload)?;
            offset += total;
        }

        Ok(offset)
    }

    /// Decode a single complete frame and push it to the queue.
    fn decode_frame(
        &mut self,
        kind: FrameKind,
        flags: u8,
        stream_id: u32,
        length: u32,
        payload: &[u8],
    ) -> Result<(), FrameError> {
        // If we're accumulating a header block, only CONTINUATION is valid
        if self.header_block.is_some() && kind != FrameKind::Continuation {
            return Err(FrameError::ProtocolError(
                "expected CONTINUATION frame while header block is incomplete",
            ));
        }

        match kind {
            FrameKind::RstStream
            | FrameKind::Ping
            | FrameKind::WindowUpdate
            | FrameKind::Priority => {
                let frame =
                    self.decode_constant_len_frame(kind, flags, stream_id, length, payload)?;
                if let Some(f) = frame {
                    self.frames.push_back(f);
                }
            }
            _ => {
                self.handle_variable_frame(
                    kind,
                    flags,
                    stream_id,
                    length,
                    BytesMut::from(payload),
                )?;
            }
        }
        Ok(())
    }

    /// Handle a variable-length frame: either push directly or accumulate headers.
    fn handle_variable_frame(
        &mut self,
        kind: FrameKind,
        flags: u8,
        stream_id: u32,
        length: u32,
        payload: BytesMut,
    ) -> Result<(), FrameError> {
        // CONTINUATION stream ID must match the HEADERS stream ID
        if let Some(ref state) = self.header_block
            && state.headers.stream_id() != StreamId::from(stream_id)
        {
            return Err(FrameError::ProtocolError(
                "CONTINUATION frame on different stream than HEADERS",
            ));
        }

        match kind {
            FrameKind::Headers => {
                let head = Head::new(Kind::Headers, flags, StreamId::from(stream_id));
                let (headers, hpack_buf) =
                    Headers::load(head, payload).map_err(FrameError::Hpack)?;
                let end_headers = headers.is_end_headers();

                if end_headers {
                    let headers = self.decode_hpack(headers, hpack_buf)?;
                    self.frames.push_back(Frame::Headers(headers));
                } else {
                    self.header_block = Some(HeaderBlockState {
                        headers,
                        buf: hpack_buf,
                    });
                }
            }
            FrameKind::Continuation => {
                if stream_id == 0 {
                    return Err(FrameError::ProtocolError(
                        "CONTINUATION frame with stream ID 0",
                    ));
                }

                let state = self.header_block.as_mut().ok_or(FrameError::ProtocolError(
                    "CONTINUATION without preceding HEADERS",
                ))?;

                state.buf.extend_from_slice(&payload);
                let end_headers = flags & 0x4 != 0;

                if end_headers {
                    let state = self.header_block.take().unwrap();
                    let headers = self.decode_hpack(state.headers, state.buf)?;
                    self.frames.push_back(Frame::Headers(headers));
                }
            }
            FrameKind::Data => {
                if stream_id == 0 {
                    return Err(FrameError::ProtocolError("DATA frame with stream ID 0"));
                }
                let data = if flags & 0x8 != 0 {
                    // PADDED flag
                    if payload.is_empty() {
                        return Err(FrameError::ProtocolError(
                            "DATA with PADDED flag but no payload",
                        ));
                    }
                    let pad_len = payload[0] as usize;
                    if pad_len >= payload.len() {
                        return Err(FrameError::ProtocolError("DATA pad length exceeds payload"));
                    }
                    payload[1..payload.len() - pad_len].to_vec()
                } else {
                    payload.to_vec()
                };
                self.frames.push_back(Frame::Data {
                    stream_id: StreamId::from(stream_id),
                    end_stream: flags & 0x1 != 0,
                    data,
                });
            }
            FrameKind::Settings => {
                if stream_id != 0 {
                    return Err(FrameError::ProtocolError(
                        "SETTINGS frame with non-zero stream ID",
                    ));
                }
                let ack = flags & 0x1 != 0;
                if ack && !payload.is_empty() {
                    return Err(FrameError::FrameSizeError(payload.len() as u32));
                }
                let settings = parse_settings(&payload)?;
                self.frames.push_back(Frame::Settings { ack, settings });
            }
            FrameKind::GoAway => {
                if stream_id != 0 {
                    return Err(FrameError::ProtocolError(
                        "GOAWAY frame with non-zero stream ID",
                    ));
                }
                if length < 8 {
                    return Err(FrameError::FrameSizeError(length));
                }
                let last_stream_id = StreamId::from(
                    u32::from_be_bytes(payload[..4].try_into().unwrap()) & 0x7FFF_FFFF,
                );
                let error_code = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                self.frames.push_back(Frame::GoAway {
                    last_stream_id,
                    error_code,
                    debug_data: payload[8..].into(),
                });
            }
            FrameKind::PushPromise => {
                return Err(FrameError::ProtocolError("PUSH_PROMISE from client"));
            }
            // Unknown — skip
            _ => {}
        }
        Ok(())
    }

    /// Run the HPACK decoder on a complete header block and return a fully decoded Headers.
    fn decode_hpack(
        &mut self,
        mut headers: Headers,
        mut buf: BytesMut,
    ) -> Result<Headers, FrameError> {
        headers
            .load_hpack(&mut buf, self.max_header_list_size, &mut self.hpack)
            .map_err(|e| match e {
                crate::hpack::Error::MalformedMessage => {
                    FrameError::ProtocolError("malformed message")
                }
                other => FrameError::Hpack(other),
            })?;
        Ok(headers)
    }

    /// Decode a fixed-size frame from its complete payload.
    /// Returns None for frames that are validated but not emitted (e.g. PRIORITY).
    fn decode_constant_len_frame(
        &self,
        kind: FrameKind,
        flags: u8,
        stream_id: u32,
        length: u32,
        payload: &[u8],
    ) -> Result<Option<Frame>, FrameError> {
        match kind {
            FrameKind::RstStream => {
                if stream_id == 0 {
                    return Err(FrameError::ProtocolError(
                        "RST_STREAM frame with stream ID 0",
                    ));
                }
                if length != 4 {
                    return Err(FrameError::FrameSizeError(length));
                }
                Ok(Some(Frame::RstStream {
                    stream_id: StreamId::from(stream_id),
                    error_code: u32::from_be_bytes(payload[..4].try_into().unwrap()),
                }))
            }
            FrameKind::Ping => {
                if stream_id != 0 {
                    return Err(FrameError::ProtocolError(
                        "PING frame with non-zero stream ID",
                    ));
                }
                if length != 8 {
                    return Err(FrameError::FrameSizeError(length));
                }
                Ok(Some(Frame::Ping {
                    ack: flags & 0x1 != 0,
                    opaque: payload[..8].try_into().unwrap(),
                }))
            }
            FrameKind::WindowUpdate => {
                if length != 4 {
                    return Err(FrameError::FrameSizeError(length));
                }
                let increment = u32::from_be_bytes(payload[..4].try_into().unwrap()) & 0x7FFF_FFFF;
                Ok(Some(Frame::WindowUpdate {
                    stream_id: StreamId::from(stream_id),
                    increment,
                }))
            }
            FrameKind::Priority => {
                if stream_id == 0 {
                    return Err(FrameError::ProtocolError("PRIORITY frame with stream ID 0"));
                }
                if length != 5 {
                    return Err(FrameError::FrameSizeError(length));
                }
                let (dep_id, _exclusive) = StreamId::parse(&payload[..4]);
                if dep_id == StreamId::from(stream_id) {
                    return Err(FrameError::ProtocolError("stream depends on itself"));
                }
                // Validated but not emitted — priority is ignored
                Ok(None)
            }
            _ => unreachable!(),
        }
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}
