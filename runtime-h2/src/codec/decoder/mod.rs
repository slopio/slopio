use std::collections::VecDeque;

use zerocopy::{FromBytes, Immutable, KnownLayout, network_endian::U32};

use crate::types::{Frame, FrameError, Setting};

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
            0x2 => Setting::EnablePush(value != 0),
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

pub struct FrameDecoder {
    max_frame_size: u32,
    pub frames: VecDeque<Frame>,
    remaining: Vec<u8>,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            frames: VecDeque::new(),
            remaining: Vec::new(),
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

        // Step 3: for constant-size frames, copy just enough to complete the frame
        match kind {
            FrameKind::RstStream | FrameKind::Ping | FrameKind::WindowUpdate => {
                let need = total - self.remaining.len();
                let available = data.len() - pos;
                if available < need {
                    self.remaining.extend_from_slice(&data[pos..]);
                    return Ok(data.len());
                }
                self.remaining.extend_from_slice(&data[pos..pos + need]);
                pos += need;

                let payload = &self.remaining[FRAME_HEADER_SIZE..total];
                let frame =
                    self.decode_constant_len_frame(kind, flags, stream_id, length, payload)?;
                self.frames.push_back(frame);
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

                let mut payload = Vec::with_capacity(length as usize);
                payload.extend_from_slice(&self.remaining[FRAME_HEADER_SIZE..]);
                payload.extend_from_slice(&data[pos..pos + need_from_data]);
                pos += need_from_data;

                if let Some(frame) =
                    self.decode_non_constant_len_frame(kind, flags, stream_id, length, payload)?
                {
                    self.frames.push_back(frame);
                }
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
        match kind {
            FrameKind::RstStream | FrameKind::Ping | FrameKind::WindowUpdate => {
                let frame =
                    self.decode_constant_len_frame(kind, flags, stream_id, length, payload)?;
                self.frames.push_back(frame);
            }
            _ => {
                if let Some(frame) = self.decode_non_constant_len_frame(
                    kind,
                    flags,
                    stream_id,
                    length,
                    payload.to_vec(),
                )? {
                    self.frames.push_back(frame);
                }
            }
        }
        Ok(())
    }

    /// Decode a fixed-size frame from its complete payload.
    fn decode_constant_len_frame(
        &self,
        kind: FrameKind,
        flags: u8,
        stream_id: u32,
        length: u32,
        payload: &[u8],
    ) -> Result<Frame, FrameError> {
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
                Ok(Frame::RstStream {
                    stream_id,
                    error_code: u32::from_be_bytes(payload[..4].try_into().unwrap()),
                })
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
                Ok(Frame::Ping {
                    ack: flags & 0x1 != 0,
                    opaque: payload[..8].try_into().unwrap(),
                })
            }
            FrameKind::WindowUpdate => {
                if length != 4 {
                    return Err(FrameError::FrameSizeError(length));
                }
                let increment = u32::from_be_bytes(payload[..4].try_into().unwrap()) & 0x7FFF_FFFF;
                Ok(Frame::WindowUpdate {
                    stream_id,
                    increment,
                })
            }
            _ => unreachable!(),
        }
    }

    /// Decode a variable-size frame from its complete payload (already in a Vec).
    /// Returns None for ignored frame types (Priority, PushPromise, Unknown).
    fn decode_non_constant_len_frame(
        &self,
        kind: FrameKind,
        flags: u8,
        stream_id: u32,
        length: u32,
        payload: Vec<u8>,
    ) -> Result<Option<Frame>, FrameError> {
        match kind {
            FrameKind::Data => {
                if stream_id == 0 {
                    return Err(FrameError::ProtocolError("DATA frame with stream ID 0"));
                }
                Ok(Some(Frame::Data {
                    stream_id,
                    end_stream: flags & 0x1 != 0,
                    data: payload,
                }))
            }
            FrameKind::Headers => {
                if stream_id == 0 {
                    return Err(FrameError::ProtocolError("HEADERS frame with stream ID 0"));
                }
                Ok(Some(Frame::Headers {
                    stream_id,
                    flags,
                    payload,
                }))
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
                Ok(Some(Frame::Settings { ack, settings }))
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
                let last_stream_id =
                    u32::from_be_bytes(payload[..4].try_into().unwrap()) & 0x7FFF_FFFF;
                let error_code = u32::from_be_bytes(payload[4..8].try_into().unwrap());
                Ok(Some(Frame::GoAway {
                    last_stream_id,
                    error_code,
                    debug_data: payload[8..].to_vec(),
                }))
            }
            FrameKind::Continuation => {
                if stream_id == 0 {
                    return Err(FrameError::ProtocolError(
                        "CONTINUATION frame with stream ID 0",
                    ));
                }
                Ok(Some(Frame::Continuation {
                    stream_id,
                    end_headers: flags & 0x4 != 0,
                    payload,
                }))
            }
            // Priority, PushPromise, Unknown — skip
            _ => Ok(None),
        }
    }
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new()
    }
}
