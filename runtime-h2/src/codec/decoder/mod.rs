use std::collections::VecDeque;

use zerocopy::{FromBytes, Immutable, KnownLayout, network_endian::U32};

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

#[derive(Debug)]
pub enum Frame {
    Data {
        stream_id: u32,
        end_stream: bool,
        data: Vec<u8>,
    },
    Headers {
        stream_id: u32,
        flags: u8,
        payload: Vec<u8>,
    },
    RstStream {
        stream_id: u32,
        error_code: u32,
    },
    Settings {
        ack: bool,
        payload: Vec<u8>,
    },
    Ping {
        ack: bool,
        opaque: [u8; 8],
    },
    GoAway {
        last_stream_id: u32,
        error_code: u32,
        debug_data: Vec<u8>,
    },
    WindowUpdate {
        stream_id: u32,
        increment: u32,
    },
    Continuation {
        stream_id: u32,
        end_headers: bool,
        payload: Vec<u8>,
    },
}

#[derive(Debug)]
pub enum FrameError {
    FrameSizeError(u32),
    ProtocolError(&'static str),
}

/// Max bytes needed in reassembly for fixed-size frames (header + PING payload)
const MAX_FIXED_FRAME: usize = FRAME_HEADER_SIZE + 8;

pub struct FrameDecoder {
    max_frame_size: u32,
    pub frames: VecDeque<Frame>,
    // Small reassembly buffer — only ever holds up to header + largest fixed payload
    reassembly: [u8; MAX_FIXED_FRAME],
    reassembly_len: usize,
}

impl FrameDecoder {
    pub fn new() -> Self {
        Self {
            max_frame_size: DEFAULT_MAX_FRAME_SIZE,
            frames: VecDeque::new(),
            reassembly: [0u8; MAX_FIXED_FRAME],
            reassembly_len: 0,
        }
    }

    /// Feed a buffer into the decoder. Decodes all complete frames,
    /// stores partial fixed-size frame bytes in reassembly.
    /// Returns the number of bytes consumed from `data`.
    pub fn decode(&mut self, data: &[u8]) -> Result<usize, FrameError> {
        let mut pos = 0;

        // Step 1: finish reassembling a partial frame from last call
        if self.reassembly_len > 0 {
            pos = self.drain_reassembly(data)?;
            if self.reassembly_len > 0 {
                // Still not enough — wait for more data
                return Ok(pos);
            }
        }

        // Step 2: decode complete frames directly from data
        pos += self.decode_inline(&data[pos..])?;

        Ok(pos)
    }

    /// Try to complete the partial frame sitting in reassembly.
    /// Returns how many bytes from `data` were consumed.
    fn drain_reassembly(&mut self, data: &[u8]) -> Result<usize, FrameError> {
        let mut pos = 0;

        // First: ensure we have the 9-byte header
        if self.reassembly_len < FRAME_HEADER_SIZE {
            let need = FRAME_HEADER_SIZE - self.reassembly_len;
            let copy = data.len().min(need);
            self.reassembly[self.reassembly_len..self.reassembly_len + copy]
                .copy_from_slice(&data[..copy]);
            self.reassembly_len += copy;
            pos += copy;

            if self.reassembly_len < FRAME_HEADER_SIZE {
                return Ok(pos);
            }
        }

        // We have the header — parse it
        let (raw, _) = RawFrameHeader::ref_from_prefix(&self.reassembly).unwrap();
        let length = raw.length();
        let kind = FrameKind::from(raw.kind);
        let flags = raw.flags;
        let stream_id = raw.stream_id();

        if length > self.max_frame_size {
            return Err(FrameError::FrameSizeError(length));
        }

        let remaining_data = &data[pos..];

        match kind {
            // Fixed-size frames: copy payload bytes into reassembly
            FrameKind::RstStream | FrameKind::Ping | FrameKind::WindowUpdate => {
                let total = FRAME_HEADER_SIZE + length as usize;
                let need = total - self.reassembly_len;
                let copy = remaining_data.len().min(need);
                self.reassembly[self.reassembly_len..self.reassembly_len + copy]
                    .copy_from_slice(&remaining_data[..copy]);
                self.reassembly_len += copy;
                pos += copy;

                if self.reassembly_len < total {
                    return Ok(pos);
                }

                let payload = &self.reassembly[FRAME_HEADER_SIZE..total];
                let frame =
                    self.decode_constant_len_frame(kind, flags, stream_id, length, payload)?;
                self.frames.push_back(frame);
                self.reassembly_len = 0;
            }
            // Variable-size frames: allocate Vec, copy from reassembly + data
            _ => {
                let payload_len = length as usize;
                let from_reassembly = self.reassembly_len - FRAME_HEADER_SIZE;
                let from_data = payload_len - from_reassembly;

                if remaining_data.len() < from_data {
                    // Not enough data yet — can't complete this frame.
                    // This shouldn't normally happen since variable-size frames
                    // only store the header in reassembly, but handle it safely.
                    return Ok(pos);
                }

                let mut payload = Vec::with_capacity(payload_len);
                // Copy any payload bytes that were in reassembly (after the header)
                if from_reassembly > 0 {
                    payload.extend_from_slice(
                        &self.reassembly[FRAME_HEADER_SIZE..self.reassembly_len],
                    );
                }
                // Copy the rest from the incoming data
                payload.extend_from_slice(&remaining_data[..from_data]);
                pos += from_data;
                self.reassembly_len = 0;

                let frame =
                    self.decode_non_constant_len_frame(kind, flags, stream_id, length, payload)?;
                if let Some(frame) = frame {
                    self.frames.push_back(frame);
                }
            }
        }

        Ok(pos)
    }

    /// Decode frames directly from contiguous data (no reassembly needed).
    /// Returns the number of bytes consumed.
    fn decode_inline(&mut self, data: &[u8]) -> Result<usize, FrameError> {
        let mut offset = 0;

        loop {
            let remaining = &data[offset..];

            if remaining.len() < FRAME_HEADER_SIZE {
                // Partial header — save to reassembly
                if !remaining.is_empty() {
                    self.reassembly[..remaining.len()].copy_from_slice(remaining);
                    self.reassembly_len = remaining.len();
                    offset += remaining.len();
                }
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
                // Partial frame — save what we have to reassembly for fixed,
                // or just the header for variable
                match kind {
                    FrameKind::RstStream | FrameKind::Ping | FrameKind::WindowUpdate => {
                        let save = remaining.len().min(MAX_FIXED_FRAME);
                        self.reassembly[..save].copy_from_slice(&remaining[..save]);
                        self.reassembly_len = save;
                    }
                    _ => {
                        // Save just the header
                        self.reassembly[..FRAME_HEADER_SIZE]
                            .copy_from_slice(&remaining[..FRAME_HEADER_SIZE]);
                        self.reassembly_len = FRAME_HEADER_SIZE;
                    }
                }
                offset += remaining.len();
                break;
            }

            let payload = &remaining[FRAME_HEADER_SIZE..total];

            match kind {
                FrameKind::RstStream | FrameKind::Ping | FrameKind::WindowUpdate => {
                    let frame =
                        self.decode_constant_len_frame(kind, flags, stream_id, length, payload)?;
                    self.frames.push_back(frame);
                }
                _ => {
                    let frame = self.decode_non_constant_len_frame(
                        kind,
                        flags,
                        stream_id,
                        length,
                        payload.to_vec(),
                    )?;
                    if let Some(frame) = frame {
                        self.frames.push_back(frame);
                    }
                }
            }

            offset += total;
        }

        Ok(offset)
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
                Ok(Some(Frame::Settings {
                    ack: flags & 0x1 != 0,
                    payload,
                }))
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
