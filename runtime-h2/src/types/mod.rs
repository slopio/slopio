use crate::codec::frames::headers::Headers;
use crate::codec::frames::stream_id::StreamId;
use crate::hpack;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Setting {
    HeaderTableSize(u32),
    EnablePush(bool),
    MaxConcurrentStreams(u32),
    InitialWindowSize(u32),
    MaxFrameSize(u32),
    MaxHeaderListSize(u32),
    Unknown { id: u16, value: u32 },
}

pub const HEADER_LEN: usize = 9;

#[derive(Debug)]
pub enum Frame {
    Data {
        stream_id: StreamId,
        end_stream: bool,
        data: Vec<u8>,
    },
    Headers(Headers),
    RstStream {
        stream_id: StreamId,
        error_code: u32,
    },
    Settings {
        ack: bool,
        settings: Vec<Setting>,
    },
    Ping {
        ack: bool,
        opaque: [u8; 8],
    },
    GoAway {
        last_stream_id: StreamId,
        error_code: u32,
        debug_data: Vec<u8>,
    },
    WindowUpdate {
        stream_id: StreamId,
        increment: u32,
    },
}

#[derive(Debug)]
pub enum FrameError {
    FrameSizeError(u32),
    ProtocolError(&'static str),
    Hpack(hpack::Error),
    Io(std::io::Error),
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::FrameSizeError(size) => write!(f, "frame size error: {size}"),
            FrameError::ProtocolError(msg) => write!(f, "protocol error: {msg}"),
            FrameError::Hpack(e) => write!(f, "hpack error: {e:?}"),
            FrameError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl std::error::Error for FrameError {}

impl From<std::io::Error> for FrameError {
    fn from(e: std::io::Error) -> Self {
        FrameError::Io(e)
    }
}
