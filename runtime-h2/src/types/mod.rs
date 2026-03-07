use bytes::BytesMut;

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
        payload: BytesMut,
    },
    RstStream {
        stream_id: u32,
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
        payload: BytesMut,
    },
}

#[derive(Debug)]
pub enum FrameError {
    FrameSizeError(u32),
    ProtocolError(&'static str),
}
