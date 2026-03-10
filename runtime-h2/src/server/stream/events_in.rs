use crate::{codec::frames::headers::Headers, types::Frame};

#[derive(Debug)]
pub enum InputStreamEvent {
    Header { headers: Headers },
    Data { end_stream: bool, data: Vec<u8> },
    Window(WindowModification),
    ResetFrame,
}

#[derive(Debug)]
pub enum WindowModification {
    Increment(u32),
    InitialSizeChange { old: u32, new: u32 },
}

impl InputStreamEvent {
    pub fn from_frame(frame: Frame) -> Option<Self> {
        match frame {
            Frame::Data {
                end_stream, data, ..
            } => Some(Self::Data { end_stream, data }),
            Frame::Headers(headers) => Some(Self::Header { headers }),
            Frame::RstStream { .. } => Some(Self::ResetFrame),
            Frame::WindowUpdate { increment, .. } => {
                Some(Self::Window(WindowModification::Increment(increment)))
            }
            _ => None,
        }
    }
}
