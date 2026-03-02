#[derive(Clone, Copy, Debug)]
pub enum Identifier {
    Numeric(u32),
}

pub struct IggyMessage {
    pub id: u128,
    pub payload: Vec<u8>,
}

impl IggyMessage {
    pub fn new(payload: Vec<u8>) -> Self {
        IggyMessage { id: 0, payload }
    }

    pub fn with_id(id: u128, payload: Vec<u8>) -> Self {
        IggyMessage { id, payload }
    }
}

/// Message received from a poll operation.
pub struct ReceivedMessage {
    pub offset: u64,
    pub timestamp: u64,
    pub id: u128,
    pub payload: Vec<u8>,
}
