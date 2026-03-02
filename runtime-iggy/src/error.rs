use std::io;

#[derive(Debug)]
pub enum IggyError {
    Io(io::Error),
    Server(u32),
    Protocol(&'static str),
    InvalidState(&'static str),
}

impl From<io::Error> for IggyError {
    fn from(e: io::Error) -> Self {
        IggyError::Io(e)
    }
}

impl std::fmt::Display for IggyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IggyError::Io(e) => write!(f, "IO error: {e}"),
            IggyError::Server(code) => write!(f, "Iggy server error: status={code}"),
            IggyError::Protocol(msg) => write!(f, "Protocol error: {msg}"),
            IggyError::InvalidState(msg) => write!(f, "Invalid state: {msg}"),
        }
    }
}

impl std::error::Error for IggyError {}
