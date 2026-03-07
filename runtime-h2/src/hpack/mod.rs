mod decoder;
mod encoder;
pub(crate) mod header;
pub(crate) mod huffman;
mod table;

// #[cfg(test)]
// mod test;

use crate::hpack;

pub use self::decoder::{Decoder, DecoderError, NeedMore};
pub use self::encoder::Encoder;
pub use self::header::{BytesStr, Header};

/// Errors that can occur during parsing an HTTP/2 frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    /// A length value other than 8 was set on a PING message.
    BadFrameSize,

    /// The padding length was larger than the frame-header-specified
    /// length of the payload.
    TooMuchPadding,

    /// An invalid setting value was provided
    InvalidSettingValue,

    /// An invalid window update value
    InvalidWindowUpdateValue,

    /// The payload length specified by the frame header was not the
    /// value necessary for the specific frame type.
    InvalidPayloadLength,

    /// Received a payload with an ACK settings frame
    InvalidPayloadAckSettings,

    /// An invalid stream identifier was provided.
    ///
    /// This is returned if a SETTINGS or PING frame is received with a stream
    /// identifier other than zero.
    InvalidStreamId,

    /// A request or response is malformed.
    MalformedMessage,

    /// An invalid stream dependency ID was provided
    ///
    /// This is returned if a HEADERS or PRIORITY frame is received with an
    /// invalid stream identifier.
    InvalidDependencyId,

    /// Failed to perform HPACK decoding
    Hpack(hpack::DecoderError),
}
