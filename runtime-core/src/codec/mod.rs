use std::io;

use bytes::{Buf, BytesMut};

use crate::io_driver::operations::sockets::{SocketWrapper, recv::socket_recv, send::socket_send};

/// Decode frames from a `BytesMut` buffer.
///
/// Consume bytes from `src` (via `split_to`, `advance`, etc.) and return
/// `Some(item)` when a complete frame is available, or `None` if more data
/// is needed.
pub trait Decoder {
    type Item;
    type Error: From<io::Error>;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error>;

    /// Called when the peer closes the connection.
    ///
    /// Default: returns `Ok(None)` on empty buffer, error if bytes remain.
    fn decode_eof(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.decode(src)? {
            Some(item) => Ok(Some(item)),
            None if src.is_empty() => Ok(None),
            None => Err(io::Error::from(io::ErrorKind::UnexpectedEof).into()),
        }
    }
}

/// Encode items into a `BytesMut` buffer.
///
/// Append the encoded bytes to `dst`; [`Framed`] handles flushing.
pub trait Encoder<Item> {
    type Error: From<io::Error>;

    fn encode(&mut self, item: Item, dst: &mut BytesMut) -> Result<(), Self::Error>;
}

const DEFAULT_READ_CHUNK: usize = 8192;

/// Wraps a [`SocketWrapper`] with a codec, providing framed read/write.
///
/// Analogous to `tokio_util::codec::Framed` but built on this runtime's
/// classic (non-provided-buffer) recv path.
pub struct Framed<C> {
    socket: SocketWrapper,
    codec: C,
    read_buf: BytesMut,
    write_buf: BytesMut,
    read_chunk: usize,
    is_eof: bool,
}

impl<C> Framed<C> {
    pub fn new(socket: SocketWrapper, codec: C) -> Self {
        Self {
            socket,
            codec,
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            read_chunk: DEFAULT_READ_CHUNK,
            is_eof: false,
        }
    }

    pub fn with_chunk_size(mut self, read_chunk: usize) -> Self {
        self.read_chunk = read_chunk;
        self
    }

    pub fn codec(&self) -> &C {
        &self.codec
    }

    pub fn codec_mut(&mut self) -> &mut C {
        &mut self.codec
    }

    pub fn socket(&self) -> &SocketWrapper {
        &self.socket
    }

    pub fn into_inner(self) -> (SocketWrapper, C) {
        (self.socket, self.codec)
    }
}

impl<C: Decoder> Framed<C> {
    /// Receive and decode the next frame.
    ///
    /// Loops internally, reading from the socket until the codec produces a
    /// complete item. Returns `Ok(None)` on clean EOF with no partial frame.
    pub async fn next(&mut self) -> Result<Option<C::Item>, C::Error> {
        loop {
            if let Some(item) = self.codec.decode(&mut self.read_buf)? {
                return Ok(Some(item));
            }

            if self.is_eof {
                return self.codec.decode_eof(&mut self.read_buf);
            }

            let n = self.fill_read_buf().await.map_err(C::Error::from)?;
            if n == 0 {
                self.is_eof = true;
            }
        }
    }

    async fn fill_read_buf(&mut self) -> io::Result<usize> {
        self.read_buf.reserve(self.read_chunk);

        // SAFETY: socket_recv initialises the first `n` bytes of spare capacity;
        // we advance the BytesMut length by exactly that amount.
        let spare = self.read_buf.spare_capacity_mut();
        let n = socket_recv(&self.socket, &mut spare[..self.read_chunk], self.read_chunk).await?;
        unsafe {
            self.read_buf.set_len(self.read_buf.len() + n);
        }
        Ok(n)
    }
}

impl<C, Item> Framed<C>
where
    C: Encoder<Item>,
{
    /// Encode `item` and send all bytes before returning.
    pub async fn send(&mut self, item: Item) -> Result<(), C::Error> {
        self.codec.encode(item, &mut self.write_buf)?;
        self.flush().await.map_err(C::Error::from)
    }

    /// Flush the write buffer, sending all pending bytes.
    pub async fn flush(&mut self) -> io::Result<()> {
        while !self.write_buf.is_empty() {
            let n = socket_send(&self.socket, &self.write_buf).await?;
            if n == 0 {
                return Err(io::Error::from(io::ErrorKind::WriteZero));
            }
            self.write_buf.advance(n);
        }
        Ok(())
    }
}
