//! Minimal replacement for `tokio_util::codec::length_delimited` and
//! `tokio_util::io::poll_write_buf`.

use bytes::{Buf, BytesMut};
use futures_core::Stream;
use std::fmt;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

// ===== LengthDelimitedCodecError =====

/// Returned when a frame exceeds the configured maximum frame length.
#[derive(Debug)]
pub struct LengthDelimitedCodecError;

impl fmt::Display for LengthDelimitedCodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("frame size exceeded maximum")
    }
}

impl std::error::Error for LengthDelimitedCodecError {}

// ===== LengthDelimitedCodec =====

/// Codec state exposed for configuration (max frame length).
#[derive(Debug, Clone)]
pub struct LengthDelimitedCodec {
    max_frame_length: usize,
}

impl LengthDelimitedCodec {
    pub fn max_frame_length(&self) -> usize {
        self.max_frame_length
    }

    pub fn set_max_frame_length(&mut self, val: usize) {
        self.max_frame_length = val;
    }
}

// ===== Builder =====

/// Builder for [`FramedRead`].
///
/// API mirrors `tokio_util::codec::length_delimited::Builder`.
pub struct Builder {
    length_field_offset: usize,
    length_field_length: usize,
    length_adjustment: i64,
    num_skip: usize,
    big_endian: bool,
    max_frame_length: usize,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    pub fn new() -> Self {
        Builder {
            length_field_offset: 0,
            length_field_length: 4,
            length_adjustment: 0,
            num_skip: 4, // length_field_offset + length_field_length
            big_endian: true,
            max_frame_length: usize::MAX,
        }
    }

    pub fn big_endian(mut self) -> Self {
        self.big_endian = true;
        self
    }

    pub fn length_field_length(mut self, n: usize) -> Self {
        self.length_field_length = n;
        self.num_skip = self.length_field_offset + n;
        self
    }

    pub fn length_adjustment(mut self, n: i64) -> Self {
        self.length_adjustment = n;
        self
    }

    pub fn num_skip(mut self, n: usize) -> Self {
        self.num_skip = n;
        self
    }

    pub fn new_read<T: AsyncRead>(self, inner: T) -> FramedRead<T> {
        FramedRead {
            inner,
            codec: LengthDelimitedCodec {
                max_frame_length: self.max_frame_length,
            },
            buf: BytesMut::with_capacity(8 * 1024),
            state: ReadState::Head,
            cfg: FramedReadCfg {
                length_field_offset: self.length_field_offset,
                length_field_length: self.length_field_length,
                length_adjustment: self.length_adjustment,
                num_skip: self.num_skip,
                big_endian: self.big_endian,
            },
        }
    }
}

// ===== FramedRead =====

#[derive(Debug)]
struct FramedReadCfg {
    length_field_offset: usize,
    length_field_length: usize,
    length_adjustment: i64,
    num_skip: usize,
    big_endian: bool,
}

#[derive(Clone, Copy, Debug)]
enum ReadState {
    Head,
    Data { frame_length: usize },
}

/// Wraps an [`AsyncRead`] and emits complete frames as [`BytesMut`].
///
/// Drop-in for `tokio_util::codec::FramedRead<T, LengthDelimitedCodec>`.
#[derive(Debug)]
pub struct FramedRead<T> {
    inner: T,
    codec: LengthDelimitedCodec,
    buf: BytesMut,
    state: ReadState,
    cfg: FramedReadCfg,
}

impl<T> FramedRead<T> {
    pub fn decoder(&self) -> &LengthDelimitedCodec {
        &self.codec
    }

    pub fn decoder_mut(&mut self) -> &mut LengthDelimitedCodec {
        &mut self.codec
    }

    pub fn get_ref(&self) -> &T {
        &self.inner
    }

    pub fn get_mut(&mut self) -> &mut T {
        &mut self.inner
    }
}

impl<T: AsyncRead + Unpin> FramedRead<T> {
    fn poll_fill(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<usize>> {
        self.buf.reserve(4096);
        let before = self.buf.len();
        let mut rb = ReadBuf::uninit(self.buf.spare_capacity_mut());
        ready!(Pin::new(&mut self.inner).poll_read(cx, &mut rb))?;
        let n = rb.filled().len();
        // SAFETY: poll_read initialised exactly n bytes of spare capacity.
        unsafe { self.buf.set_len(before + n) };
        Poll::Ready(Ok(n))
    }
}

impl<T: AsyncRead + Unpin> Stream for FramedRead<T> {
    type Item = io::Result<BytesMut>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let me = self.get_mut();

        loop {
            match me.state {
                ReadState::Head => {
                    let needed = me.cfg.length_field_offset + me.cfg.length_field_length;
                    while me.buf.len() < needed {
                        match ready!(me.poll_fill(cx)) {
                            Ok(0) => {
                                return if me.buf.is_empty() {
                                    Poll::Ready(None)
                                } else {
                                    Poll::Ready(Some(Err(io::Error::from(
                                        io::ErrorKind::UnexpectedEof,
                                    ))))
                                };
                            }
                            Ok(_) => {}
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    }

                    // Parse the length field.
                    let start = me.cfg.length_field_offset;
                    let end = start + me.cfg.length_field_length;
                    let mut raw: u64 = 0;
                    if me.cfg.big_endian {
                        for &b in &me.buf[start..end] {
                            raw = (raw << 8) | b as u64;
                        }
                    } else {
                        for &b in me.buf[start..end].iter().rev() {
                            raw = (raw << 8) | b as u64;
                        }
                    }

                    let adjusted = (raw as i128) + (me.cfg.length_adjustment as i128);
                    if adjusted < 0 || adjusted > usize::MAX as i128 {
                        return Poll::Ready(Some(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "invalid frame length after length_adjustment",
                        ))));
                    }
                    let frame_length = adjusted as usize;

                    if frame_length > me.codec.max_frame_length {
                        return Poll::Ready(Some(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            LengthDelimitedCodecError,
                        ))));
                    }

                    me.state = ReadState::Data { frame_length };
                }

                ReadState::Data { frame_length } => {
                    let needed = me.cfg.num_skip + frame_length;
                    while me.buf.len() < needed {
                        match ready!(me.poll_fill(cx)) {
                            Ok(0) => {
                                return Poll::Ready(Some(Err(io::Error::from(
                                    io::ErrorKind::UnexpectedEof,
                                ))));
                            }
                            Ok(_) => {}
                            Err(e) => return Poll::Ready(Some(Err(e))),
                        }
                    }

                    me.buf.advance(me.cfg.num_skip);
                    let frame = me.buf.split_to(frame_length);
                    me.state = ReadState::Head;
                    return Poll::Ready(Some(Ok(frame)));
                }
            }
        }
    }
}

// ===== poll_write_buf =====

/// Write as much of `buf` as possible to `io`, using vectored I/O when available.
///
/// Drop-in for `tokio_util::io::poll_write_buf`.
pub fn poll_write_buf<T, B>(
    io: Pin<&mut T>,
    cx: &mut Context<'_>,
    buf: &mut B,
) -> Poll<io::Result<usize>>
where
    T: AsyncWrite,
    B: Buf,
{
    if !buf.has_remaining() {
        return Poll::Ready(Ok(0));
    }

    let is_vectored = io.is_write_vectored();
    let n = if is_vectored {
        let mut slices = [IoSlice::new(&[]); 64];
        let cnt = buf.chunks_vectored(&mut slices);
        ready!(io.poll_write_vectored(cx, &slices[..cnt]))?
    } else {
        ready!(io.poll_write(cx, buf.chunk()))?
    };

    buf.advance(n);
    Poll::Ready(Ok(n))
}
