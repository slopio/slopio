use bytes::{BufMut, BytesMut};
use runtime_h2::decoding::codec::{DecodeError, FrameDecoder};
use runtime_h2::h2_types::frame::{Frame, Reason, StreamId};

/// Encode a 9-byte frame header into buf.
fn encode_frame_header(
    buf: &mut BytesMut,
    payload_len: usize,
    frame_type: u8,
    flags: u8,
    stream_id: u32,
) {
    buf.put_u8((payload_len >> 16) as u8);
    buf.put_u8((payload_len >> 8) as u8);
    buf.put_u8(payload_len as u8);
    buf.put_u8(frame_type);
    buf.put_u8(flags);
    buf.put_u32(stream_id);
}

#[test]
fn decode_data_frame() {
    let mut dec = FrameDecoder::new();
    let payload = b"hello world";
    encode_frame_header(&mut dec.buf, payload.len(), 0x0, 0x0, 1);
    dec.buf.extend_from_slice(payload);

    match dec.decode().unwrap().unwrap() {
        Frame::Data(data) => {
            assert_eq!(data.stream_id(), StreamId::from(1u32));
            assert!(!data.is_end_stream());
            assert_eq!(data.payload().as_ref(), b"hello world");
        }
        other => panic!("expected Data, got {:?}", other),
    }
}

#[test]
fn decode_data_end_stream() {
    let mut dec = FrameDecoder::new();
    encode_frame_header(&mut dec.buf, 3, 0x0, 0x1, 3);
    dec.buf.extend_from_slice(b"bye");

    match dec.decode().unwrap().unwrap() {
        Frame::Data(data) => {
            assert_eq!(data.stream_id(), StreamId::from(3u32));
            assert!(data.is_end_stream());
            assert_eq!(data.payload().as_ref(), b"bye");
        }
        other => panic!("expected Data, got {:?}", other),
    }
}

#[test]
fn decode_empty_data_end_stream() {
    let mut dec = FrameDecoder::new();
    encode_frame_header(&mut dec.buf, 0, 0x0, 0x1, 1);

    match dec.decode().unwrap().unwrap() {
        Frame::Data(data) => {
            assert!(data.is_end_stream());
            assert_eq!(data.payload().len(), 0);
        }
        other => panic!("expected Data, got {:?}", other),
    }
}

#[test]
fn decode_settings() {
    let mut dec = FrameDecoder::new();
    let mut payload = BytesMut::new();
    payload.put_u16(0x3); // MAX_CONCURRENT_STREAMS
    payload.put_u32(100);
    payload.put_u16(0x4); // INITIAL_WINDOW_SIZE
    payload.put_u32(32768);

    encode_frame_header(&mut dec.buf, payload.len(), 0x4, 0x0, 0);
    dec.buf.extend_from_slice(&payload);

    match dec.decode().unwrap().unwrap() {
        Frame::Settings(s) => {
            assert!(!s.is_ack());
            assert_eq!(s.max_concurrent_streams(), Some(100));
            assert_eq!(s.initial_window_size(), Some(32768));
        }
        other => panic!("expected Settings, got {:?}", other),
    }
}

#[test]
fn decode_settings_ack() {
    let mut dec = FrameDecoder::new();
    encode_frame_header(&mut dec.buf, 0, 0x4, 0x1, 0);

    match dec.decode().unwrap().unwrap() {
        Frame::Settings(s) => assert!(s.is_ack()),
        other => panic!("expected Settings ACK, got {:?}", other),
    }
}

#[test]
fn decode_ping() {
    let mut dec = FrameDecoder::new();
    let opaque: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
    encode_frame_header(&mut dec.buf, 8, 0x6, 0x0, 0);
    dec.buf.extend_from_slice(&opaque);

    match dec.decode().unwrap().unwrap() {
        Frame::Ping(p) => {
            assert!(!p.is_ack());
            assert_eq!(*p.payload(), opaque);
        }
        other => panic!("expected Ping, got {:?}", other),
    }
}

#[test]
fn decode_ping_ack() {
    let mut dec = FrameDecoder::new();
    let opaque: [u8; 8] = [1, 2, 3, 4, 5, 6, 7, 8];
    encode_frame_header(&mut dec.buf, 8, 0x6, 0x1, 0);
    dec.buf.extend_from_slice(&opaque);

    match dec.decode().unwrap().unwrap() {
        Frame::Ping(p) => assert!(p.is_ack()),
        other => panic!("expected Ping ACK, got {:?}", other),
    }
}

#[test]
fn decode_window_update() {
    let mut dec = FrameDecoder::new();
    encode_frame_header(&mut dec.buf, 4, 0x8, 0x0, 1);
    dec.buf.put_u32(65535);

    match dec.decode().unwrap().unwrap() {
        Frame::WindowUpdate(wu) => {
            assert_eq!(wu.stream_id(), StreamId::from(1u32));
            assert_eq!(wu.size_increment(), 65535);
        }
        other => panic!("expected WindowUpdate, got {:?}", other),
    }
}

#[test]
fn decode_window_update_connection() {
    let mut dec = FrameDecoder::new();
    encode_frame_header(&mut dec.buf, 4, 0x8, 0x0, 0);
    dec.buf.put_u32(1024);

    match dec.decode().unwrap().unwrap() {
        Frame::WindowUpdate(wu) => {
            assert_eq!(wu.stream_id(), StreamId::from(0u32));
            assert_eq!(wu.size_increment(), 1024);
        }
        other => panic!("expected WindowUpdate, got {:?}", other),
    }
}

#[test]
fn decode_rst_stream() {
    let mut dec = FrameDecoder::new();
    encode_frame_header(&mut dec.buf, 4, 0x3, 0x0, 1);
    dec.buf.put_u32(0x8); // CANCEL

    match dec.decode().unwrap().unwrap() {
        Frame::Reset(rst) => {
            assert_eq!(rst.stream_id(), StreamId::from(1u32));
            assert_eq!(rst.reason(), Reason::CANCEL);
        }
        other => panic!("expected Reset, got {:?}", other),
    }
}

#[test]
fn decode_goaway() {
    let mut dec = FrameDecoder::new();
    encode_frame_header(&mut dec.buf, 8, 0x7, 0x0, 0);
    dec.buf.put_u32(5);
    dec.buf.put_u32(0x0);

    match dec.decode().unwrap().unwrap() {
        Frame::GoAway(ga) => {
            assert_eq!(ga.last_stream_id(), StreamId::from(5u32));
            assert_eq!(ga.reason(), Reason::NO_ERROR);
        }
        other => panic!("expected GoAway, got {:?}", other),
    }
}

#[test]
fn decode_goaway_with_debug_data() {
    let mut dec = FrameDecoder::new();
    let debug = b"something broke";
    encode_frame_header(&mut dec.buf, 8 + debug.len(), 0x7, 0x0, 0);
    dec.buf.put_u32(7);
    dec.buf.put_u32(0x2);
    dec.buf.extend_from_slice(debug);

    match dec.decode().unwrap().unwrap() {
        Frame::GoAway(ga) => {
            assert_eq!(ga.last_stream_id(), StreamId::from(7u32));
            assert_eq!(ga.reason(), Reason::INTERNAL_ERROR);
            assert_eq!(ga.debug_data().as_ref(), b"something broke");
        }
        other => panic!("expected GoAway, got {:?}", other),
    }
}

#[test]
fn decode_headers() {
    let mut dec = FrameDecoder::new();
    let hpack_payload: &[u8] = &[0x82, 0x84];
    encode_frame_header(&mut dec.buf, hpack_payload.len(), 0x1, 0x4, 1);
    dec.buf.extend_from_slice(hpack_payload);

    match dec.decode().unwrap().unwrap() {
        Frame::Headers(h) => {
            assert_eq!(h.stream_id(), StreamId::from(1u32));
            assert!(h.is_end_headers());
        }
        other => panic!("expected Headers, got {:?}", other),
    }
}

#[test]
fn decode_incomplete_header_returns_none() {
    let mut dec = FrameDecoder::new();
    dec.buf.extend_from_slice(&[0, 0, 5, 0, 0]);
    assert!(dec.decode().unwrap().is_none());
}

#[test]
fn decode_incomplete_payload_returns_none() {
    let mut dec = FrameDecoder::new();
    encode_frame_header(&mut dec.buf, 10, 0x0, 0x0, 1);
    dec.buf.extend_from_slice(&[1, 2, 3]);
    assert!(dec.decode().unwrap().is_none());
}

#[test]
fn decode_frame_too_large() {
    let mut dec = FrameDecoder::new();
    encode_frame_header(&mut dec.buf, 20000, 0x0, 0x0, 1);
    dec.buf.extend_from_slice(&vec![0u8; 20000]);

    match dec.decode() {
        Err(DecodeError::Connection(r)) => assert_eq!(r, Reason::FRAME_SIZE_ERROR),
        other => panic!("expected FRAME_SIZE_ERROR, got {:?}", other.err()),
    }
}

#[test]
fn decode_multiple_frames() {
    let mut dec = FrameDecoder::new();

    encode_frame_header(&mut dec.buf, 8, 0x6, 0x0, 0);
    dec.buf.extend_from_slice(&[10, 20, 30, 40, 50, 60, 70, 80]);

    encode_frame_header(&mut dec.buf, 9, 0x0, 0x1, 1);
    dec.buf.extend_from_slice(b"test data");

    encode_frame_header(&mut dec.buf, 4, 0x8, 0x0, 0);
    dec.buf.put_u32(4096);

    assert!(matches!(dec.decode().unwrap().unwrap(), Frame::Ping(_)));

    match dec.decode().unwrap().unwrap() {
        Frame::Data(d) => {
            assert_eq!(d.payload().as_ref(), b"test data");
            assert!(d.is_end_stream());
        }
        other => panic!("expected Data, got {:?}", other),
    }

    match dec.decode().unwrap().unwrap() {
        Frame::WindowUpdate(wu) => assert_eq!(wu.size_increment(), 4096),
        other => panic!("expected WindowUpdate, got {:?}", other),
    }

    assert!(dec.decode().unwrap().is_none());
}

#[test]
fn decode_unknown_frame_type_ignored() {
    let mut dec = FrameDecoder::new();
    encode_frame_header(&mut dec.buf, 4, 0xFF, 0x0, 1);
    dec.buf.extend_from_slice(&[0, 0, 0, 0]);
    assert!(dec.decode().unwrap().is_none());
}

#[test]
fn decode_priority_on_stream_zero_errors() {
    let mut dec = FrameDecoder::new();
    encode_frame_header(&mut dec.buf, 5, 0x2, 0x0, 0);
    dec.buf.put_u32(1);
    dec.buf.put_u8(15);

    match dec.decode() {
        Err(DecodeError::Connection(r)) => assert_eq!(r, Reason::PROTOCOL_ERROR),
        other => panic!("expected PROTOCOL_ERROR, got {:?}", other.err()),
    }
}
