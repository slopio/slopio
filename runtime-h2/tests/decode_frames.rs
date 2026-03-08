use runtime_h2::codec::frames::FrameDecoder;
use runtime_h2::types::{Frame, Setting};
use std::path::Path;

#[test]
fn decode_all_frames_from_bin() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/frames.bin");
    let data = std::fs::read(&path).expect("run `cargo test --test gen_frames -- --ignored` first");

    let mut decoder = FrameDecoder::new();
    decoder.decode(&data).unwrap();

    // 15 wire frames, minus 1 PRIORITY (skipped), minus 1 CONTINUATION (merged) = 13
    assert_eq!(
        decoder.frames.len(),
        13,
        "expected 13 frames, got {}",
        decoder.frames.len()
    );

    // 1. SETTINGS with 2 parsed settings
    let f = decoder.frames.pop_front().unwrap();
    match f {
        Frame::Settings { ack, ref settings } => {
            assert!(!ack);
            assert_eq!(settings.len(), 2);
            assert_eq!(settings[0], Setting::HeaderTableSize(4096));
            assert_eq!(settings[1], Setting::MaxConcurrentStreams(100));
        }
        other => panic!("expected Settings, got {:?}", other),
    }

    // 2. SETTINGS ACK
    let f = decoder.frames.pop_front().unwrap();
    match f {
        Frame::Settings { ack, ref settings } => {
            assert!(ack);
            assert!(settings.is_empty());
        }
        other => panic!("expected Settings ACK, got {:?}", other),
    }

    // 3. WINDOW_UPDATE stream 0
    let f = decoder.frames.pop_front().unwrap();
    match f {
        Frame::WindowUpdate {
            stream_id,
            increment,
        } => {
            assert_eq!(stream_id, 0);
            assert_eq!(increment, 65535);
        }
        other => panic!("expected WindowUpdate, got {:?}", other),
    }

    // 4. HEADERS stream 1, END_STREAM, decoded: GET / https
    let f = decoder.frames.pop_front().unwrap();
    match f {
        Frame::Headers(ref h) => {
            assert_eq!(u32::from(h.stream_id()), 1);
            assert!(h.is_end_stream());
            let pseudo = h.pseudo();
            assert_eq!(pseudo.method.as_ref().unwrap(), "GET");
            assert_eq!(&**pseudo.path.as_ref().unwrap(), "/");
            assert_eq!(&**pseudo.scheme.as_ref().unwrap(), "https");
        }
        other => panic!("expected Headers, got {:?}", other),
    }

    // 5. HEADERS stream 3, no END_STREAM, decoded: GET / http
    let f = decoder.frames.pop_front().unwrap();
    match f {
        Frame::Headers(ref h) => {
            assert_eq!(u32::from(h.stream_id()), 3);
            assert!(!h.is_end_stream());
            let pseudo = h.pseudo();
            assert_eq!(pseudo.method.as_ref().unwrap(), "GET");
            assert_eq!(&**pseudo.path.as_ref().unwrap(), "/");
            assert_eq!(&**pseudo.scheme.as_ref().unwrap(), "http");
        }
        other => panic!("expected Headers, got {:?}", other),
    }

    // 6. DATA stream 1
    let f = decoder.frames.pop_front().unwrap();
    match f {
        Frame::Data {
            stream_id,
            end_stream,
            ref data,
        } => {
            assert_eq!(stream_id, 1);
            assert!(!end_stream);
            assert_eq!(data, b"Hello, HTTP/2!");
        }
        other => panic!("expected Data, got {:?}", other),
    }

    // 7. DATA stream 1, END_STREAM
    let f = decoder.frames.pop_front().unwrap();
    match f {
        Frame::Data {
            stream_id,
            end_stream,
            ref data,
        } => {
            assert_eq!(stream_id, 1);
            assert!(end_stream);
            assert_eq!(data, b"goodbye");
        }
        other => panic!("expected Data END_STREAM, got {:?}", other),
    }

    // 8. PING
    let f = decoder.frames.pop_front().unwrap();
    match f {
        Frame::Ping { ack, opaque } => {
            assert!(!ack);
            assert_eq!(opaque, [0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE]);
        }
        other => panic!("expected Ping, got {:?}", other),
    }

    // 9. PING ACK
    let f = decoder.frames.pop_front().unwrap();
    match f {
        Frame::Ping { ack, opaque } => {
            assert!(ack);
            assert_eq!(opaque, [0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE]);
        }
        other => panic!("expected Ping ACK, got {:?}", other),
    }

    // 10. RST_STREAM stream 1, CANCEL
    let f = decoder.frames.pop_front().unwrap();
    match f {
        Frame::RstStream {
            stream_id,
            error_code,
        } => {
            assert_eq!(stream_id, 1);
            assert_eq!(error_code, 0x8);
        }
        other => panic!("expected RstStream, got {:?}", other),
    }

    // 11. WINDOW_UPDATE stream 1
    let f = decoder.frames.pop_front().unwrap();
    match f {
        Frame::WindowUpdate {
            stream_id,
            increment,
        } => {
            assert_eq!(stream_id, 1);
            assert_eq!(increment, 32768);
        }
        other => panic!("expected WindowUpdate, got {:?}", other),
    }

    // 12. GOAWAY
    let f = decoder.frames.pop_front().unwrap();
    match f {
        Frame::GoAway {
            last_stream_id,
            error_code,
            ref debug_data,
        } => {
            assert_eq!(last_stream_id, 1);
            assert_eq!(error_code, 0);
            assert_eq!(debug_data, b"server shutting down");
        }
        other => panic!("expected GoAway, got {:?}", other),
    }

    // 13. HEADERS stream 5 (from HEADERS + CONTINUATION), decoded: POST / https
    let f = decoder.frames.pop_front().unwrap();
    match f {
        Frame::Headers(ref h) => {
            assert_eq!(u32::from(h.stream_id()), 5);
            assert!(!h.is_end_stream());
            let pseudo = h.pseudo();
            assert_eq!(pseudo.method.as_ref().unwrap(), "POST");
            assert_eq!(&**pseudo.path.as_ref().unwrap(), "/");
            assert_eq!(&**pseudo.scheme.as_ref().unwrap(), "https");
        }
        other => panic!("expected Headers from continuation, got {:?}", other),
    }
}

#[test]
fn decode_frames_one_byte_at_a_time() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/frames.bin");
    let data = std::fs::read(&path).expect("run `cargo test --test gen_frames -- --ignored` first");

    let mut decoder = FrameDecoder::new();

    // Feed one byte at a time to stress reassembly logic
    for byte in &data {
        decoder.decode(std::slice::from_ref(byte)).unwrap();
    }

    assert_eq!(decoder.frames.len(), 13);
}
