use runtime::io_driver::operations::sockets::multi_accept::MultiAccept;
use runtime::main_thread;
use runtime_core::channels::unbounded_channel;
use runtime_h2::decoding::codec::{ConnectionEvent, ConnectionFrameReader};
use runtime_h2::h2_types::frame::{Frame, Reason, StreamId};
use runtime_streams::StreamExt;
use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;

/// Encode a 9-byte HTTP/2 frame header.
fn encode_frame_header(
    buf: &mut Vec<u8>,
    payload_len: usize,
    frame_type: u8,
    flags: u8,
    stream_id: u32,
) {
    buf.push((payload_len >> 16) as u8);
    buf.push((payload_len >> 8) as u8);
    buf.push(payload_len as u8);
    buf.push(frame_type);
    buf.push(flags);
    buf.extend_from_slice(&stream_id.to_be_bytes());
}

fn make_reader(
    socket: runtime_core::io_driver::operations::sockets::SocketWrapper,
) -> (
    ConnectionFrameReader,
    runtime_core::channels::Receiver<ConnectionEvent>,
) {
    let (tx, rx) = unbounded_channel();
    (ConnectionFrameReader::new(socket, tx), rx)
}

#[test]
fn read_data_frame_from_socket() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9300).unwrap();
            let socket = ma.next().await.unwrap();

            let (mut reader, _rx) = make_reader(socket);
            let frame = reader.next_frame().await.unwrap();

            match frame {
                Frame::Data(data) => {
                    assert_eq!(data.stream_id(), StreamId::from(1u32));
                    assert!(data.is_end_stream());
                    assert_eq!(data.payload().as_ref(), b"hello from tcp");
                    true
                }
                other => panic!("expected Data, got {:?}", other),
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9300").unwrap();
    let mut buf = Vec::new();
    let payload = b"hello from tcp";
    encode_frame_header(&mut buf, payload.len(), 0x0, 0x1, 1);
    buf.extend_from_slice(payload);
    client.write_all(&buf).unwrap();

    assert!(handle.join().unwrap());
}

#[test]
fn read_settings_frame_from_socket() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9301).unwrap();
            let socket = ma.next().await.unwrap();

            let (mut reader, _rx) = make_reader(socket);
            let frame = reader.next_frame().await.unwrap();

            match frame {
                Frame::Settings(s) => {
                    assert!(!s.is_ack());
                    assert_eq!(s.initial_window_size(), Some(65535));
                    assert_eq!(s.max_concurrent_streams(), Some(100));
                    true
                }
                other => panic!("expected Settings, got {:?}", other),
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9301").unwrap();
    let mut buf = Vec::new();
    let mut payload = Vec::new();
    payload.extend_from_slice(&0x3u16.to_be_bytes());
    payload.extend_from_slice(&100u32.to_be_bytes());
    payload.extend_from_slice(&0x4u16.to_be_bytes());
    payload.extend_from_slice(&65535u32.to_be_bytes());
    encode_frame_header(&mut buf, payload.len(), 0x4, 0x0, 0);
    buf.extend_from_slice(&payload);
    client.write_all(&buf).unwrap();

    assert!(handle.join().unwrap());
}

#[test]
fn read_ping_frame_from_socket() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9302).unwrap();
            let socket = ma.next().await.unwrap();

            let (mut reader, _rx) = make_reader(socket);
            let frame = reader.next_frame().await.unwrap();

            match frame {
                Frame::Ping(p) => {
                    assert!(!p.is_ack());
                    assert_eq!(*p.payload(), [0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE]);
                    true
                }
                other => panic!("expected Ping, got {:?}", other),
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9302").unwrap();
    let mut buf = Vec::new();
    encode_frame_header(&mut buf, 8, 0x6, 0x0, 0);
    buf.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE]);
    client.write_all(&buf).unwrap();

    assert!(handle.join().unwrap());
}

#[test]
fn read_multiple_frames_from_socket() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9303).unwrap();
            let socket = ma.next().await.unwrap();

            let (mut reader, _rx) = make_reader(socket);

            let f1 = reader.next_frame().await.unwrap();
            assert!(matches!(&f1, Frame::Settings(s) if s.is_ack()));

            let f2 = reader.next_frame().await.unwrap();
            match &f2 {
                Frame::WindowUpdate(wu) => {
                    assert_eq!(wu.stream_id(), StreamId::from(0u32));
                    assert_eq!(wu.size_increment(), 1048576);
                }
                other => panic!("expected WindowUpdate, got {:?}", other),
            }

            let f3 = reader.next_frame().await.unwrap();
            match &f3 {
                Frame::Data(d) => {
                    assert_eq!(d.stream_id(), StreamId::from(1u32));
                    assert_eq!(d.payload().as_ref(), b"body");
                }
                other => panic!("expected Data, got {:?}", other),
            }

            let f4 = reader.next_frame().await.unwrap();
            match &f4 {
                Frame::Reset(rst) => {
                    assert_eq!(rst.stream_id(), StreamId::from(3u32));
                    assert_eq!(rst.reason(), Reason::CANCEL);
                }
                other => panic!("expected Reset, got {:?}", other),
            }

            true
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9303").unwrap();
    let mut buf = Vec::new();

    encode_frame_header(&mut buf, 0, 0x4, 0x1, 0);

    encode_frame_header(&mut buf, 4, 0x8, 0x0, 0);
    buf.extend_from_slice(&1048576u32.to_be_bytes());

    let body = b"body";
    encode_frame_header(&mut buf, body.len(), 0x0, 0x0, 1);
    buf.extend_from_slice(body);

    encode_frame_header(&mut buf, 4, 0x3, 0x0, 3);
    buf.extend_from_slice(&8u32.to_be_bytes());

    client.write_all(&buf).unwrap();

    assert!(handle.join().unwrap());
}

#[test]
fn read_frames_sent_in_chunks() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9304).unwrap();
            let socket = ma.next().await.unwrap();

            let (mut reader, _rx) = make_reader(socket);
            let frame = reader.next_frame().await.unwrap();

            match frame {
                Frame::Data(d) => {
                    assert_eq!(d.stream_id(), StreamId::from(5u32));
                    assert_eq!(d.payload().as_ref(), b"chunked payload data");
                    true
                }
                other => panic!("expected Data, got {:?}", other),
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9304").unwrap();
    let mut buf = Vec::new();
    let payload = b"chunked payload data";
    encode_frame_header(&mut buf, payload.len(), 0x0, 0x0, 5);
    buf.extend_from_slice(payload);

    client.write_all(&buf[..4]).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    client.write_all(&buf[4..12]).unwrap();
    std::thread::sleep(Duration::from_millis(50));
    client.write_all(&buf[12..]).unwrap();

    assert!(handle.join().unwrap());
}

#[test]
fn read_goaway_from_socket() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9305).unwrap();
            let socket = ma.next().await.unwrap();

            let (mut reader, _rx) = make_reader(socket);
            let frame = reader.next_frame().await.unwrap();

            match frame {
                Frame::GoAway(ga) => {
                    assert_eq!(ga.last_stream_id(), StreamId::from(7u32));
                    assert_eq!(ga.reason(), Reason::NO_ERROR);
                    assert_eq!(ga.debug_data().as_ref(), b"goodbye");
                    true
                }
                other => panic!("expected GoAway, got {:?}", other),
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9305").unwrap();
    let mut buf = Vec::new();
    let debug = b"goodbye";
    encode_frame_header(&mut buf, 8 + debug.len(), 0x7, 0x0, 0);
    buf.extend_from_slice(&7u32.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());
    buf.extend_from_slice(debug);
    client.write_all(&buf).unwrap();

    assert!(handle.join().unwrap());
}

#[test]
fn read_headers_from_socket() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9306).unwrap();
            let socket = ma.next().await.unwrap();

            let (mut reader, _rx) = make_reader(socket);
            let frame = reader.next_frame().await.unwrap();

            match frame {
                Frame::Headers(h) => {
                    assert_eq!(h.stream_id(), StreamId::from(1u32));
                    assert!(h.is_end_headers());
                    true
                }
                other => panic!("expected Headers, got {:?}", other),
            }
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9306").unwrap();
    let mut buf = Vec::new();
    let hpack = &[0x82, 0x84];
    encode_frame_header(&mut buf, hpack.len(), 0x1, 0x4, 1);
    buf.extend_from_slice(hpack);
    client.write_all(&buf).unwrap();

    assert!(handle.join().unwrap());
}

/// Test process() — sends Settings + Ping + GoAway, verifies they arrive
/// on the connection event channel. GoAway causes process() to return Ok(()).
#[test]
fn process_dispatches_connection_events() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9307).unwrap();
            let socket = ma.next().await.unwrap();

            let (mut reader, mut rx) = make_reader(socket);

            // process() runs until GoAway, then returns Ok(())
            reader.process().await.unwrap();

            // Now drain the connection events channel
            let e1 = rx.recv().await.unwrap();
            match e1 {
                ConnectionEvent::Settings(s) => {
                    assert!(s.is_ack());
                }
                _ => panic!("expected Settings event"),
            }

            let e2 = rx.recv().await.unwrap();
            match e2 {
                ConnectionEvent::Ping(p) => {
                    assert_eq!(*p.payload(), [1, 2, 3, 4, 5, 6, 7, 8]);
                }
                _ => panic!("expected Ping event"),
            }

            let e3 = rx.recv().await.unwrap();
            match e3 {
                ConnectionEvent::WindowUpdate(inc) => {
                    assert_eq!(inc, 8192);
                }
                _ => panic!("expected WindowUpdate event"),
            }

            let e4 = rx.recv().await.unwrap();
            match e4 {
                ConnectionEvent::GoAway(ga) => {
                    assert_eq!(ga.reason(), Reason::NO_ERROR);
                }
                _ => panic!("expected GoAway event"),
            }

            true
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9307").unwrap();
    let mut buf = Vec::new();

    // Settings ACK
    encode_frame_header(&mut buf, 0, 0x4, 0x1, 0);

    // Ping
    encode_frame_header(&mut buf, 8, 0x6, 0x0, 0);
    buf.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);

    // WindowUpdate connection-level
    encode_frame_header(&mut buf, 4, 0x8, 0x0, 0);
    buf.extend_from_slice(&8192u32.to_be_bytes());

    // GoAway — causes process() to return
    encode_frame_header(&mut buf, 8, 0x7, 0x0, 0);
    buf.extend_from_slice(&0u32.to_be_bytes()); // last stream id 0
    buf.extend_from_slice(&0u32.to_be_bytes()); // NO_ERROR

    client.write_all(&buf).unwrap();

    assert!(handle.join().unwrap());
}

/// Test process() dispatches stream frames to registered stream channels.
#[test]
fn process_dispatches_stream_frames() {
    let handle = std::thread::spawn(|| {
        main_thread(async {
            let mut ma = MultiAccept::new(9308).unwrap();
            let socket = ma.next().await.unwrap();

            let (mut reader, _conn_rx) = make_reader(socket);

            // Register stream 1
            let (stream_tx, mut stream_rx) = unbounded_channel();
            reader.register_stream(StreamId::from(1u32), stream_tx);

            // process() until GoAway
            reader.process().await.unwrap();

            // Check stream 1 received its DATA frame
            let frame = stream_rx.recv().await.unwrap();
            match frame {
                Frame::Data(d) => {
                    assert_eq!(d.stream_id(), StreamId::from(1u32));
                    assert_eq!(d.payload().as_ref(), b"stream data");
                    assert!(d.is_end_stream());
                }
                other => panic!("expected Data on stream 1, got {:?}", other),
            }

            true
        })
    });

    std::thread::sleep(Duration::from_millis(100));

    let mut client = TcpStream::connect("127.0.0.1:9308").unwrap();
    let mut buf = Vec::new();

    // DATA on stream 1, END_STREAM
    let payload = b"stream data";
    encode_frame_header(&mut buf, payload.len(), 0x0, 0x1, 1);
    buf.extend_from_slice(payload);

    // GoAway to stop process()
    encode_frame_header(&mut buf, 8, 0x7, 0x0, 0);
    buf.extend_from_slice(&0u32.to_be_bytes());
    buf.extend_from_slice(&0u32.to_be_bytes());

    client.write_all(&buf).unwrap();

    assert!(handle.join().unwrap());
}
