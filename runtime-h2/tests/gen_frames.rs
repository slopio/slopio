/// Generates a binary file containing a sequence of valid HTTP/2 frames
/// for testing the frame decoder.
///
/// Run with: cargo test -p runtime-h2 --test gen_frames -- --ignored
use std::fs;
use std::path::Path;

/// HTTP/2 frame header: 3-byte length + 1-byte type + 1-byte flags + 4-byte stream_id (big-endian)
fn frame(kind: u8, flags: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
    let len = payload.len() as u32;
    let mut buf = Vec::with_capacity(9 + payload.len());
    // 3-byte length
    buf.push((len >> 16) as u8);
    buf.push((len >> 8) as u8);
    buf.push(len as u8);
    buf.push(kind);
    buf.push(flags);
    buf.extend_from_slice(&stream_id.to_be_bytes());
    buf.extend_from_slice(payload);
    buf
}

#[test]
#[ignore]
fn generate_frames_bin() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    fs::create_dir_all(&dir).unwrap();

    let mut out = Vec::new();

    // --- 1. SETTINGS frame (type 0x4), stream 0, no ack ---
    // Two settings: HEADER_TABLE_SIZE=4096, MAX_CONCURRENT_STREAMS=100
    let mut settings_payload = Vec::new();
    settings_payload.extend_from_slice(&1u16.to_be_bytes()); // HEADER_TABLE_SIZE
    settings_payload.extend_from_slice(&4096u32.to_be_bytes());
    settings_payload.extend_from_slice(&3u16.to_be_bytes()); // MAX_CONCURRENT_STREAMS
    settings_payload.extend_from_slice(&100u32.to_be_bytes());
    out.extend(frame(0x4, 0x0, 0, &settings_payload));

    // --- 2. SETTINGS ACK (type 0x4, flags=0x1, empty payload) ---
    out.extend(frame(0x4, 0x1, 0, &[]));

    // --- 3. WINDOW_UPDATE frame (type 0x8), stream 0, increment=65535 ---
    out.extend(frame(0x8, 0x0, 0, &65535u32.to_be_bytes()));

    // --- 4. HEADERS frame (type 0x1), stream 1, END_HEADERS=0x4 ---
    // Fake HPACK payload (not real headers, just bytes for frame-level testing)
    let hpack = b"\x82\x86\x84\x41\x8a\x08\x9d\x5c\x0b\x81\x70\xdc\x78\x0f\x03";
    out.extend(frame(0x1, 0x4, 1, hpack));

    // --- 5. HEADERS frame (type 0x1), stream 1, END_HEADERS|END_STREAM=0x5 ---
    let hpack2 = b"\x82\x86\x84";
    out.extend(frame(0x1, 0x5, 1, hpack2));

    // --- 6. DATA frame (type 0x0), stream 1, no flags ---
    let body = b"Hello, HTTP/2!";
    out.extend(frame(0x0, 0x0, 1, body));

    // --- 7. DATA frame (type 0x0), stream 1, END_STREAM=0x1 ---
    let body2 = b"goodbye";
    out.extend(frame(0x0, 0x1, 1, body2));

    // --- 8. PING frame (type 0x6), stream 0, 8 opaque bytes ---
    out.extend(frame(0x6, 0x0, 0, &[0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE]));

    // --- 9. PING ACK (type 0x6, flags=0x1) ---
    out.extend(frame(0x6, 0x1, 0, &[0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE]));

    // --- 10. RST_STREAM (type 0x3), stream 1, error_code=CANCEL(0x8) ---
    out.extend(frame(0x3, 0x0, 1, &8u32.to_be_bytes()));

    // --- 11. WINDOW_UPDATE (type 0x8), stream 1, increment=32768 ---
    out.extend(frame(0x8, 0x0, 1, &32768u32.to_be_bytes()));

    // --- 12. GOAWAY (type 0x7), stream 0, last_stream=1, error=NO_ERROR(0x0) ---
    let mut goaway = Vec::new();
    goaway.extend_from_slice(&1u32.to_be_bytes()); // last_stream_id
    goaway.extend_from_slice(&0u32.to_be_bytes()); // error_code
    goaway.extend_from_slice(b"server shutting down");
    out.extend(frame(0x7, 0x0, 0, &goaway));

    // --- 13. CONTINUATION (type 0x9), stream 3, no END_HEADERS ---
    out.extend(frame(0x9, 0x0, 3, b"\x82\x86"));

    // --- 14. CONTINUATION (type 0x9), stream 3, END_HEADERS=0x4 ---
    out.extend(frame(0x9, 0x4, 3, b"\x84"));

    // --- 15. PRIORITY (type 0x2), stream 5 — decoder should skip this ---
    let mut prio = Vec::new();
    prio.extend_from_slice(&0u32.to_be_bytes()); // stream dependency
    prio.push(15); // weight
    out.extend(frame(0x2, 0x0, 5, &prio));

    let path = dir.join("frames.bin");
    fs::write(&path, &out).unwrap();
    eprintln!("wrote {} bytes to {}", out.len(), path.display());
}
