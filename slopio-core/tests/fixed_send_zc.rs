use std::io::{Read, Write};
use std::net::TcpListener;
use std::rc::Rc;

use slopio_core::net::{FixedSendBufPool, connect_tcp, socket_write_fixed_zc};
use slopio_core::runtime::main_thread;

// Zero-copy send from a registered ("fixed") buffer. Drives several buffers
// through the pool so the `Sent -> Notif -> Ended` path runs for each, then
// lets each release on drop. Verifies the server receives every byte in order
// and that the slots round-trip back to the pool.
#[test]
fn fixed_send_zc_end_to_end() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).unwrap();
        buf
    });

    main_thread(async move {
        let pool = Rc::new(FixedSendBufPool::new(3, 64).unwrap());
        let socket = connect_tcp(addr).await.unwrap();

        // Send 6 messages, cycling through the 3-slot pool twice — each send
        // resolves only on its zero-copy notification, after which the buffer
        // is dropped and its slot reused.
        for i in 0..6u32 {
            let mut b = pool.try_acquire().expect("slot free");
            write!(b, "zc-{:02} ", i).unwrap();
            let expect_len = b.len();

            let (result, b) = socket_write_fixed_zc(&socket, b).await;
            assert_eq!(result.unwrap(), expect_len);
            drop(b); // slot returns to the pool
        }

        // All slots came back to the pool: three can be held at once.
        assert_eq!(pool.capacity(), 3);
        let all: Vec<_> = (0..3)
            .map(|_| pool.try_acquire().expect("slot free"))
            .collect();
        assert!(
            pool.try_acquire().is_none(),
            "pool handed out a fourth slot"
        );
        drop(all);

        drop(socket);
    });

    let received = server.join().unwrap();
    let s = String::from_utf8(received).unwrap();
    let expected: String = (0..6u32).map(|i| format!("zc-{i:02} ")).collect();
    assert_eq!(s, expected);
}

// `MSG_WAITALL` (the zero-copy default) must make one submission cover the
// whole buffer even when it is far larger than the socket's send buffer.
// Without it the kernel returns a partial count and the caller has to
// resubmit, paying a notification round trip per round.
#[test]
fn zc_send_completes_in_one_submission() {
    use slopio_core::net::socket_send_zc;

    const TOTAL: usize = 8 * 1024 * 1024;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).unwrap();
        buf.len()
    });

    main_thread(async move {
        let socket = connect_tcp(addr).await.unwrap();
        let payload = vec![b'z'; TOTAL];
        // One `socket_send_zc`, not `send_all_zc`: no retry loop to hide a
        // short send.
        let (result, _payload) = socket_send_zc(&socket, payload).await;
        assert_eq!(
            result.unwrap(),
            TOTAL,
            "short send: MSG_WAITALL not applied"
        );
        drop(socket);
    });

    assert_eq!(server.join().unwrap(), TOTAL);
}
