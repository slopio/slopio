use std::io::{Read, Write};
use std::net::TcpListener;
use std::rc::Rc;

use runtime_core::net::{FixedSendBufPool, connect_tcp, socket_write_fixed_zc};
use runtime_core::runtime::main_thread;

// Zero-copy send from a registered ("fixed") buffer. Drives several buffers
// through the pool so the `Sent -> Notif -> Ended` path runs for each, then
// recycles the slots. Verifies the server receives every byte in order and
// that the slots round-trip back to the pool.
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
        // resolves only on its zero-copy notification, after which the slot is
        // released and reused.
        for i in 0..6u32 {
            let mut b = pool.acquire().await;
            write!(b, "zc-{:02} ", i).unwrap();
            let expect_len = b.len();

            let idx = b.buf_index();
            let on_cancel = {
                let pool = pool.clone();
                move || pool.release_index(idx)
            };

            let n = socket_write_fixed_zc(&socket, &b, on_cancel)
                .await
                .unwrap();
            assert_eq!(n, expect_len);

            pool.release(b);
        }

        // All slots came back to the pool.
        assert_eq!(pool.capacity(), 3);
        for _ in 0..3 {
            let _ = pool.acquire().await;
        }

        drop(socket);
    });

    let received = server.join().unwrap();
    let s = String::from_utf8(received).unwrap();
    let expected: String = (0..6u32).map(|i| format!("zc-{i:02} ")).collect();
    assert_eq!(s, expected);
}
