use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::rc::Rc;
use std::time::Duration;

use runtime_core::net::{
    FixedSendBufPool, PooledBuf, SocketWrapper, connect_tcp, socket_write_fixed,
};
use runtime_core::runtime::main_thread;
use runtime_core::runtime::time::timeout;

#[test]
fn fixed_send_pool_end_to_end() {
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
        assert_eq!(pool.capacity(), 3);
        assert_eq!(pool.buffer_size(), 64);

        let socket = connect_tcp(addr).await.unwrap();

        // --- 1. Concurrent acquire exercises every slot in the pool ---
        let round_one = send_round(&pool, &socket, 0).await;
        let set_one: HashSet<u16> = round_one.iter().copied().collect();
        assert_eq!(set_one.len(), 3, "round 1 indices not distinct: {round_one:?}");
        for &i in &round_one {
            assert!(i < 3, "round 1 index {i} out of range");
        }

        // --- 2. Recycling: a second round draws from the same slot set ---
        let round_two = send_round(&pool, &socket, 3).await;
        let set_two: HashSet<u16> = round_two.iter().copied().collect();
        assert_eq!(set_one, set_two, "round 2 didn't recycle the same slots");

        // --- 3. Cancel-safety: a dropped Acquire future must not strand
        //        the next live waiter. We hold all 3 slots, start a waiter
        //        that times out (dropping its Acquire while parked), then
        //        release one slot and confirm a fresh acquire still works.
        let held = [
            pool.acquire().await,
            pool.acquire().await,
            pool.acquire().await,
        ];

        let timed_out = timeout(Duration::from_millis(80), pool.acquire()).await;
        assert!(timed_out.is_err(), "acquire should have timed out on empty pool");

        let [a, b, c] = held;
        let a_idx = a.buf_index().get();
        pool.release(a);

        // Slot just freed should be the one a fresh acquire receives.
        let revived = pool.acquire().await;
        assert_eq!(revived.buf_index().get(), a_idx);
        pool.release(revived);
        pool.release(b);
        pool.release(c);

        // --- 4. Waiting acquire wakes when a slot is released ---
        let h0 = pool.acquire().await;
        let h1 = pool.acquire().await;
        let h2 = pool.acquire().await;
        let h2_idx = h2.buf_index().get();

        // race a parked acquire against a small sleep that triggers a release
        use runtime_core::runtime::select::{SelectResult, select};
        use runtime_core::runtime::time::Sleep;

        let releaser = {
            let pool = pool.clone();
            async move {
                Sleep::during(Duration::from_millis(20)).await;
                pool.release(h2);
            }
        };
        let acquirer = pool.acquire();
        match select(acquirer, releaser).await {
            SelectResult::First(_) => panic!("acquire resolved before the release fired"),
            SelectResult::Second(()) => {}
        }
        // After the release fires the parked acquire is gone (we dropped it
        // when select picked Second), but the slot is now free.
        let woken = pool.acquire().await;
        assert_eq!(woken.buf_index().get(), h2_idx);
        pool.release(woken);
        pool.release(h0);
        pool.release(h1);

        // Drop the socket → server-side read_to_end returns.
        drop(socket);
    });

    let received = server.join().unwrap();
    let s = String::from_utf8(received).unwrap();
    let expected: String = (0..6u32).map(|i| format!("msg-{i:02} ")).collect();
    assert_eq!(s, expected);
}

/// Acquire 3 slots concurrently, send each, release each. Returns the
/// indices used (in send order).
async fn send_round(
    pool: &Rc<FixedSendBufPool>,
    socket: &SocketWrapper,
    base: u32,
) -> Vec<u16> {
    let mut bufs: Vec<PooledBuf<'_>> = Vec::new();
    for i in 0..3u32 {
        let mut b = pool.acquire().await;
        write!(b, "msg-{:02} ", base + i).unwrap();
        bufs.push(b);
    }
    let mut indices = Vec::new();
    for buf in bufs.iter() {
        let idx = buf.buf_index();
        indices.push(idx.get());
        let on_cancel = {
            let pool = pool.clone();
            move || pool.release_index(idx)
        };
        let n = socket_write_fixed(socket, buf, on_cancel).await.unwrap();
        assert_eq!(n, buf.len());
    }
    for buf in bufs {
        pool.release(buf);
    }
    indices
}
