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

        // --- 3. Cancel-safety: a dropped Acquire future must not strand the
        //        next live waiter. We hold all 3 slots, start a waiter that
        //        times out (dropping its Acquire while parked), then drop one
        //        held buffer and confirm a fresh acquire still works. The held
        //        buffers auto-release on drop. ---
        let held = [
            pool.acquire().await,
            pool.acquire().await,
            pool.acquire().await,
        ];

        let timed_out = timeout(Duration::from_millis(80), pool.acquire()).await;
        assert!(timed_out.is_err(), "acquire should have timed out on empty pool");

        let [a, b, c] = held;
        let a_idx = a.buf_index().get();
        drop(a); // returns the slot to the pool

        // Slot just freed should be the one a fresh acquire receives.
        let revived = pool.acquire().await;
        assert_eq!(revived.buf_index().get(), a_idx);
        drop(revived);
        drop(b);
        drop(c);

        // --- 4. Waiting acquire wakes when a slot is released ---
        let h0 = pool.acquire().await;
        let h1 = pool.acquire().await;
        let h2 = pool.acquire().await;
        let h2_idx = h2.buf_index().get();

        // race a parked acquire against a small sleep that triggers a release
        use runtime_core::runtime::select::{SelectResult, select};
        use runtime_core::runtime::time::Sleep;

        let releaser = {
            async move {
                Sleep::during(Duration::from_millis(20)).await;
                drop(h2); // releasing the slot wakes the parked acquire
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
        drop(woken);
        drop(h0);
        drop(h1);

        // Drop the socket → server-side read_to_end returns.
        drop(socket);
    });

    let received = server.join().unwrap();
    let s = String::from_utf8(received).unwrap();
    let expected: String = (0..6u32).map(|i| format!("msg-{i:02} ")).collect();
    assert_eq!(s, expected);
}

// Dropping a fixed send mid-flight must return its slot to the pool exactly
// once — not early (kernel may still be reading) and not twice (no double
// free). With a single-slot pool the only way a later acquire can succeed is
// if the cancelled send's buffer was correctly released after its CQE; a
// double free would instead hand the same slot out twice.
#[test]
fn fixed_send_cancel_returns_slot_once() {
    use runtime_core::runtime::select::{SelectResult, select};
    use runtime_core::runtime::time::Sleep;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).unwrap();
        buf
    });

    main_thread(async move {
        let pool = Rc::new(FixedSendBufPool::new(1, 64).unwrap());
        let socket = connect_tcp(addr).await.unwrap();

        // Acquire the only slot and start a send, then race it against an
        // immediate timer so the send future is dropped (cancelled) — whether
        // or not it managed to complete first, the slot must round-trip back.
        let mut b = pool.acquire().await;
        write!(b, "cancelled").unwrap();
        let send = socket_write_fixed(&socket, b);
        match select(send, Sleep::during(Duration::from_nanos(1))).await {
            SelectResult::First((res, buf)) => {
                res.unwrap();
                drop(buf);
            }
            SelectResult::Second(()) => { /* send dropped mid-flight */ }
        }

        // The slot must come back (immediately if completed, after the cancel
        // CQE if it was in flight). A long timeout tolerates the cancel round
        // trip; failure here means the slot was lost.
        let h = timeout(Duration::from_secs(2), pool.acquire())
            .await
            .expect("slot should return to the pool after cancel")
            .buf_index()
            .get();
        assert_eq!(h, 0);

        // Re-acquire and hold it, then a second acquire must time out: the pool
        // has exactly one slot. If the cancel path had double-released, the
        // free-list would contain the slot twice and this would succeed.
        let held = pool.acquire().await;
        let dup = timeout(Duration::from_millis(80), pool.acquire()).await;
        assert!(
            dup.is_err(),
            "pool handed out the single slot twice — double free on cancel"
        );
        drop(held);

        // Registration is still intact: a normal send goes through.
        let mut fb = pool.acquire().await;
        write!(fb, "final").unwrap();
        let (res, fb) = socket_write_fixed(&socket, fb).await;
        assert_eq!(res.unwrap(), 5);
        drop(fb);

        drop(socket);
    });

    let received = server.join().unwrap();
    assert!(
        received.ends_with(b"final"),
        "final message missing; got {received:?}"
    );
}

/// Acquire 3 slots concurrently, send each, let each release on drop. Returns
/// the indices used (in send order).
async fn send_round(
    pool: &Rc<FixedSendBufPool>,
    socket: &SocketWrapper,
    base: u32,
) -> Vec<u16> {
    let mut bufs: Vec<PooledBuf> = Vec::new();
    for i in 0..3u32 {
        let mut b = pool.acquire().await;
        write!(b, "msg-{:02} ", base + i).unwrap();
        bufs.push(b);
    }
    let mut indices = Vec::new();
    for buf in bufs {
        let idx = buf.buf_index().get();
        let expect_len = buf.len();
        indices.push(idx);
        let (result, buf) = socket_write_fixed(socket, buf).await;
        assert_eq!(result.unwrap(), expect_len);
        drop(buf); // slot returns to the pool
    }
    indices
}

// `try_acquire` is the non-parking door into the pool: callers that share it
// across connections need to degrade to another path rather than block
// everyone behind one stalled peer.
#[test]
fn try_acquire_never_parks() {
    main_thread(async move {
        let pool = FixedSendBufPool::new(2, 64).unwrap();

        let a = pool.try_acquire().expect("slot free");
        let b = pool.try_acquire().expect("slot free");
        assert_ne!(a.buf_index().get(), b.buf_index().get());

        assert!(
            pool.try_acquire().is_none(),
            "drained pool must report empty, not park"
        );

        let a_idx = a.buf_index().get();
        drop(a);
        let revived = pool.try_acquire().expect("released slot is takeable again");
        assert_eq!(revived.buf_index().get(), a_idx);

        drop(revived);
        drop(b);
    });
}

// `advance` exists so a caller can fill a slot in place and only then say how
// far it got — here the pattern the HTTP/2 body writer needs: payload written
// first, fixed-size header stamped in front once its length is known, both
// committed in one go. The bytes must reach the wire in that order.
#[test]
fn advance_commits_a_header_written_after_its_payload() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).unwrap();
        buf
    });

    main_thread(async move {
        let pool = FixedSendBufPool::new(1, 64).unwrap();
        let socket = connect_tcp(addr).await.unwrap();

        let mut b = pool.acquire().await;
        const HDR: usize = 9;
        let payload = b"world";
        {
            let dst = b.remaining_mut();
            // Payload after the reserved header, header in front afterwards.
            dst[HDR..HDR + payload.len()].copy_from_slice(payload);
            dst[..HDR].copy_from_slice(b"HDR:0005:");
        }
        assert_eq!(b.len(), 0, "nothing committed before advance");
        b.advance(HDR + payload.len());
        assert_eq!(b.len(), HDR + payload.len());
        assert_eq!(b.as_slice(), b"HDR:0005:world");

        let (res, b) = socket_write_fixed(&socket, b).await;
        assert_eq!(res.unwrap(), HDR + payload.len());
        drop(b);
        drop(socket);
    });

    assert_eq!(server.join().unwrap(), b"HDR:0005:world");
}

// Committing past the slot would hand the kernel a length outside the
// registered buffer.
#[test]
#[should_panic(expected = "advance")]
fn advance_past_the_slot_panics() {
    main_thread(async move {
        let pool = FixedSendBufPool::new(1, 16).unwrap();
        let mut b = pool.acquire().await;
        b.advance(17);
    });
}
