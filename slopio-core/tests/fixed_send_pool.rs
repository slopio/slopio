use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::rc::Rc;
use std::time::{Duration, Instant};

use slopio_core::net::{
    FixedSendBufPool, PooledBuf, SocketWrapper, connect_tcp, socket_write_fixed,
};
use slopio_core::runtime::main_thread;

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

        // --- 1. Holding several slots at once exercises the whole pool ---
        // The slab is one registered region, so `buf_index` is the same for
        // every slot; the slot pointer is what identifies one.
        let round_one = send_round(&pool, &socket, 0).await;
        let set_one: HashSet<*const u8> = round_one.iter().copied().collect();
        assert_eq!(
            set_one.len(),
            3,
            "round 1 slots not distinct: {round_one:?}"
        );

        // --- 2. Recycling: a second round draws from the same slot set ---
        let round_two = send_round(&pool, &socket, 3).await;
        let set_two: HashSet<*const u8> = round_two.iter().copied().collect();
        assert_eq!(set_one, set_two, "round 2 didn't recycle the same slots");

        // --- 3. A drained pool reports empty instead of waiting, and the slot
        //        released next is the one the following take receives (LIFO).
        //        The held buffers auto-release on drop. ---
        let held = [
            pool.try_acquire().expect("slot free"),
            pool.try_acquire().expect("slot free"),
            pool.try_acquire().expect("slot free"),
        ];
        assert!(
            pool.try_acquire().is_none(),
            "drained pool must report empty"
        );

        let [a, b, c] = held;
        let a_slot = a.ptr();
        drop(a); // returns the slot to the pool

        let revived = pool.try_acquire().expect("released slot is takeable again");
        assert_eq!(revived.ptr(), a_slot);
        drop(revived);
        drop(b);
        drop(c);

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
    use slopio_core::runtime::select::{SelectResult, select};
    use slopio_core::runtime::time::Sleep;

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
        let mut b = pool.try_acquire().expect("slot free");
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
        // CQE if it was in flight). The pool never waits, so poll it until the
        // cancel round trip lands; failure here means the slot was lost.
        let h = poll_until_free(&pool, Duration::from_secs(2))
            .await
            .expect("slot should return to the pool after cancel")
            .buf_index()
            .get();
        assert_eq!(h, 0);

        // Take it and hold it: a second take must report empty, the pool has
        // exactly one slot. If the cancel path had double-released, the
        // free-list would contain the slot twice and this would succeed.
        let held = pool.try_acquire().expect("slot free");
        assert!(
            pool.try_acquire().is_none(),
            "pool handed out the single slot twice — double free on cancel"
        );
        drop(held);

        // Registration is still intact: a normal send goes through.
        let mut fb = pool.try_acquire().expect("slot free");
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

/// Waiting for a slot is the caller's business now: poll the pool until one
/// frees up or the deadline passes.
async fn poll_until_free(pool: &FixedSendBufPool, within: Duration) -> Option<PooledBuf> {
    use slopio_core::runtime::time::Sleep;

    let deadline = Instant::now() + within;
    loop {
        if let Some(buf) = pool.try_acquire() {
            return Some(buf);
        }
        if Instant::now() >= deadline {
            return None;
        }
        Sleep::during(Duration::from_millis(5)).await;
    }
}

/// Take 3 slots concurrently, send each, let each release on drop. Returns
/// the slot pointers used (in send order).
async fn send_round(
    pool: &Rc<FixedSendBufPool>,
    socket: &SocketWrapper,
    base: u32,
) -> Vec<*const u8> {
    let mut bufs: Vec<PooledBuf> = Vec::new();
    for i in 0..3u32 {
        let mut b = pool.try_acquire().expect("slot free");
        write!(b, "msg-{:02} ", base + i).unwrap();
        bufs.push(b);
    }
    let mut indices = Vec::new();
    for buf in bufs {
        let idx = buf.ptr();
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
        // One registered region for the slab, so slots differ by pointer.
        assert_ne!(a.ptr(), b.ptr());

        assert!(
            pool.try_acquire().is_none(),
            "drained pool must report empty, not park"
        );

        let a_slot = a.ptr();
        drop(a);
        let revived = pool.try_acquire().expect("released slot is takeable again");
        assert_eq!(revived.ptr(), a_slot);

        drop(revived);
        drop(b);
    });
}

// `available` is what a caller sizes a batch off, so it must track the live
// `PooledBuf`s exactly — including slots released by drop.
#[test]
fn available_tracks_live_buffers() {
    main_thread(async move {
        let pool = FixedSendBufPool::new(3, 64).unwrap();
        assert_eq!(pool.available(), 3);
        assert!(!pool.is_exhausted());

        let held: Vec<PooledBuf> = (0..pool.available())
            .map(|_| pool.try_acquire().expect("slot free"))
            .collect();
        assert_eq!(pool.available(), 0);
        assert!(pool.is_exhausted());
        assert!(pool.try_acquire().is_none());

        let mut held = held;
        held.pop();
        assert_eq!(pool.available(), 1, "drop must give the slot back");

        drop(held);
        assert_eq!(pool.available(), pool.capacity());
        assert!(!pool.is_exhausted());
    });
}

// A `PooledBuf` is an `IoBuf`, so a caller whose pool still has a slot but
// whose peer needs a plain send can hand it to the owned-buffer ops — and get
// the slot back through them. Covers the direct path, `send_all`, and
// `slice()`, and checks every slot round-trips.
#[test]
fn pooled_buf_goes_through_the_generic_send_path() {
    use slopio_core::net::{IoBuf, send_all, socket_send};

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).unwrap();
        buf
    });

    main_thread(async move {
        let pool = FixedSendBufPool::new(2, 64).unwrap();
        let socket = connect_tcp(addr).await.unwrap();

        let mut a = pool.try_acquire().expect("slot free");
        write!(a, "plain ").unwrap();
        assert_eq!(a.bytes_init(), 6, "IoBuf length is the written prefix");
        let (res, a) = socket_send(&socket, a).await;
        assert_eq!(res.unwrap(), 6);
        drop(a);

        let mut b = pool.try_acquire().expect("slot free");
        write!(b, "all ").unwrap();
        let (res, b) = send_all(&socket, b).await;
        res.unwrap();
        drop(b);

        // A slice of a slot is an IoBuf too; `into_inner` gives the slot back.
        let mut c = pool.try_acquire().expect("slot free");
        write!(c, "XXsliceXX").unwrap();
        let (res, slice) = socket_send(&socket, c.slice(2..7)).await;
        assert_eq!(res.unwrap(), 5);
        drop(slice.into_inner());

        assert_eq!(pool.available(), pool.capacity(), "slots must round-trip");
        drop(socket);
    });

    assert_eq!(server.join().unwrap(), b"plain all slice");
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

        let mut b = pool.try_acquire().expect("slot free");
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
        let mut b = pool.try_acquire().expect("slot free");
        b.advance(17);
    });
}
