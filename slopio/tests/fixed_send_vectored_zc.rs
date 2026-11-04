use std::io::{Read, Write};
use std::net::TcpListener;
use std::rc::Rc;

use slopio::net::{
    FixedSendBufPool, connect_tcp, send_vectored_fixed_all_zc, socket_send_vectored_fixed_zc,
};
use slopio::runtime::main_thread;

// DANGER, io-uring upgrades: `SendMsgZc` has no `buf_index` setter, so the
// fixed gather send depends on the crate's SQE *encoding* — `build()` leaving
// `buf_index` zeroed, `ioprio` arriving intact. If this fails after a bump,
// the crate now writes `buf_index`/`buf_group` or repurposes `ioprio`, and the
// send would read from the wrong registered region: pin the old version, or
// add a `buf_index` setter upstream and set it to SLAB_BUF_INDEX.
#[test]
fn sendmsg_zc_sqe_encoding() {
    use io_uring::{opcode, squeue::Entry, types::Fd};

    // `struct io_uring_sqe` offsets, from offsetof against linux/io_uring.h.
    const IORING_RECVSEND_FIXED_BUF: u16 = 1 << 2;
    const OFF_IOPRIO: usize = 2;
    const OFF_BUF_INDEX: usize = 40;

    assert_eq!(
        std::mem::size_of::<Entry>(),
        64,
        "SQE is no longer 64 bytes; the offsets below are stale"
    );

    // Check the offsets against an opcode that does expose buf_index, so a
    // layout shift can't leave the real assertions reading the wrong bytes.
    let probe = opcode::SendZc::new(Fd(3), std::ptr::null(), 0)
        .buf_index(Some(7))
        .build();
    // SAFETY: `Entry` is #[repr(C)] over the #[repr(C)] `io_uring_sqe`.
    let probe: [u8; 64] = unsafe { std::mem::transmute(probe) };
    assert_eq!(
        u16::from_ne_bytes([probe[OFF_BUF_INDEX], probe[OFF_BUF_INDEX + 1]]),
        7,
        "buf_index is not at offset {OFF_BUF_INDEX}"
    );
    assert_eq!(
        u16::from_ne_bytes([probe[OFF_IOPRIO], probe[OFF_IOPRIO + 1]]),
        IORING_RECVSEND_FIXED_BUF,
        "ioprio is not at offset {OFF_IOPRIO}, or the flag value changed"
    );

    let msghdr: libc::msghdr = unsafe { std::mem::zeroed() };
    let entry = opcode::SendMsgZc::new(Fd(3), &msghdr)
        .ioprio(IORING_RECVSEND_FIXED_BUF)
        .flags(libc::MSG_WAITALL as u32)
        .build();

    // SAFETY: `Entry` is #[repr(C)] over the #[repr(C)] `io_uring_sqe`.
    let raw: [u8; 64] = unsafe { std::mem::transmute(entry) };

    assert_eq!(
        raw[0],
        opcode::SendMsgZc::CODE,
        "opcode is not at offset 0; SQE layout changed"
    );
    assert_eq!(
        u16::from_ne_bytes([raw[OFF_IOPRIO], raw[OFF_IOPRIO + 1]]),
        IORING_RECVSEND_FIXED_BUF,
        "ioprio no longer reaches the SQE carrying only our flag"
    );
    assert_eq!(
        u16::from_ne_bytes([raw[OFF_BUF_INDEX], raw[OFF_BUF_INDEX + 1]]),
        slopio::net::SLAB_BUF_INDEX.get(),
        "SendMsgZc now writes buf_index/buf_group; the send would use the wrong region"
    );
}

fn serve(listener: TcpListener) -> std::thread::JoinHandle<Vec<u8>> {
    std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut buf = Vec::new();
        stream.read_to_end(&mut buf).unwrap();
        buf
    })
}

// Gather across slots of one pool. Fails with EFAULT under a per-slot
// registration, so this is what pins the slab to a single region.
#[test]
fn gather_across_slots_of_one_pool() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = serve(listener);

    main_thread(async move {
        let pool = Rc::new(FixedSendBufPool::new(4, 64).unwrap());
        let socket = connect_tcp(addr).await.unwrap();

        let mut bufs = Vec::new();
        for i in 0..3u32 {
            let mut b = pool.try_acquire().expect("slot free");
            write!(b, "[chunk-{i}]").unwrap();
            bufs.push(b);
        }
        let expected: usize = bufs.iter().map(|b| b.len()).sum();

        let (result, bufs) = socket_send_vectored_fixed_zc(&socket, bufs).await;
        assert_eq!(
            result.unwrap(),
            expected,
            "gather across slots must not be rejected"
        );

        drop(bufs); // slots return to the pool
        let all: Vec<_> = (0..4)
            .map(|_| pool.try_acquire().expect("slot free"))
            .collect();
        assert!(pool.try_acquire().is_none(), "pool handed out a fifth slot");
        drop(all);
        drop(socket);
    });

    let s = String::from_utf8(server.join().unwrap()).unwrap();
    assert_eq!(s, "[chunk-0][chunk-1][chunk-2]");
}

// A zero-length iovec fails the whole registered-vector request with EFAULT,
// so empty slots must be dropped from the submission rather than passed along.
#[test]
fn empty_slot_in_the_middle_is_skipped() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = serve(listener);

    main_thread(async move {
        let pool = Rc::new(FixedSendBufPool::new(3, 64).unwrap());
        let socket = connect_tcp(addr).await.unwrap();

        let mut head = pool.try_acquire().expect("slot free");
        head.write_all(b"head").unwrap();
        let empty = pool.try_acquire().expect("slot free"); // never written to
        let mut tail = pool.try_acquire().expect("slot free");
        tail.write_all(b"tail").unwrap();

        let (result, bufs) = socket_send_vectored_fixed_zc(&socket, vec![head, empty, tail]).await;
        assert_eq!(result.unwrap(), 8);

        drop(bufs);
        drop(socket);
    });

    assert_eq!(server.join().unwrap(), b"headtail");
}

// The `_all` loop drives the whole payload and hands every slot back.
#[test]
fn send_all_covers_every_slot() {
    const SLOTS: u16 = 8;
    const SIZE: u32 = 4096;

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let server = serve(listener);

    main_thread(async move {
        let pool = Rc::new(FixedSendBufPool::new(SLOTS, SIZE).unwrap());
        let socket = connect_tcp(addr).await.unwrap();

        let mut bufs = Vec::new();
        for i in 0..SLOTS {
            let mut b = pool.try_acquire().expect("slot free");
            b.write_all(&vec![b'a' + i as u8; SIZE as usize]).unwrap();
            bufs.push(b);
        }

        let (result, bufs) = send_vectored_fixed_all_zc(&socket, bufs).await;
        result.unwrap();
        assert_eq!(bufs.len(), SLOTS as usize);

        drop(bufs);
        drop(socket);
    });

    let received = server.join().unwrap();
    assert_eq!(received.len(), SLOTS as usize * SIZE as usize);
    for i in 0..SLOTS as usize {
        let start = i * SIZE as usize;
        let chunk = &received[start..start + SIZE as usize];
        assert!(
            chunk.iter().all(|&c| c == b'a' + i as u8),
            "chunk {i} is not contiguous or out of order"
        );
    }
}
