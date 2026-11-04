//! The reserve hands back the buffer released last, not the one that has
//! waited longest: that one is still in cache, and a pool that rotates has the
//! whole pool for a working set.

use slopio_buf_ring::RingBuffer;

type Ring = RingBuffer<4096, 2, 8>;

/// Consume `n` buffers the way a kernel would, and say so.
fn consume(ring: &Ring, n: usize) -> Vec<u16> {
    (0..n)
        .map(|_| {
            let (bid, _) = ring.take().expect("a published buffer");
            ring.note_consumed();
            bid
        })
        .collect()
}

#[test]
fn the_last_buffer_released_is_the_first_back_in_the_ring() {
    let ring = Ring::new_unregistered(0).unwrap();

    // Hold the ring's own two, and let the reserve stand in for them.
    assert_eq!(consume(&ring, 2), [0, 1]);
    assert_eq!(ring.refill(), 2);

    // The ring is full again, so both go to the reserve: 0, then 1 on top.
    ring.recycle_bid(0);
    ring.recycle_bid(1);
    assert_eq!(ring.reserved(), 6);

    assert_eq!(consume(&ring, 2), [2, 3]);
    assert_eq!(ring.refill(), 2);

    // 1 was released last, so it is first in; the untouched 4..8 stay put.
    assert_eq!(consume(&ring, 2), [1, 0]);
    assert_eq!(ring.reserved(), 4);
}
