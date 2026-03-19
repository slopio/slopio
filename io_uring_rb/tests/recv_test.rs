use core::panic;
use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::thread::{self, sleep};
use std::time::Duration;

use io_uring::opcode;
use io_uring::types::Fd;
use io_uring_rb::RingBuffer;

use crate::tools::RandomChunkIterator;

#[test]
fn test_recv_with_buffer_ring() {
    let mut random_bytes = vec![0u8; 100_000_000];
    for (i, chunk) in random_bytes.chunks_mut(4).enumerate() {
        let idx = (i as u32).to_le_bytes();
        chunk.copy_from_slice(&idx);
    }
    let random_bytes_clone = random_bytes.clone();

    const BUFFER_SIZE: u32 = 1024;
    const SIZE: u16 = 1024;
    const BGID: u16 = 0;

    let mut ring = io_uring::IoUring::new(64).unwrap();

    let br = RingBuffer::<BUFFER_SIZE, SIZE>::new(&ring, 0, BGID).unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let handle = thread::spawn(move || {
        let mut client = TcpStream::connect(addr).unwrap();
        let it = RandomChunkIterator::new(&random_bytes, 1, 4096);
        for chunk in it {
            client.write_all(chunk).unwrap();
        }
    });
    let mut received = Vec::<u8>::new();
    let (server, _) = listener.accept().unwrap();
    server.set_nonblocking(true).unwrap();
    sleep(Duration::from_millis(200));
    let recv_entry = opcode::RecvMulti::new(Fd(server.as_raw_fd()), 0)
        .build()
        .user_data(0x42);
    unsafe {
        ring.submission().push(&recv_entry).unwrap();
    }
    ring.submit().unwrap();
    let mut need_resubmit = false;
    'outer: loop {
        if need_resubmit {
            unsafe {
                ring.submission().push(&recv_entry).unwrap();
            }
            need_resubmit = false;
        }
        ring.submit_and_wait(1).unwrap();

        // Collect CQEs first to avoid borrow issues
        let cqes: Vec<_> = ring
            .completion()
            .map(|cqe| (cqe.result(), cqe.flags()))
            .collect();

        for (bytes_read, flags) in cqes {
            match bytes_read {
                n if n > 0 => {
                    let buffer_id = (flags >> 16) as u16;
                    let buf_more = (flags & 0x08) != 0; // IORING_CQE_F_BUF_MORE
                    let buffer = br.get_buffer(buffer_id, n as _).unwrap();
                    received.extend(buffer.as_ref());
                    br.recycle_buffer(&buffer);
                    if !buf_more {
                        need_resubmit = true;
                    }
                }
                0 => break 'outer,
                -105 => {
                    need_resubmit = true;
                }
                e => panic!("{}", e),
            }
        }
        ring.submit().unwrap();
    }

    handle.join().unwrap();
    assert_eq!(random_bytes_clone.len(), received.len());
    assert_eq!(random_bytes_clone, received);
}

pub mod tools {

    use rand::Rng;
    use rand::rngs::ThreadRng;

    pub struct RandomChunkIterator<'a> {
        data: &'a [u8],
        rng: ThreadRng,
        min_chunk: usize,
        max_chunk: usize,
        current_pos: usize,
    }

    impl<'a> RandomChunkIterator<'a> {
        pub fn new(data: &'a [u8], min_chunk: usize, max_chunk: usize) -> Self {
            Self {
                data,
                rng: rand::rng(),
                min_chunk,
                max_chunk,
                current_pos: 0,
            }
        }
    }

    impl<'a> Iterator for RandomChunkIterator<'a> {
        type Item = &'a [u8];

        fn next(&mut self) -> Option<Self::Item> {
            // If we've reached the end, stop iterating
            if self.current_pos >= self.data.len() {
                return None;
            }

            let chunk_size = self.rng.random_range(self.min_chunk..=self.max_chunk);
            let end = (self.current_pos + chunk_size).min(self.data.len());
            let chunk = &self.data[self.current_pos..end];
            self.current_pos = end;

            Some(chunk)
        }
    }
}
