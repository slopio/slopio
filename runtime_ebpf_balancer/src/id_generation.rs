pub struct ThreadAwareCidGenerator {
    thread_index: u8,
    cid_len: usize,
    counter: u32,
}

impl ThreadAwareCidGenerator {
    pub fn new(thread_index: u8, cid_len: usize) -> Self {
        Self {
            thread_index,
            cid_len,
            counter: 0,
        }
    }

    pub fn cid_len(&self) -> usize {
        self.cid_len
    }

    pub fn generate(&mut self, buf: &mut [u8], random: &[u8]) {
        buf[0] = self.thread_index;
        let counter_bytes = self.counter.to_le_bytes();
        buf[1..4].copy_from_slice(&counter_bytes[..3]);
        self.counter = self.counter.wrapping_add(1);
        buf[4..self.cid_len].copy_from_slice(&random[..self.cid_len - 4]);
    }
}
