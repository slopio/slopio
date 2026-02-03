use io_uring_rb::buffer::Buffer;

use crate::io_driver::BUFFER_SIZE;

trait ABuffer<const SIZE: u32>: AsRef<[u8]> + AsMut<[u8]> {
    fn available_bytes(&self) -> usize {
        SIZE as usize - Self::as_ref(self).len()
    }
    fn total_size(&self) -> u32 {
        SIZE
    }
}
//todo

impl ABuffer<BUFFER_SIZE> for Buffer<BUFFER_SIZE> {}
