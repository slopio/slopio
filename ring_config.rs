// Buffer ring configuration for this application.
//
// Each entry defines a named ring instance with:
//   - bgid:        buffer group ID passed to io_uring (must be unique)
//   - buffer_size: size of each buffer in bytes
//   - ring_size:   number of buffers in the ring
//
// Multiple rings of the same buffer_size are allowed (use distinct names and bgids).
// The generated `BufGroup` enum and `BorrowedBuffer` enum will have one variant per entry.
//
// Example with two rings:
//
//   define_buffer_rings! {
//       Small { bgid: 0, buffer_size: 4096,  ring_size: 1024 }
//       Large { bgid: 1, buffer_size: 65536, ring_size: 64   }
//   }

define_buffer_rings! {
    Small  { bgid: 0, buffer_size: 4096,  ring_size: 8192 }
    Medium { bgid: 1, buffer_size: 16384, ring_size: 2048 }
}
