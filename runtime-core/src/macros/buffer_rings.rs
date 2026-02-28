/// Generates the types needed to support multiple io_uring provided-buffer rings.
///
/// Each entry defines a named ring instance with its buffer group ID, buffer size,
/// and ring capacity. Multiple instances may share the same buffer size.
///
/// # Example
///
/// ```rust,ignore
/// define_buffer_rings! {
///     SmallA { bgid: 0, buffer_size: 4096, ring_size: 1024 }
///     SmallB { bgid: 1, buffer_size: 4096, ring_size: 1024 }
///     Large  { bgid: 2, buffer_size: 65536, ring_size: 64 }
/// }
/// ```
///
/// # Generated items
///
/// - `BufGroup` enum — passed to IO operations to select a ring
/// - `BorrowedBuffer` enum — returned by IO operations, one variant per ring instance
/// - `MultiRing` struct — holds all ring buffers, used internally by `IODriver`
#[macro_export]
macro_rules! define_buffer_rings {
    (
        $($name:ident { bgid: $bgid:literal, buffer_size: $bsize:literal, ring_size: $rsize:literal })*
    ) => {
        /// Selects which provided-buffer ring to use for an IO operation.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum BufGroup {
            $($name,)*
        }

        impl BufGroup {
            /// Returns the io_uring buffer group ID (bgid) for this ring.
            pub fn bgid(self) -> u16 {
                match self {
                    $(BufGroup::$name => $bgid,)*
                }
            }
        }

        /// A buffer borrowed from one of the registered provided-buffer rings.
        ///
        /// Automatically recycled back to the correct ring on drop.
        #[allow(non_camel_case_types)]
        pub enum BorrowedBuffer {
            $($name(::io_uring_rb::buffer::Buffer<$bsize>),)*
        }

        impl ::std::convert::AsRef<[u8]> for BorrowedBuffer {
            fn as_ref(&self) -> &[u8] {
                match self {
                    $(BorrowedBuffer::$name(b) => b.as_ref(),)*
                }
            }
        }

        impl ::std::convert::AsMut<[u8]> for BorrowedBuffer {
            fn as_mut(&mut self) -> &mut [u8] {
                match self {
                    $(BorrowedBuffer::$name(b) => b.as_mut(),)*
                }
            }
        }

        impl ::std::ops::Drop for BorrowedBuffer {
            fn drop(&mut self) {
                $crate::io_driver::IO_DRIVER.with(|d| d.recycle_buffer(self))
            }
        }

        /// Holds all registered io_uring provided-buffer rings.
        #[allow(non_snake_case)]
        pub(crate) struct MultiRing {
            $($name: ::io_uring_rb::RingBuffer<$bsize, $rsize>,)*
        }

        #[allow(non_snake_case)]
        impl MultiRing {
            pub(crate) fn new(ring: &::io_uring::IoUring) -> Self {
                Self {
                    $($name: ::io_uring_rb::RingBuffer::new(ring, $bgid, 0).unwrap(),)*
                }
            }

            pub(crate) fn get_buffer(&self, bgid: u16, id: u16, len: usize) -> BorrowedBuffer {
                match bgid {
                    $($bgid => BorrowedBuffer::$name(self.$name.get_buffer(id, len).unwrap()),)*
                    _ => panic!("unknown buffer group id: {bgid}"),
                }
            }

            pub(crate) fn recycle_buffer(&self, buf: &BorrowedBuffer) {
                match buf {
                    $(BorrowedBuffer::$name(b) => self.$name.recycle_buffer(b),)*
                }
            }
        }
    };
}
