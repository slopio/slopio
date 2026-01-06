use crate::io_driver::operations::IoOP;

struct Write<'data> {
    buffer: &'data [u8],
}
