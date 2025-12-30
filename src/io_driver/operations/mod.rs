use io_uring::squeue::Entry;

mod open;
pub use open::Open;

pub trait Op {
    fn into_entry(self) -> Entry;
}

//opcode -> entry -> added_to_driver
