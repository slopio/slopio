use io_uring::squeue::Entry;

pub trait Op {
    fn into_entry(self) -> Entry;
}

//opcode -> entry -> added_to_driver
