use io_uring::squeue::Entry;

pub mod open;

pub(crate) type IoDriverToken = u64;

trait IoOP {
    fn register_to_driver(&mut self) -> IoDriverToken;

    fn to_entry(&self) -> Entry;
}
