use io_uring::squeue::Entry;

pub(crate) mod files;

pub(crate) type IoDriverToken = u64;

trait IoOP: Drop {
    fn register_to_driver(&mut self) -> IoDriverToken;

    fn free_driver(&mut self);

    fn to_entry(&self) -> Entry;
}
