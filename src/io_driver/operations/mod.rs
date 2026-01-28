use io_uring::squeue::Entry;

pub mod files;
pub mod io_traits;
pub mod sockets;

pub(crate) type IoDriverToken = u64;

#[allow(drop_bounds)]
trait IoOP: Drop {
    fn register_to_driver(&mut self) -> IoDriverToken;

    fn driver_cleanup(&mut self);

    fn to_entry(&self) -> Entry;
}
