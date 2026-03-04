// Must be defined before any `mod` declarations so all child modules see it.
macro_rules! proto_err {
    (conn: $($msg:tt)+) => {
        drop(format_args!($($msg)+))
    };
    (stream: $($msg:tt)+) => {
        drop(format_args!($($msg)+))
    };
}

pub mod codec;
pub mod error;
pub mod ext;
pub mod frame;
pub mod hpack;
pub mod io;
pub mod proto;
pub mod server;
pub mod share;

pub use error::{Error, Reason};
pub use share::StreamId;
pub(crate) use proto::poll_fn;
