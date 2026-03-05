// Must be defined before any `mod` declarations so all child modules see it.
macro_rules! proto_err {
    (conn: $($msg:tt)+) => {
        drop(format_args!($($msg)+))
    };
    (stream: $($msg:tt)+) => {
        drop(format_args!($($msg)+))
    };
}

pub mod client;
pub mod codec;
pub mod error;
pub mod ext;
pub mod frame;
pub mod hpack;
pub mod io;
pub mod proto;
pub mod server;
pub mod share;
pub mod workflow;

pub use error::{Error, Reason};
pub(crate) use proto::poll_fn;
pub use share::StreamId;
