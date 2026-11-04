// Two independent timer stores, sharing nothing: the hierarchical wheel that
// takes any duration, and the per-duration deadline queues.
mod fixed;
mod wheel;

pub use fixed::{Timers, Token};
pub use wheel::{DurationTooLong, MAX_DURATION, TimeWheel};
