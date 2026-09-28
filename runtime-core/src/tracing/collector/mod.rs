pub mod console;
#[cfg(feature = "local_debug_collector")]
pub mod local_debug_collector;

use crate::tracing::span::SpanWrapper;

///the collector receives dropped spans through a channel and processses them
/// there is one collector per worker thread, built on that thread
pub trait Collector: Sized {
    fn process(&mut self, batch: Vec<SpanWrapper>) -> impl Future<Output = ()>;
}
