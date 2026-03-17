pub mod console;
pub mod otlp;

use crate::tracing::span::SpanWrapper;

///the collector receives dropped spans through a channel and processses them
/// there is one collector per worker thread
pub trait Collector: Sized {
    fn init() -> Self;
    fn process(&mut self, batch: Vec<SpanWrapper>) -> impl Future<Output = ()>;
}
