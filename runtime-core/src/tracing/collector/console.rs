use super::Collector;
use crate::tracing::span::SpanWrapper;

pub struct ConsoleCollector;

impl Collector for ConsoleCollector {
    async fn process(&mut self, batch: Vec<SpanWrapper>) {
        for buf in &batch {
            let trace_id = u128::from_be_bytes(buf.data.trace_id.to_bytes());
            let span_id = u64::from_be_bytes(buf.data.span_id.to_bytes());
            eprintln!(
                "[trace] {:032x} {:016x} {}",
                trace_id, span_id, buf.data.name
            );
        }
    }
}
