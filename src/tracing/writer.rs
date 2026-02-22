use opentelemetry_sdk::trace::{SpanData, SpanExporter as _};
use opentelemetry_stdout::SpanExporter;

pub trait SpanProcessor: Sized {
    fn init() -> Self;
    fn process(&self, batch: Vec<SpanData>) -> impl Future<Output = ()>;
}

pub struct TraceCollector {
    span_exporter: SpanExporter,
}

impl SpanProcessor for TraceCollector {
    fn init() -> Self {
        Self {
            span_exporter: SpanExporter::default(),
        }
    }

    async fn process(&self, batch: Vec<SpanData>) {
        let _ = self.span_exporter.export(batch).await;
    }
}
