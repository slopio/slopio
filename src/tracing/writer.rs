use std::cell::UnsafeCell;
use std::future::Future;
use std::mem;
use std::pin::pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

use opentelemetry_sdk::trace::{SpanData, SpanExporter as _};
use opentelemetry_stdout::SpanExporter;

use crate::tracing::TraceWriter;

/// Per-thread collector that accumulates ended spans for batch export.
pub struct TraceCollector {
    spans: UnsafeCell<Vec<SpanData>>,
    span_exporter: SpanExporter,
}

impl TraceCollector {
    pub fn init() -> Self {
        Self {
            spans: UnsafeCell::new(Vec::new()),
            span_exporter: SpanExporter::default(),
        }
    }

    pub(super) fn push_ended_span(&self, span: SpanData) {
        unsafe { &mut *self.spans.get() }.push(span);
    }

    /// Drain all collected spans, returning them as a batch ready for export.
    pub fn drain(&self) -> Vec<SpanData> {
        let spans = unsafe { &mut *self.spans.get() };
        std::mem::take(spans)
    }
}

impl TraceWriter for TraceCollector {
    fn write_span(&self, span: SpanData) {
        self.push_ended_span(span);
    }

    async fn flush(&self) {
        let spans = unsafe { &mut *self.spans.get() };
        let mut new = Vec::new();
        mem::swap(&mut new, spans);
        self.span_exporter.export(new).await.unwrap();
    }
}

impl Drop for TraceCollector {
    fn drop(&mut self) {}
}
