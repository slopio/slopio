use crate::tracing::task::with_current_task_tracing;
use crate::tracing::types::{SpanId, TraceId};

/// Serialize the current span context as a W3C `traceparent` header value.
pub fn inject() -> Option<String> {
    with_current_task_tracing(|tracing| {
        tracing.spans.back().map(|buf| {
            let trace_id = u128::from_be_bytes(buf.data.trace_id.to_bytes());
            let span_id = u64::from_be_bytes(buf.data.span_id.to_bytes());
            format!("00-{:032x}-{:016x}-01", trace_id, span_id)
        })
    })
    .flatten()
}

/// Parse a W3C `traceparent` header value into `(TraceId, SpanId)`.
pub fn extract(traceparent: &str) -> Option<(TraceId, SpanId)> {
    let mut parts = traceparent.split('-');
    if parts.next()? != "00" {
        return None;
    }
    let trace_id = u128::from_str_radix(parts.next()?, 16).ok()?;
    let span_id = u64::from_str_radix(parts.next()?, 16).ok()?;
    Some((TraceId::from(trace_id), SpanId::from(span_id)))
}
