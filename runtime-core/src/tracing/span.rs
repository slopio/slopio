use rand::Rng;

use crate::runtime::TIME_DRIVER;
use crate::tracing::types::{SpanData, SpanEvents, SpanId, SpanKind, Status, TraceId};

pub fn random_trace_id() -> TraceId {
    TraceId::from(rand::thread_rng().r#gen::<u128>())
}

pub fn new_span_data(
    trace_id: TraceId,
    parent_span_id: Option<SpanId>,
    name: &'static str,
    kind: SpanKind,
) -> SpanData {
    let span_id: SpanId = rand::thread_rng().r#gen::<u64>().into();
    let time = TIME_DRIVER.with(|d| d.now());
    SpanData {
        trace_id,
        span_id,
        parent_span_id: parent_span_id.unwrap_or(SpanId::INVALID),
        parent_span_is_remote: false,
        name,
        span_kind: kind,
        start_time: time,
        end_time: time,
        attributes: Vec::new(),
        dropped_attributes_count: 0,
        events: SpanEvents::default(),
        status: Status::Unset,
    }
}
