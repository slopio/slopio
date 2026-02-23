use std::time::SystemTime;

use opentelemetry::trace::{SpanContext, SpanKind, Status, TraceState};
use opentelemetry::{InstrumentationScope, SpanId, TraceFlags, TraceId};
use opentelemetry_sdk::trace::{SpanData, SpanEvents, SpanLinks};
use rand::Rng;

use crate::runtime::TIME_DRIVER;

pub fn random_trace_id() -> TraceId {
    TraceId::from(rand::thread_rng().r#gen::<u128>())
}

pub fn new_span_data(
    trace_id: TraceId,
    parent_span_id: Option<SpanId>,
    name: &'static str,
) -> SpanData {
    let span_id: SpanId = rand::thread_rng().r#gen::<u64>().into();
    let span_context = SpanContext::new(
        trace_id,
        span_id,
        TraceFlags::SAMPLED,
        false,
        TraceState::NONE,
    );
    let time = TIME_DRIVER.with(|d| d.now());
    SpanData {
        span_context,
        parent_span_id: parent_span_id.unwrap_or(SpanId::INVALID),
        parent_span_is_remote: false,
        span_kind: SpanKind::Internal,
        name: name.into(),
        start_time: time,
        end_time: time,
        attributes: Vec::new(),
        dropped_attributes_count: 0,
        events: SpanEvents::default(),
        links: SpanLinks::default(),
        status: Status::Unset,
        instrumentation_scope: InstrumentationScope::builder("my runtime").build(),
    }
}
