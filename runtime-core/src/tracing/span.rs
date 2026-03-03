use bumpalo::Bump;
use rand::Rng;
use smallvec::SmallVec;

use crate::runtime::TIME_DRIVER;
use crate::tracing::types::{
    KeyValue, SpanData, SpanEvent, SpanEvents, SpanId, SpanKind, SpanLink, Status, TraceId,
};

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
        attributes: SmallVec::new(),
        dropped_attributes_count: 0,
        events: SpanEvents::default(),
        links: SmallVec::new(),
        dropped_links_count: 0,
        status: Status::Unset,
    }
}

/// A span co-located with a bump arena for zero-alloc dynamic string attributes.
///
/// # Drop order
/// `data` is declared before `arena` — Rust drops fields in declaration order, so
/// `data` drops first. Arena-allocated strings in `data` are read only during proto
/// encoding, which happens while this `SpanBuf` is still alive.
pub struct SpanBuf {
    pub data: SpanData,
    pub arena: Bump,
}

impl SpanBuf {
    pub fn new(data: SpanData) -> Self {
        Self {
            data,
            arena: Bump::new(),
        }
    }

    fn alloc_str(&mut self, s: &str) -> &'static str {
        let s: &str = self.arena.alloc_str(s);
        // SAFETY: `s` lives in `self.arena` which is a field of this struct,
        // declared after `self.data`, so it outlives all references stored in data.
        // These `&'static str` values are only read while this `SpanBuf` is alive.
        unsafe { std::mem::transmute(s) }
    }

    /// Add a dynamic string attribute, allocated in the arena. Zero heap allocation.
    pub fn push_attr_dyn(&mut self, key: &'static str, value: &str) {
        let s = self.alloc_str(value);
        self.data.attributes.push(KeyValue::new(key, s));
    }

    /// Add an event with a dynamic name, allocated in the arena. Zero heap allocation.
    pub fn push_event_dyn(&mut self, message: &str) {
        let s = self.alloc_str(message);
        self.data.events.events.push(SpanEvent::new(s));
    }

    /// Set a dynamic error status, message allocated in the arena. Zero heap allocation.
    pub fn push_status_error_dyn(&mut self, message: &str) {
        let s = self.alloc_str(message);
        self.data.status = Status::Error(s);
    }

    /// Add a link to another span. Both IDs are `Copy` — no allocation.
    pub fn push_link(&mut self, link: SpanLink) {
        self.data.links.push(link);
    }
}
