use bumpalo::Bump;
use rand::Rng;
use smallvec::SmallVec;

use crate::runtime::TIME_DRIVER;
use crate::tracing::task::{Level, level_enabled, with_current_task_tracing};
use crate::tracing::types::{
    KeyValue, SpanData, SpanEvent, SpanEvents, SpanId, SpanKind, SpanLink, Status, TraceId,
};

// ─── Constructors ─────────────────────────────────────────────────────────────

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

// ─── SpanWrapper ──────────────────────────────────────────────────────────────────

/// A span co-located with a bump arena for zero-alloc dynamic string attributes.
///
/// # Drop order
/// `data` is declared before `arena` — Rust drops fields in declaration order, so
/// `data` drops first. Arena-allocated strings in `data` are read only during proto
/// encoding, which happens while this `SpanBuf` is still alive.
pub struct SpanWrapper {
    pub data: SpanData,
    pub arena: Bump,
}

impl SpanWrapper {
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

    /// Add an event with a dynamic name and attributes. All strings (name + attribute values)
    /// are arena-allocated. Keys must be `&'static str`. Zero heap allocation.
    pub fn push_event_attrs_dyn(&mut self, message: &str, attrs: &[(&'static str, &str)]) {
        let name = self.alloc_str(message);
        let mut event = SpanEvent::new(name);
        for &(k, v) in attrs {
            let v = self.alloc_str(v);
            event.attributes.push(KeyValue::new(k, v));
        }
        self.data.events.events.push(event);
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

// ─── SpanGuard ────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct SpanGuard {
    span_id: SpanId,
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        let buf = with_current_task_tracing(|tracing| {
            if let Some(pos) = tracing
                .spans
                .iter()
                .rposition(|s| s.data.span_id == self.span_id)
            {
                tracing.spans.remove(pos)
            } else {
                None
            }
        })
        .flatten();

        if let Some(mut buf) = buf {
            buf.data.end_time = TIME_DRIVER.with(|d| d.now());
            crate::tracing::TRACING_SINK.with(|output| output.push_span(buf))
        }
    }
}

// ─── Span builder ─────────────────────────────────────────────────────────────

/// RAII guard for a span. Holds the span open until dropped.
/// Use the builder methods (`.kind()`, `.attr()`, `.attr_dyn()`, `.link()`) immediately after creation.
#[must_use = "Span must be held for the duration of the operation"]
#[derive(Debug)]
pub struct Span(Option<SpanGuard>);

impl Span {
    /// Set the [`SpanKind`] for this span.
    pub fn kind(self, kind: SpanKind) -> Self {
        if self.0.is_some() {
            with_current_task_tracing(|tracing| {
                if let Some(buf) = tracing.spans.back_mut() {
                    buf.data.span_kind = kind;
                }
            });
        }
        self
    }

    /// Add a static-string attribute. Zero allocation.
    pub fn attr(self, kv: KeyValue) -> Self {
        if self.0.is_some() {
            with_current_task_tracing(|tracing| {
                if let Some(buf) = tracing.spans.back_mut() {
                    buf.data.attributes.push(kv);
                }
            });
        }
        self
    }

    /// Add a dynamic string attribute, allocated in the span's arena. Zero heap allocation.
    pub fn attr_dyn(self, key: &'static str, value: &str) -> Self {
        if self.0.is_some() {
            with_current_task_tracing(|tracing| {
                if let Some(buf) = tracing.spans.back_mut() {
                    buf.push_attr_dyn(key, value);
                }
            });
        }
        self
    }

    /// Add a link to another span (e.g. extracted from an incoming `traceparent`).
    pub fn link(self, link: SpanLink) -> Self {
        if self.0.is_some() {
            with_current_task_tracing(|tracing| {
                if let Some(buf) = tracing.spans.back_mut() {
                    buf.push_link(link);
                }
            });
        }
        self
    }

    /// Add a link if present. No-op when `None`.
    pub fn link_opt(self, link: Option<SpanLink>) -> Self {
        if let Some(link) = link {
            self.link(link)
        } else {
            self
        }
    }
}

// ─── Span context queries ─────────────────────────────────────────────────────

pub fn current_trace_id() -> Option<TraceId> {
    with_current_task_tracing(|tracing| tracing.spans.back().map(|s| s.data.trace_id)).flatten()
}

pub fn current_span_id() -> Option<SpanId> {
    with_current_task_tracing(|tracing| tracing.spans.back().map(|s| s.data.span_id)).flatten()
}

// ─── Span constructors ────────────────────────────────────────────────────────

pub fn new_span(name: &'static str, level: Level) -> Span {
    if !level_enabled(level) {
        return Span(None);
    }
    Span(with_current_task_tracing(|tracing| SpanGuard {
        span_id: tracing.create_new_span(name, false, level),
    }))
}

pub fn new_trace(name: &'static str, level: Level) -> Span {
    if !level_enabled(level) {
        return Span(None);
    }
    Span(with_current_task_tracing(|tracing| SpanGuard {
        span_id: tracing.create_new_span(name, true, level),
    }))
}

/// Start a new trace rooted at a remote parent (extracted from e.g. a `traceparent` header).
pub fn new_trace_with_parent(name: &'static str, level: Level, parent: (TraceId, SpanId)) -> Span {
    if !level_enabled(level) {
        return Span(None);
    }
    Span(with_current_task_tracing(|tracing| SpanGuard {
        span_id: tracing.create_span_with_remote_parent(name, level, parent),
    }))
}

// ─── Span mutations ───────────────────────────────────────────────────────────

pub fn event(message: &str) {
    with_current_task_tracing(|tracing| {
        if let Some(buf) = tracing.spans.back_mut() {
            buf.push_event_dyn(message);
        }
    });
}

/// Add an event with attributes to the current span.
/// All strings (name + attribute values) are arena-allocated. Keys must be `&'static str`.
pub fn event_attrs(message: &str, attrs: &[(&'static str, &str)]) {
    with_current_task_tracing(|tracing| {
        if let Some(buf) = tracing.spans.back_mut() {
            buf.push_event_attrs_dyn(message, attrs);
        }
    });
}

pub fn span_set_attribute(kv: KeyValue) {
    with_current_task_tracing(|tracing| {
        if let Some(buf) = tracing.spans.back_mut() {
            buf.data.attributes.push(kv);
        }
    });
}

pub fn span_set_status(status: Status) {
    with_current_task_tracing(|tracing| {
        if let Some(buf) = tracing.spans.back_mut() {
            buf.data.status = status;
        }
    });
}

/// Set a dynamic error status, message allocated in the current span's arena.
pub fn span_set_error(message: &str) {
    with_current_task_tracing(|tracing| {
        if let Some(buf) = tracing.spans.back_mut() {
            buf.push_status_error_dyn(message);
        }
    });
}

pub fn span_set_kind(kind: SpanKind) {
    with_current_task_tracing(|tracing| {
        if let Some(buf) = tracing.spans.back_mut() {
            buf.data.span_kind = kind;
        }
    });
}
