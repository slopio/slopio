use std::cell::Cell;
use std::collections::VecDeque;
use std::sync::OnceLock;

use ::opentelemetry::SpanId;
use ::opentelemetry::TraceId;
use ::opentelemetry::trace::Event;
use ::opentelemetry_sdk::trace::SpanData;

use crate::runtime::TIME_DRIVER;
use crate::scoped_thread_local;
use crate::tracing::span::{new_span_data, random_trace_id};

mod collector;
mod opentelemetry;
pub(crate) mod otlp;
pub mod proto;
mod span;

pub use ::opentelemetry::KeyValue;
pub use ::opentelemetry::trace::{SpanKind, Status};
pub use collector::{Collector, TempoExporter, TraceCollector};
pub use opentelemetry::{TraceSink, init_trace_sink};

scoped_thread_local!(pub static TRACING_SINK : TraceSink);

/// Resource attributes describing this service/process. Set once before starting the runtime.
pub struct ResourceConfig {
    pub service_name: &'static str,
    pub service_version: &'static str,
    /// Hostname of the machine (e.g. `tracing::hostname()`).
    pub host_name: Option<String>,
}

impl Default for ResourceConfig {
    fn default() -> Self {
        Self {
            service_name: "my-runtime",
            service_version: "",
            host_name: None,
        }
    }
}

static RESOURCE: OnceLock<ResourceConfig> = OnceLock::new();

/// Set the resource attributes for this process. Call before `main_thread` / `thread_per_core`.
/// Has no effect if called more than once.
pub fn set_resource(config: ResourceConfig) {
    let _ = RESOURCE.set(config);
}

pub(crate) fn resource_config() -> &'static ResourceConfig {
    RESOURCE.get_or_init(ResourceConfig::default)
}

/// Read the machine hostname from `/etc/hostname`.
pub fn hostname() -> String {
    std::fs::read_to_string("/etc/hostname")
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

thread_local!(
    static CURRENT_TASK_TRACING: Cell<*mut TaskTelemetry> =
        const { Cell::new(std::ptr::null_mut()) };
    static MAX_LEVEL: Cell<Level> = const { Cell::new(Level::Info) };
    static THREAD_ID: Cell<Option<usize>> = const { Cell::new(None) };
);

pub fn set_thread_id(n: usize) {
    THREAD_ID.with(|c| c.set(Some(n)));
}

pub(crate) fn thread_id() -> Option<usize> {
    THREAD_ID.with(|c| c.get())
}

/// Verbosity level for span filtering. Spans below the current max level are dropped.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum Level {
    Error = 0,
    Warn = 1,
    Info = 2,
    Debug = 3,
    Trace = 4,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Level::Error => "ERROR",
            Level::Warn => "WARN",
            Level::Info => "INFO",
            Level::Debug => "DEBUG",
            Level::Trace => "TRACE",
        }
    }
}

pub fn set_max_level(level: Level) {
    MAX_LEVEL.with(|c| c.set(level));
}

fn level_enabled(level: Level) -> bool {
    MAX_LEVEL.with(|c| level <= c.get())
}

#[derive(Default)]
pub struct TaskTelemetry {
    spans: VecDeque<SpanData>,
    inherited_trace_id: Option<TraceId>,
}

impl TaskTelemetry {
    pub fn with_trace_id(trace_id: TraceId) -> Self {
        Self {
            spans: VecDeque::new(),
            inherited_trace_id: Some(trace_id),
        }
    }

    fn create_new_span(&mut self, name: &'static str, new_trace: bool, level: Level) -> SpanId {
        let (trace_id, parent_span_id) = if new_trace {
            (random_trace_id(), None)
        } else {
            match self.spans.back() {
                Some(top) => (
                    top.span_context.trace_id(),
                    Some(top.span_context.span_id()),
                ),
                None => (
                    self.inherited_trace_id.unwrap_or_else(random_trace_id),
                    None,
                ),
            }
        };

        let mut span = new_span_data(trace_id, parent_span_id, name, SpanKind::Internal);
        span.attributes.push(KeyValue::new("level", level.as_str()));
        let span_id = span.span_context.span_id();
        self.spans.push_back(span);
        span_id
    }

    fn create_span_with_remote_parent(
        &mut self,
        name: &'static str,
        level: Level,
        parent: (TraceId, SpanId),
    ) -> SpanId {
        let mut span = new_span_data(parent.0, Some(parent.1), name, SpanKind::Internal);
        span.parent_span_is_remote = true;
        span.attributes.push(KeyValue::new("level", level.as_str()));
        let span_id = span.span_context.span_id();
        self.spans.push_back(span);
        span_id
    }
}

pub fn set_current_task_tracing(ptr: *mut TaskTelemetry) {
    CURRENT_TASK_TRACING.with(|c| c.set(ptr));
}

pub fn swap_current_task_tracing(ptr: *mut TaskTelemetry) -> *mut TaskTelemetry {
    CURRENT_TASK_TRACING.with(|c| {
        let prev = c.get();
        c.set(ptr);
        prev
    })
}

pub fn with_current_task_tracing<R>(f: impl FnOnce(&mut TaskTelemetry) -> R) -> Option<R> {
    CURRENT_TASK_TRACING.with(|c| {
        let ptr = c.get();
        if ptr.is_null() {
            None
        } else {
            Some(f(unsafe { &mut *ptr }))
        }
    })
}

#[derive(Debug)]
struct SpanGuard {
    span_id: SpanId,
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        let span = with_current_task_tracing(|tracing| {
            if let Some(pos) = tracing
                .spans
                .iter()
                .rposition(|s| s.span_context.span_id() == self.span_id)
            {
                tracing.spans.remove(pos)
            } else {
                None
            }
        })
        .flatten();

        if let Some(mut span) = span {
            span.end_time = TIME_DRIVER.with(|d| d.now());
            TRACING_SINK.with(|output| output.push_span(span))
        }
    }
}

/// RAII guard for a span. Holds the span open until dropped.
/// Use the builder methods (`.kind()`) immediately after creation.
#[must_use = "Span must be held for the duration of the operation"]
#[derive(Debug)]
pub struct Span(Option<SpanGuard>);

impl Span {
    /// Set the [`SpanKind`] for this span.
    pub fn kind(self, kind: SpanKind) -> Self {
        if self.0.is_some() {
            with_current_task_tracing(|tracing| {
                if let Some(span) = tracing.spans.back_mut() {
                    span.span_kind = kind;
                }
            });
        }
        self
    }

    /// Add an attribute to this span.
    pub fn attr(self, kv: KeyValue) -> Self {
        if self.0.is_some() {
            with_current_task_tracing(|tracing| {
                if let Some(span) = tracing.spans.back_mut() {
                    span.attributes.push(kv);
                }
            });
        }
        self
    }
}

pub fn current_trace_id() -> Option<TraceId> {
    with_current_task_tracing(|tracing| tracing.spans.back().map(|s| s.span_context.trace_id()))
        .flatten()
}

pub fn current_span_id() -> Option<SpanId> {
    with_current_task_tracing(|tracing| tracing.spans.back().map(|s| s.span_context.span_id()))
        .flatten()
}

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

pub fn event(message: &str) {
    with_current_task_tracing(|tracing| {
        if let Some(span) = tracing.spans.back_mut() {
            span.events
                .events
                .push(Event::with_name(message.to_owned()));
        }
    });
}

pub fn span_set_attribute(kv: KeyValue) {
    with_current_task_tracing(|tracing| {
        if let Some(span) = tracing.spans.back_mut() {
            span.attributes.push(kv);
        }
    });
}

pub fn span_set_status(status: Status) {
    with_current_task_tracing(|tracing| {
        if let Some(span) = tracing.spans.back_mut() {
            span.status = status;
        }
    });
}

pub fn span_set_kind(kind: SpanKind) {
    with_current_task_tracing(|tracing| {
        if let Some(span) = tracing.spans.back_mut() {
            span.span_kind = kind;
        }
    });
}

/// Serialize the current span context as a W3C `traceparent` header value.
pub fn inject() -> Option<String> {
    with_current_task_tracing(|tracing| {
        tracing.spans.back().map(|span| {
            let trace_id = u128::from_be_bytes(span.span_context.trace_id().to_bytes());
            let span_id = u64::from_be_bytes(span.span_context.span_id().to_bytes());
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
