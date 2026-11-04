use crate::scoped_thread_local;
use crate::tracing::opentelemetry::TraceSink;

pub mod collector;
#[macro_use]
mod macros;
mod opentelemetry;
mod propagation;
mod span;
mod task;
pub mod types;

pub use collector::Collector;
pub use collector::console::ConsoleCollector;
#[cfg(feature = "local_debug_collector")]
pub use collector::local_debug_collector::{
    LocalDebugCollector, LocalDebugConfig, set_local_debug_config,
};
pub use opentelemetry::init_trace_sink;
pub use propagation::{extract, inject};
pub use span::{
    Span, SpanWrapper, current_span_id, current_trace_id, event, event_attrs, new_span, new_trace,
    new_trace_with_parent, span_set_attribute, span_set_error, span_set_kind, span_set_status,
};
pub use task::{
    Level, STATIC_MAX_LEVEL, TaskTelemetry, level_enabled, set_current_task_tracing,
    swap_current_task_tracing, thread_id, with_current_task_tracing,
};
pub use types::{KeyValue, SpanKind, SpanLink, Status};

scoped_thread_local!(pub static TRACING_SINK : TraceSink);
