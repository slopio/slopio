use crate::scoped_thread_local;
use crate::tracing::opentelemetry::TraceSink;

pub mod collector;
mod opentelemetry;
mod propagation;
mod resource;
mod span;
mod task;
pub mod types;

pub use collector::Collector;
pub use collector::console::ConsoleCollector;
pub use opentelemetry::init_trace_sink;
pub use propagation::{extract, inject};
pub(crate) use resource::resource_config;
pub use resource::{ResourceConfig, hostname, set_resource};
pub use span::{
    Span, SpanWrapper, current_span_id, current_trace_id, event, event_attrs, new_span, new_trace,
    new_trace_with_parent, span_set_attribute, span_set_error, span_set_kind, span_set_status,
};
pub use task::{
    Level, TaskTelemetry, set_current_task_tracing, set_max_level, set_thread_id,
    swap_current_task_tracing, with_current_task_tracing,
};
pub use types::{KeyValue, SpanKind, SpanLink, Status};

pub(crate) use task::thread_id;

scoped_thread_local!(pub static TRACING_SINK : TraceSink);
