use std::collections::VecDeque;

use ::opentelemetry::SpanId;
use ::opentelemetry::TraceId;
use ::opentelemetry::trace::Event;
use ::opentelemetry_sdk::trace::SpanData;

use crate::scoped_thread_local;
use crate::tracing::span::{new_span_data, random_trace_id};

mod opentelemetry;
mod span;
mod writer;

pub use writer::TraceCollector;

scoped_thread_local!(pub(crate) static TRACE_COLLECTOR: TraceCollector);

thread_local!(
    static CURRENT_TASK_TRACING: std::cell::Cell<*mut TaskTelemetry> =
        const { std::cell::Cell::new(std::ptr::null_mut()) }
);

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

    pub fn create_new_span(&mut self, name: &'static str) -> SpanId {
        let (trace_id, parent_span_id) = match self.spans.back() {
            Some(top) => (
                top.span_context.trace_id(),
                Some(top.span_context.span_id()),
            ),
            None => (self.inherited_trace_id.unwrap_or_else(random_trace_id), None),
        };

        let span = new_span_data(trace_id, parent_span_id, name);
        let span_id = span.span_context.span_id();

        self.spans.push_back(span);
        span_id
    }
}

/// Set the active task's tracing metadata.
pub fn set_current_task_tracing(ptr: *mut TaskTelemetry) {
    CURRENT_TASK_TRACING.with(|c| c.set(ptr));
}

/// Set the active task's tracing metadata, returning the previous pointer.
/// Call with `null_mut()` to clear after polling.
pub fn swap_current_task_tracing(ptr: *mut TaskTelemetry) -> *mut TaskTelemetry {
    CURRENT_TASK_TRACING.with(|c| {
        let prev = c.get();
        c.set(ptr);
        prev
    })
}

/// Access the current task's `TracingMetadata` mutably.
/// Returns `None` if called outside of a task poll.
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

/// RAII guard that pops its span from the current task's stack when dropped.
pub struct SpanGuard {
    span_id: SpanId,
    name: &'static str,
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

        TRACE_COLLECTOR.try_with(|opt| {
            if let Some(manager) = opt {
                if let Some(span) = span {
                    manager.push_ended_span(span);
                } else {
                    unreachable!()
                }
            }
        });
    }
}

/// Get the trace_id of the current active span, if any.
pub fn current_trace_id() -> Option<TraceId> {
    with_current_task_tracing(|tracing| tracing.spans.back().map(|s| s.span_context.trace_id()))
        .flatten()
}

/// Create a new span in the current task, returning a `SpanGuard` that pops it on drop.
/// Returns `None` if called outside of a task poll.
pub fn new_span(name: &'static str) -> Option<SpanGuard> {
    with_current_task_tracing(|tracing| SpanGuard {
        span_id: tracing.create_new_span(name),
        name,
    })
}

/// Add an event to the current span.
/// No-op if called outside of a task poll or when no span is active.
pub fn event(message: &str) {
    with_current_task_tracing(|tracing| {
        if let Some(span) = tracing.spans.back_mut() {
            span.events
                .events
                .push(Event::with_name(message.to_owned()));
        }
    });
}

pub trait TraceWriter: Drop {
    fn write_span(&self, span: SpanData);
    async fn flush(&self);
}
