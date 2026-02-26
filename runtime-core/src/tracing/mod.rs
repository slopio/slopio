use std::collections::VecDeque;

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

pub use collector::{Collector, TempoExporter, TraceCollector};
pub use opentelemetry::{TraceSink, init_trace_sink};

scoped_thread_local!(pub static TRACING_SINK : TraceSink);

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

    pub fn create_new_span(&mut self, name: &'static str, new_trace: bool) -> SpanId {
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

        let span = new_span_data(trace_id, parent_span_id, name);
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

#[must_use = "SpanGuard must be held for the duration of the span"]
#[derive(Debug)]
pub struct SpanGuard {
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

pub fn current_trace_id() -> Option<TraceId> {
    with_current_task_tracing(|tracing| tracing.spans.back().map(|s| s.span_context.trace_id()))
        .flatten()
}

pub fn new_span(name: &'static str) -> Option<SpanGuard> {
    with_current_task_tracing(|tracing| SpanGuard {
        span_id: tracing.create_new_span(name, false),
    })
}

pub fn new_trace(name: &'static str) -> Option<SpanGuard> {
    with_current_task_tracing(|tracing| SpanGuard {
        span_id: tracing.create_new_span(name, true),
    })
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
