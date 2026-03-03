use std::cell::Cell;
use std::collections::VecDeque;

use crate::tracing::span::random_trace_id;
use crate::tracing::span::{SpanWrapper, new_span_data};
use crate::tracing::types::{KeyValue, SpanId, SpanKind, TraceId};

// ─── Level ────────────────────────────────────────────────────────────────────

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
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Level::Error => "ERROR",
            Level::Warn => "WARN",
            Level::Info => "INFO",
            Level::Debug => "DEBUG",
            Level::Trace => "TRACE",
        }
    }
}

// ─── Thread-locals ────────────────────────────────────────────────────────────

thread_local!(
    static CURRENT_TASK_TRACING: Cell<*mut TaskTelemetry> =
        const { Cell::new(std::ptr::null_mut()) };
    static MAX_LEVEL: Cell<Level> = const { Cell::new(Level::Info) };
    static THREAD_ID: Cell<Option<usize>> = const { Cell::new(None) };
);

pub fn set_max_level(level: Level) {
    MAX_LEVEL.with(|c| c.set(level));
}

pub(crate) fn level_enabled(level: Level) -> bool {
    MAX_LEVEL.with(|c| level <= c.get())
}

pub fn set_thread_id(n: usize) {
    THREAD_ID.with(|c| c.set(Some(n)));
}

pub(crate) fn thread_id() -> Option<usize> {
    THREAD_ID.with(|c| c.get())
}

// ─── TaskTelemetry ───────────────────────────────────────────────────────────

#[derive(Default)]
pub struct TaskTelemetry {
    pub(crate) spans: VecDeque<SpanWrapper>,
    inherited_trace_id: Option<TraceId>,
}

impl TaskTelemetry {
    pub fn with_trace_id(trace_id: TraceId) -> Self {
        Self {
            spans: VecDeque::new(),
            inherited_trace_id: Some(trace_id),
        }
    }

    pub(crate) fn create_new_span(
        &mut self,
        name: &'static str,
        new_trace: bool,
        level: Level,
    ) -> SpanId {
        let (trace_id, parent_span_id) = if new_trace {
            (random_trace_id(), None)
        } else {
            match self.spans.back() {
                Some(top) => (top.data.trace_id, Some(top.data.span_id)),
                None => (
                    self.inherited_trace_id.unwrap_or_else(random_trace_id),
                    None,
                ),
            }
        };

        let mut span = new_span_data(trace_id, parent_span_id, name, SpanKind::Internal);
        span.attributes.push(KeyValue::new("level", level.as_str()));
        let span_id = span.span_id;
        self.spans.push_back(SpanWrapper::new(span));
        span_id
    }

    pub(crate) fn create_span_with_remote_parent(
        &mut self,
        name: &'static str,
        level: Level,
        parent: (TraceId, SpanId),
    ) -> SpanId {
        let mut span = new_span_data(parent.0, Some(parent.1), name, SpanKind::Internal);
        span.parent_span_is_remote = true;
        span.attributes.push(KeyValue::new("level", level.as_str()));
        let span_id = span.span_id;
        self.spans.push_back(SpanWrapper::new(span));
        span_id
    }
}

// ─── Task-tracing accessors ───────────────────────────────────────────────────

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
