use std::cell::Cell;
use std::collections::VecDeque;

use crate::tracing::span::random_trace_id;
use crate::tracing::span::{SpanWrapper, new_span_data};
use crate::tracing::types::{KeyValue, SpanId, SpanKind, TraceId};

// ─── Level ────────────────────────────────────────────────────────────────────

/// Verbosity level for span/event filtering. `Off` disables everything.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(u8)]
pub enum Level {
    Off = 0,
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

impl Level {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Level::Off => "OFF",
            Level::Error => "ERROR",
            Level::Warn => "WARN",
            Level::Info => "INFO",
            Level::Debug => "DEBUG",
            Level::Trace => "TRACE",
        }
    }
}

// ─── Compile-time max level ───────────────────────────────────────────────────

/// Maximum verbosity emitted, chosen at compile time by Cargo features.
///
/// Resolution order:
/// 1. `up_to_*` features (apply to both debug and release).
/// 2. `up_to_*_in_release` features (apply only when `!debug_assertions`).
/// 3. Default: `Trace` in debug, `Warn` in release.
pub const STATIC_MAX_LEVEL: Level = {
    if cfg!(feature = "up_to_off") {
        Level::Off
    } else if cfg!(feature = "up_to_error") {
        Level::Error
    } else if cfg!(feature = "up_to_warn") {
        Level::Warn
    } else if cfg!(feature = "up_to_info") {
        Level::Info
    } else if cfg!(feature = "up_to_debug") {
        Level::Debug
    } else if cfg!(feature = "up_to_trace") {
        Level::Trace
    } else if cfg!(all(not(debug_assertions), feature = "up_to_off_in_release")) {
        Level::Off
    } else if cfg!(all(
        not(debug_assertions),
        feature = "up_to_error_in_release"
    )) {
        Level::Error
    } else if cfg!(all(
        not(debug_assertions),
        feature = "up_to_warn_in_release"
    )) {
        Level::Warn
    } else if cfg!(all(
        not(debug_assertions),
        feature = "up_to_info_in_release"
    )) {
        Level::Info
    } else if cfg!(all(
        not(debug_assertions),
        feature = "up_to_debug_in_release"
    )) {
        Level::Debug
    } else if cfg!(all(
        not(debug_assertions),
        feature = "up_to_trace_in_release"
    )) {
        Level::Trace
    } else if cfg!(not(debug_assertions)) {
        Level::Warn
    } else {
        Level::Trace
    }
};

/// `true` iff `level` is enabled at compile time. Const-foldable when `level`
/// is a constant expression — call sites that fail this check are eliminated
/// entirely by the optimiser.
pub const fn level_enabled(level: Level) -> bool {
    (level as u8) <= (STATIC_MAX_LEVEL as u8)
}

// ─── Thread-locals ────────────────────────────────────────────────────────────

thread_local!(
    static CURRENT_TASK_TRACING: Cell<*mut TaskTelemetry> =
        const { Cell::new(std::ptr::null_mut()) };
);

/// The worker this thread runs, `None` under `main_thread`.
pub fn thread_id() -> Option<usize> {
    crate::runtime::thread_options().and_then(|o| o.thread_id)
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
