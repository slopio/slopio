use std::collections::VecDeque;

use crate::{scoped_thread_local, tracing::span::SpanMetadata};

mod span;
mod writer;
pub use writer::TraceCollector;

scoped_thread_local!(pub(crate) static TRACE_COLLECTOR: TraceCollector);

thread_local!(
    static CURRENT_TASK_TRACING: std::cell::Cell<*mut TracingMetadata> =
        const { std::cell::Cell::new(std::ptr::null_mut()) }
);

#[derive(Default)]
pub struct TracingMetadata {
    spans: VecDeque<SpanMetadata>,
}

impl TracingMetadata {
    pub fn create_new_span(&mut self, name: &'static str) -> u64 {
        let (trace_id, parent_span_id) = match self.spans.back() {
            Some(top) => (top.trace_id, Some(top.span_id)),
            None => (rand::random(), None),
        };

        let span = SpanMetadata::new(trace_id, parent_span_id, name);
        let span_id = span.span_id;

        self.spans.push_back(span);
        TRACE_COLLECTOR.try_with(|opt| {
            if let Some(manager) = opt {
                manager.trace(name);
            }
        });
        span_id
    }
}

/// Set the active task's tracing metadata.
pub fn set_current_task_tracing(ptr: *mut TracingMetadata) {
    CURRENT_TASK_TRACING.with(|c| c.set(ptr));
}

/// Set the active task's tracing metadata, returning the previous pointer.
/// Call with `null_mut()` to clear after polling.
pub fn swap_current_task_tracing(ptr: *mut TracingMetadata) -> *mut TracingMetadata {
    CURRENT_TASK_TRACING.with(|c| {
        let prev = c.get();
        c.set(ptr);
        prev
    })
}

/// Access the current task's `TracingMetadata` mutably.
/// Returns `None` if called outside of a task poll.
pub fn with_current_task_tracing<R>(f: impl FnOnce(&mut TracingMetadata) -> R) -> Option<R> {
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
    span_id: u64,
    name: &'static str,
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        let span = with_current_task_tracing(|tracing| {
            if let Some(pos) = tracing
                .spans
                .iter()
                .rposition(|s| s.span_id == self.span_id)
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

/// Create a new span in the current task, returning a `SpanGuard` that pops it on drop.
/// Returns `None` if called outside of a task poll.
pub fn new_span(name: &'static str) -> Option<SpanGuard> {
    with_current_task_tracing(|tracing| SpanGuard {
        span_id: tracing.create_new_span(name),
        name,
    })
}

/// Append a trace string on the current thread.
/// No-op if called outside of a runtime context.
pub fn event(message: &str) {}

pub trait TraceWriter: Drop {
    fn write_span(&self, span: SpanMetadata);
    fn flush(&self);
}
