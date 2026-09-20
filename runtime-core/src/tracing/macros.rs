//! Logging and span macros.
//!
//! Each macro checks [`crate::tracing::level_enabled`] at the expansion site.
//! Because `level_enabled` is a `const fn` and `STATIC_MAX_LEVEL` is a
//! compile-time constant, the check folds to `true` or `false` whenever the
//! macro is called with a constant `Level`. Statically-disabled call sites are
//! eliminated entirely by the optimiser — no allocation, no `format_args!`
//! evaluation, no thread-local lookup.

/// Emit an event at `$lvl` on the current span.
///
/// ```ignore
/// event!(Level::Info, "user logged in");
/// event!(Level::Debug, "query took {} ms", elapsed);
/// ```
#[macro_export]
macro_rules! event {
    ($lvl:expr, $msg:literal $(,)?) => {{
        if $crate::tracing::level_enabled($lvl) {
            $crate::tracing::event($lvl, $msg);
        }
    }};
    ($lvl:expr, $fmt:literal, $($arg:tt)+) => {{
        if $crate::tracing::level_enabled($lvl) {
            $crate::tracing::event($lvl, &::std::format!($fmt, $($arg)+));
        }
    }};
}

#[macro_export]
macro_rules! error {
    ($($arg:tt)*) => { $crate::event!($crate::tracing::Level::Error, $($arg)*) };
}

#[macro_export]
macro_rules! warn {
    ($($arg:tt)*) => { $crate::event!($crate::tracing::Level::Warn, $($arg)*) };
}

#[macro_export]
macro_rules! info {
    ($($arg:tt)*) => { $crate::event!($crate::tracing::Level::Info, $($arg)*) };
}

#[macro_export]
macro_rules! debug {
    ($($arg:tt)*) => { $crate::event!($crate::tracing::Level::Debug, $($arg)*) };
}

#[macro_export]
macro_rules! trace {
    ($($arg:tt)*) => { $crate::event!($crate::tracing::Level::Trace, $($arg)*) };
}

/// Open a span at `$lvl` rooted in the current trace. Returns a
/// [`crate::tracing::Span`] RAII guard — drop ends the span.
///
/// ```ignore
/// let _s = span!(Level::Info, "handle_request");
/// ```
#[macro_export]
macro_rules! span {
    ($lvl:expr, $name:expr) => {{
        if $crate::tracing::level_enabled($lvl) {
            $crate::tracing::new_span($name, $lvl)
        } else {
            $crate::tracing::Span::disabled()
        }
    }};
}

/// Start a new trace (new root span) at `$lvl`.
#[macro_export]
macro_rules! new_trace {
    ($lvl:expr, $name:expr) => {{
        if $crate::tracing::level_enabled($lvl) {
            $crate::tracing::new_trace($name, $lvl)
        } else {
            $crate::tracing::Span::disabled()
        }
    }};
}

/// Start a new trace rooted at a remote parent (extracted from e.g. `traceparent`).
#[macro_export]
macro_rules! new_trace_with_parent {
    ($lvl:expr, $name:expr, $parent:expr) => {{
        if $crate::tracing::level_enabled($lvl) {
            $crate::tracing::new_trace_with_parent($name, $lvl, $parent)
        } else {
            $crate::tracing::Span::disabled()
        }
    }};
}

#[macro_export]
macro_rules! error_span {
    ($name:expr) => {
        $crate::span!($crate::tracing::Level::Error, $name)
    };
}

#[macro_export]
macro_rules! warn_span {
    ($name:expr) => {
        $crate::span!($crate::tracing::Level::Warn, $name)
    };
}

#[macro_export]
macro_rules! info_span {
    ($name:expr) => {
        $crate::span!($crate::tracing::Level::Info, $name)
    };
}

#[macro_export]
macro_rules! debug_span {
    ($name:expr) => {
        $crate::span!($crate::tracing::Level::Debug, $name)
    };
}

#[macro_export]
macro_rules! trace_span {
    ($name:expr) => {
        $crate::span!($crate::tracing::Level::Trace, $name)
    };
}
