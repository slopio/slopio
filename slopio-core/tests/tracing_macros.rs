//! Sanity tests for the tracing macros. We don't validate emission here
//! (that needs a Collector setup) — we just check that the macros expand,
//! compile, and obey compile-time level gating.

#![cfg(feature = "tracing")]

#[cfg(not(any(
    feature = "up_to_off",
    feature = "up_to_error",
    feature = "up_to_warn",
    feature = "up_to_info",
    feature = "up_to_debug",
    feature = "up_to_trace",
)))]
use slopio_core::tracing::STATIC_MAX_LEVEL;
use slopio_core::tracing::{Level, level_enabled};

#[test]
fn level_enabled_is_const() {
    // Const-evaluable in const context — proves it's a const fn.
    const _ERROR_ENABLED: bool = level_enabled(Level::Error);
    const _OFF_ENABLED: bool = level_enabled(Level::Off);
    assert!(_ERROR_ENABLED);
    assert!(_OFF_ENABLED);
}

#[test]
#[cfg(not(any(
    feature = "up_to_off",
    feature = "up_to_error",
    feature = "up_to_warn",
    feature = "up_to_info",
    feature = "up_to_debug",
    feature = "up_to_trace",
)))]
fn default_level_matches_build_profile() {
    // Without any explicit up_to_* feature, debug → Trace, release → Warn.
    #[cfg(debug_assertions)]
    assert_eq!(STATIC_MAX_LEVEL, Level::Trace);

    #[cfg(not(debug_assertions))]
    assert_eq!(STATIC_MAX_LEVEL, Level::Warn);
}

#[test]
fn macros_expand_without_a_collector() {
    // None of these should panic when called outside a tracing setup;
    // they all early-return when there's no current task tracing.
    slopio_core::error!("static error");
    slopio_core::warn!("static warn");
    slopio_core::info!("static info");
    slopio_core::debug!("static debug");
    slopio_core::trace!("static trace");

    let x = 42;
    slopio_core::info!("formatted {} value", x);
    slopio_core::debug!("multi {} args {}", x, "ok");

    let _s1 = slopio_core::info_span!("test_span");
    let _s2 = slopio_core::debug_span!("nested");
    let _s3 = slopio_core::new_trace!(Level::Info, "root");
}
