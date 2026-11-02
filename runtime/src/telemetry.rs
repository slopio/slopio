/// What a worker installs around its root future: a span collector under
/// `tracing`, built on the worker by a factory; nothing otherwise.
pub trait Telemetry {
    fn install<O>(&self, run: impl FnOnce() -> O) -> O;
}

#[cfg(feature = "tracing")]
impl<C: crate::tracing::Collector + 'static, M: Fn() -> C> Telemetry for M {
    fn install<O>(&self, run: impl FnOnce() -> O) -> O {
        let sink = crate::tracing::init_trace_sink(self());
        crate::tracing::TRACING_SINK.set(&sink, run)
    }
}

#[cfg(not(feature = "tracing"))]
impl Telemetry for () {
    fn install<O>(&self, run: impl FnOnce() -> O) -> O {
        run()
    }
}

#[cfg(feature = "tracing")]
pub type DefaultTelemetry = fn() -> crate::tracing::ConsoleCollector;
#[cfg(not(feature = "tracing"))]
pub type DefaultTelemetry = ();

#[cfg(feature = "tracing")]
pub const DEFAULT_TELEMETRY: DefaultTelemetry = || crate::tracing::ConsoleCollector;
#[cfg(not(feature = "tracing"))]
pub const DEFAULT_TELEMETRY: DefaultTelemetry = ();
