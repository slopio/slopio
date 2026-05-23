- io operations are hold by a SLAB, when no task has the id pointing to the op, it should be freed.
- same for time operations
- io_uring always replies to demands.
- tracing event macros: formatted events (e.g. `info!("x = {}", val)`) currently
  do `format!()` inside the level-gated branch, then pass `&str` to `event()`,
  which arena-allocates a copy. Cost when filtered: zero. Cost when enabled:
  one heap alloc (String) + one arena copy — double allocation.
  Fix: expose `event_fmt(level, args: std::fmt::Arguments)` that writes the
  formatted output directly into the span's bumpalo arena via `write!`, no
  intermediate String. Then have the macro call that instead of `format!`.
- `CollectorChoice` enum (when wired into `main_thread` / `thread_per_core`)
  will only allow the built-in collectors (Console, Otlp, LocalDebug).
  User-defined `Collector` impls can't be plugged in via the enum.
  Future: add a `Custom(...)` escape hatch. Tradeoffs: `Box<dyn Collector>`
  requires the trait to be object-safe, but the async `process` method makes
  that awkward; a closure-based factory works but adds complexity. Until then,
  custom collectors drop down to `init_trace_sink::<C>()` + `TRACING_SINK.set`
  and bypass the convenience entry points.
