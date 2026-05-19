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
