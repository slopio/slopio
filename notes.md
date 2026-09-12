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

TCP hot path allocations, measured 2026-09-11 with a counting global
allocator on the runtime thread only (harness not kept). Steady-state echo on
an established connection is 0 alloc / 0 B per round-trip. What is left costs
per connection or per timeout:

- two `MultiOpQueue` allocations per multishot op: 336 B each
  (`VecDeque::with_capacity(QUEUE_SIZE + 10)` × 8 B/event), one in `MultiRecv`
  and one built driver-side at the `Init -> Sent` transition. A full
  connection is 4 allocs / 896 B — `[24, 200, 336, 336]` = `Rc<SocketInner>`,
  task box, then those two queues. So 75% of the bytes and half the
  allocations of a short-lived connection are the queues.
  Fix: `QUEUE_SIZE` is already a const generic on `MultiRecv`, so its queue
  can be an inline `[MultishotCompletionEvent; QUEUE_SIZE]` ring. The
  driver-side one needs `MultiOpState` to carry a fixed capacity (or a
  `SmallVec`) since it is not generic over the size. Tradeoff: inline in the
  multishot slab means the memory is no longer returned when a connection
  closes, it stays sized to the peak.
- a cancelled timer keeps its slot until the clock reaches its deadline, in
  both stores: the wheel's `TimerStorage::drop` writes `Cancelled` but leaves
  the id parked in its bucket, and `Timers::cancel` only clears a bit unless
  the entry is at the head. Intentional in the wheel (removing it early would
  let the slab reuse an id the bucket still names), but the live set is then
  `rate × timeout duration`, not `timeouts in flight`. A read+write
  `timeout(5s, ..)` per request at 100k req/s is ~1M dead slots ≈ 40 MB
  (32 B slab entry + 8 B bucket entry), with the per-slot bucket `SmallVec`s
  well past their 8 inline entries. Both stores are written up in
  `async_timers/DESIGN.md`. Shows up in the echo-under-timeout phase as 18
  reallocs / 327 KB over 2000 round-trips — growth to the plateau, not a
  per-op cost.
  Bounded by the duration itself (1 min in the wheel, 5 s in the fixed
  store) rather than by an arbitrary timeout — but the shape is unchanged. The
  1M-dead-slot figure above is the fixed store's case, which is where the
  high-rate timeouts are meant to live; the wheel's minute-long reach is for
  the occasional long sleep, not for a timeout per request.
  Fix, if that profile matters: sweep dead entries without waiting for the
  clock, or document that timeouts should be short and re-armed.
- `socket_send_vectored` allocates a `Vec<libc::iovec>` per call (16 B per
  chunk) on top of the caller's `Vec<B>` — the only send path that allocates
  on the hot path. `send_vectored_fixed_zc` does the same `Vec::with_capacity`.
  Fix: a reusable per-socket iovec scratch buffer.
- the root future of `block_on` is polled with `Waker::noop()`, so a self-wake
  from the root is dropped and the loop falls through to `submit_and_wait`.
  Correct (the root is re-polled every pass) but a `yield_now()` in the root
  costs a blocking round-trip to the next timer tick instead of a requeue.
  Spawned tasks are unaffected — they get a real waker.
