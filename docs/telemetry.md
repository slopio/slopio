# Telemetry

The runtime ships its own tracing: spans, events, W3C trace propagation and OpenTelemetry export, with no dependency on the `tracing` or `opentelemetry` crates. It is off unless you ask for it, and when it is off it is not there at all.

## Turning it on

Telemetry is a Cargo feature. With it on, `main_thread_with` and `thread_per_core_with` print spans to stderr; `main_thread_traced` and `thread_per_core_traced` take a closure that builds the collector instead, called once on each worker.

| Feature | What you get |
|---------|--------------|
| none | No spans, no events. The macros compile to nothing. |
| `tracing` | Spans and events, printed one line per span to stderr. |
| `tracing` + `local_debug_collector` | `LocalDebugCollector`: spans written as JSONL files, one file per worker, rotated by size and age, capped on disk. For reading traces locally without a backend. |
| `otlp` on slopio-h2 | `OtlpCollector`: spans exported over OTLP/gRPC to a backend such as Tempo or Jaeger. It lives in slopio-h2 because it needs an HTTP/2 client. |

```rust
let endpoint = "127.0.0.1:4317".parse().unwrap();
let service = OtlpService { name: "api".into(), version: None, host_name: None };
runtime::thread_per_core_traced::<Ring, _, _>(
    move || OtlpCollector::<Ring>::new(endpoint, &service),
    |ctx| Box::pin(worker(ctx)),
);
```

`OtlpService` becomes the resource the backend files spans under, with each worker's index as `service.instance.id`. `OtlpCollector` receives its responses into a buffer ring of yours, so the ring it names must be one the runtime was started with. Any type implementing `Collector` can be plugged in the same way.

The verbosity ceiling is also a feature, chosen at compile time: `up_to_error`, `up_to_warn`, `up_to_info`, `up_to_debug`, `up_to_trace` or `up_to_off`, with `_in_release` variants that apply to release builds only. Without any of them, debug builds emit everything and release builds emit warnings and errors. A span or event above the ceiling is not filtered, it is removed by the compiler along with the code that formats its arguments.

## Spans and events

Open a span with `span!`, or start a fresh trace with `new_trace!`. The span is a guard: it is open while you hold it and ends when you drop it. Attach attributes with a builder on creation, or later with the `span_set_*` functions. Log with `info!`, `warn!`, `error!`, `debug!` and `trace!`; each becomes an event on the span currently open in the task.

Spans nest per task. A span opened while another is open becomes its child, and a task spawned from inside a span inherits its trace id, so a request that fans out across tasks stays one trace. Spans do not cross tasks otherwise: the current span is a property of the task, not of the thread.

An event emitted with no span open is dropped. Open a span first, even a coarse one per request.

## Propagation

`inject()` gives you the current span as a W3C `traceparent` header value, and `extract()` parses one coming in. Start a span with `new_trace_with_parent!` on the extracted context and the incoming request joins the caller's trace.

## Cost

Telemetry is designed so that you can leave it on in production at the `info` level.

- **Compile-time filtering.** Anything above the ceiling costs nothing, not even a branch.
- **No heap allocation per span.** Attribute values and event messages are copied into a small arena that lives with the span. Static strings are not copied at all.
- **Timestamps are free.** Span start and end use the runtime's cached clock, see [time](time.md).
- **Export is off the hot path.** A closed span is handed to a per-worker channel and the exporter runs as an ordinary task on the same thread, batching whatever accumulated during the last turn of the event loop. Nothing blocks and no lock is taken.

## What is not there yet

The OTLP exporter has one setting, the endpoint, over plaintext HTTP/2. A batch that fails or is unanswered after ten seconds is dropped. There is no sampling, no bounded queue, no compression, no TLS, no authentication headers, and no reading of the standard `OTEL_*` environment variables. In a busy service you will want at least a bounded queue and sampling before pointing this at a shared collector. `telemetry_options.md` in the repository root lists what is planned.
