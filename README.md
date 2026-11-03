# slopio

A thread-per-core async runtime for Rust, built directly on io_uring. Linux only, no tokio, no epoll fallback.
This runtime was the opportunity for me to try a lot of things


## Why

io_uring can do things a readiness-based runtime cannot express: the kernel picks the receive buffer, one submission delivers a stream of results, memory is registered once and sent without copies, and workers wake each other through the ring itself. This runtime exposes those as first-class operations instead of hiding them behind `AsyncRead` and `AsyncWrite`.

The rules that follow from that:

- **One thread, one ring, no sharing.** Each worker owns its tasks, sockets and buffers. Futures need not be `Send`, and there is not a single lock in the hot path.
- **No hidden copy, no hidden allocation.** Every I/O call says which buffer it uses and who owns it afterwards. See [ways of doing I/O](docs/io-model.md).
- **Correct by construction.** Cancelled operations never leave the kernel writing into freed memory, descriptors close only after their last completion, and invariant violations abort rather than corrupt.

## Status

Experimental. The API changes without notice, there is no release on crates.io, and some subsystems are stubs. Read [requirements](docs/requirements.md) before trying it: a 6.1 kernel is the floor and containers need a seccomp profile that allows io_uring.

Use `thread_per_core` instead of `main_thread` to run one such loop per core, with the workers able to [message each other](docs/messaging.md). When threads do different jobs, a [topology](docs/topology.md) gives each kind its own CPUs and ring settings.

## Documentation

- [Requirements](docs/requirements.md). Kernel versions, containers, capabilities, toolchain.
- [Ways of doing I/O](docs/io-model.md). Owned buffers, provided rings, multishot, registered and zero-copy, and why there is no single interface.
- [Streams and backpressure](docs/streams.md). How multishot accept and receive pause a fast peer and resume on their own.
- [Cancelling I/O](docs/cancellation.md). What dropping a future guarantees and what it does not.
- [Time and timers](docs/time.md). Sleeps, timeouts, which durations are served, resolution, and why reading the clock is free.
- [Messaging between workers](docs/messaging.md). Raw, typed, shared and request/reply messages over `MSG_RING`.
- [Topology](docs/topology.md). Groups of threads with their own CPUs and ring options, `SQPOLL` included, next to the per-core workers.
- [Telemetry](docs/telemetry.md). Spans, events, propagation and export, all behind features.
- [Deterministic simulation testing](docs/deterministic-testing.md). Running the runtime against a scripted kernel.
- [Architecture and code style](docs/architecture.md). How the crates and a worker fit together, the design rules, and the conventions the code is written in.
- [Potential improvements](docs/future-work.md). Queue-to-core alignment, NAPI busy polling, NUMA and SMT awareness, and when each pays.

## Crates

| Crate | Role |
|-------|------|
| `runtime` | The facade to depend on: entry points, buffer ring declarations, telemetry wiring, global allocator. |
| `runtime-core` | The runtime itself: tasks, I/O driver, timers, channels, tracing, simulator. |
| `runtime-machine` | Reads CPUs, cores, NUMA nodes and NIC queues from `/sys` and `/proc`, without privilege. |
| `runtime-streams` | The minimal `Stream` and `StreamExt` traits the streaming operations implement. |
| `runtime-http` | The `http` crate's types on the runtime's `Bytes`. |
| `async_timers` | The two timer stores: a one-minute timing wheel, and the fixed-duration deadline queues. |
| `io_uring_rb` | Provided buffer ring memory management. |
| `runtime_ebpf_balancer` | Optional eBPF program steering QUIC connections to the right core under `SO_REUSEPORT`. |

## Building and testing

```sh
cargo build --workspace
cargo test --workspace
```

The simulated suites run the runtime against a scripted kernel and need the `simulation` feature:

```sh
cargo test -p runtime-core --features simulation --test simulated_runtime --test cancellation
```

Those same suites run under Miri; the exact flags are in the header of each test file.

To exercise the HTTP/3 path against a running server by hand, a `curl` built with HTTP/3 support works:

```sh
curl --http3 -k https://localhost:4433/{1,2,3} --parallel
```

Optional features on the `runtime` crate: `tracing`, `local_debug_collector`, `ebpf_balancer` (needs `clang`), `simulation`.
