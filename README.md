# slopio

A thread-per-core async runtime for Rust, built directly on io_uring. Linux only, no tokio, no epoll fallback.

slopio exposes what io_uring does differently instead of hiding it behind `AsyncRead` and `AsyncWrite`, and it is built to be checked. Every operation's lifecycle is an explicit state machine, exercised against a scripted kernel in deterministic simulation and run under Miri, and the core ones are specified in [Quint](specifications/). It is a personal project, used to try ideas properly rather than to ship them fast.


## Why

io_uring can do things a readiness-based runtime cannot express: the kernel picks the receive buffer, one submission delivers a stream of results, memory is registered once and sent without copies, and workers wake each other through the ring itself. This runtime exposes those as first-class operations instead of hiding them behind `AsyncRead` and `AsyncWrite`.

The rules that follow from that:

- **One thread, one ring, no sharing.** Each worker owns its tasks, sockets and buffers. Futures need not be `Send`, and there is not a single lock in the hot path.
- **No hidden copy, no hidden allocation.** Every I/O call says which buffer it uses and who owns it afterwards. See [ways of doing I/O](docs/io-model.md).
- **Correct by construction.** Cancelled operations never leave the kernel writing into freed memory, descriptors close only after their last completion, and invariant violations abort rather than corrupt.

## Status

Experimental. The API changes between 0.x versions, and some subsystems are stubs. Read [requirements](docs/requirements.md) before trying it: a 6.1 kernel is the floor and containers need a seccomp profile that allows io_uring.

```toml
[dependencies]
slopio = "0.1"
```

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
| `slopio` | The runtime: entry points and topologies, tasks, the io_uring driver, timers, channels, `Bytes`, messaging, the machine reader, HTTP types, tracing and the simulator. See [architecture](docs/architecture.md) for its modules. |
| `slopio-ebpf-balancer` | Optional eBPF program steering QUIC connections to the right core under `SO_REUSEPORT`, behind the `ebpf_balancer` feature. Apart because it compiles C with `clang`. |

## Building and testing

```sh
cargo build --workspace
cargo test --workspace
```

The simulated suites run the runtime against a scripted kernel and need the `simulation` feature:

```sh
cargo test -p slopio --features simulation --test simulated_runtime --test cancellation
```

Those same suites run under Miri; the exact flags are in the header of each test file.

To exercise the HTTP/3 path against a running server by hand, a `curl` built with HTTP/3 support works:

```sh
curl --http3 -k https://localhost:4433/{1,2,3} --parallel
```

Optional features on the `slopio` crate: `tracing`, `local_debug_collector`, `ebpf_balancer` (needs `clang`), `simulation`.

## License

MIT, see [LICENSE](LICENSE).
