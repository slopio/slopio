# Architecture and code style

How the runtime is put together, the rules its design follows, and the conventions the code is written in. The other documents describe how to use the runtime; this one is for reading and changing it.

## The crate and its modules

Everything is in one crate, `slopio`, except the eBPF balancer, `slopio-ebpf-balancer`: it compiles a C program with `clang` at build time, so it stays apart, behind the `ebpf_balancer` feature.

| Module | Role |
|--------|------|
| `runtime` | Tasks and the event loop, time, messaging between threads, the options a runtime is built with. |
| `io_driver`, `net` | The io_uring driver and its operations, sockets and files. |
| `topology` | Groups of threads, their placement on CPUs, the block printed at startup. |
| `machine` | Reads the machine from `/sys` and `/proc` without privilege: CPUs, cores, NUMA nodes, NIC queues and where their packets are processed. |
| `bytes`, `channels`, `streams`, `task` | `Bytes` and `BytesMut` for one thread, channels, the `Stream` traits, task handles. |
| `http` | The `http` crate's types on the runtime's `Bytes`, not `Send`. |
| `timers`, `buf_ring_memory` | Internal: the two timer stores the event loop drives, a timing wheel for any duration up to one hour and a queue for exactly five seconds, see [time](time.md); and the memory of provided buffer rings. |
| `tracing`, `deterministic_testing` | Behind the `tracing` and `simulation` features: spans and their export, the simulated kernel. |

Dependencies are few and chosen one by one: `io-uring`, `libc`, `rustix`, `socket2`, `slab`, `bumpalo`, `smallvec`, `pin-project`, `bitfield-struct`, `rand`, and `libmimalloc-sys` for the global allocator. There is no tokio, no `futures`, no `bytes` crate in the runtime's dependencies: what the runtime needs from them it has its own version of, shaped for one thread.

## One thread, one runtime

Every worker thread runs one runtime, and a runtime belongs to one thread for its whole life. It owns:

- a **task list**, the run queue of the futures spawned on it;
- an **I/O driver**, one io_uring ring with its submission and completion queues, the slabs that hold every operation in flight, and the buffer rings installed on it;
- a **time driver**, the timer stores;
- the **options** it was built with, which code on the thread reads with `thread_options()`.

Nothing of it is shared with another thread. That is why futures need not be `Send`, why the driver's state sits in `UnsafeCell` and `Cell` rather than behind locks, and why there are no atomics on the hot path. While `block_on` runs, the task list, the driver and the time driver are reachable through scoped thread-locals; outside it they are not, and calling into them panics.

The event loop is in `RuntimeInner::block_on`. Each turn:

1. advances the timers;
2. polls the tasks that were queued when the turn started, and only those, so a task that keeps waking itself cannot starve timers and I/O;
3. submits and reaps completions, and only when tasks queued work or the kernel holds completions back, so a turn with no I/O costs no syscall;
4. polls the root future;
5. when nothing is runnable, sleeps in the kernel until the next completion or the next timer.

The ring is set up with `SINGLE_ISSUER`, `DEFER_TASKRUN` and `COOP_TASKRUN` by default: completions are processed in batches, when the thread asks for them, never in the middle of its work.

## Operations and their lifetime

An I/O call returns a future that owns everything the kernel will touch: the buffer, a reference to the descriptor. Submitting it puts an entry in a slab; the slab index, with a few tag bits, is the SQE's `user_data`, and the CQE that comes back finds its entry by it. Owned buffers are handed back when the operation completes.

Dropping the future does not free what the kernel may still write into. The entry stays in the slab, the buffer and the descriptor reference move into it, and a cancel is submitted; only the operation's final completion releases them. A descriptor is closed, with a close submitted on the ring, only once its last reference is gone, so its close is always the last event on it. A completion that reports a closed descriptor means that rule was broken, and the runtime panics rather than go on.

Multishot operations deliver a stream of completions from one submission. They pause themselves when their consumer falls behind and re-arm when it catches up, see [streams](streams.md).

Buffer rings are declared as zero-sized types with `define_buf_ring!`. Each type carries its ring's id and sizes as constants and its per-thread state, so a receive against a ring is generic over that type and the buffer it yields, `BorrowedBuffer<S>`, gives its slot back on drop. A `Bytes` built on a received slot holds the slot until its last view is dropped.

## Between threads

Threads of a topology talk over `MSG_RING`: a message is posted onto the receiving thread's completion queue and wakes it like any completion, see [messaging](messaging.md). Every thread finds its peers' rings in a table filled in at startup, before any task runs.

A waker is only ever woken on the thread it belongs to. Nothing in the runtime makes wakers `Send`, counts their references atomically, or hands them to another thread: work that crosses threads crosses as a message.

## Configuration

Everything a runtime is built with is decided when it is created and does not change afterwards: the ring flags, whether its listeners steer connections, which CPU it runs on. There is no automatic mode that detects and decides, and no setter that changes a running runtime. Detection exists, in `slopio::machine`, but it only reports; the application reads it and decides what to pass. At startup a topology prints what it resolved, so the decision is visible in the logs, see [topology](topology.md).

## Testing

- **Real-ring tests** live in each crate's `tests/` directory and run against the kernel. They are the default.
- **Simulated suites** run the real driver, slabs and state machines over `FakeRing`, a scripted kernel with virtual time, see [deterministic testing](deterministic-testing.md). They need the `simulation` feature: each declares `required-features = ["simulation"]` in its manifest and is listed in `MIRI_SUITES` in the `justfile`, which `just test-sim` and CI run. The same suites run under Miri; the flags are in each file's header.
- **Inline `#[cfg(test)]` modules** are for code that is not async: parsing, placement, formatting. Code that reads the machine takes its root directories as parameters, so its tests read recorded `/sys` and `/proc` trees.

Test binaries run in parallel sometimes fail to build a ring. Before blaming a change, run the failing binary on its own.

## Code style

### Design rules

- **No hidden copy, no hidden allocation.** Each call says which buffer it uses and who owns it afterwards. A claim about cost comes with a measurement: instructions per request, allocations counted, requests per second over interleaved runs.
- **Lifecycles are explicit state machines.** An operation, a task or a multishot stream is an enum whose variants carry exactly what is valid in that state: a waker once the operation is sent, the result and the waker while a zero-copy send waits for the kernel to let go of its buffer, the memory the kernel may still read and the descriptor it may still use once the operation is cancelled. A state that cannot happen cannot be written, and each transition is a `match` the compiler checks is exhaustive. That makes them easy to verify, one transition at a time, by reading and by driving them in the simulated suites, and safe by construction: memory the kernel may still touch belongs to the state that waits for it, so nothing can free it early. `SingleOpState` and `MultiOpState` in the I/O driver, the task's `State`, and `Continuation`, which says whether a multishot operation that ended is worth re-arming, are the main ones.
- **Typed over erased when the set is bounded.** When the types involved are few and known, such as the buffer rings a runtime starts with, propagate them as generics, up to a tuple of twelve, rather than erase them behind function pointers or `dyn`. The extra monomorphization is accepted.
- **Primitives do not wait.** A pooled resource offers a fallible take and counters of what is in use. It never offers an async acquire: when to wait, retry or shed load is the caller's decision.
- **No bookkeeping on the hot path for a cold one.** No per-task structure, header field or per-spawn registration to serve shutdown or debugging.
- **No defaults for where to connect.** Addresses and endpoints are arguments to a constructor, never a default value or a global setter with a fallback.
- **An invariant broken is a panic.** Release and debug builds use `panic = "abort"`: a broken invariant ends the process instead of corrupting the slab or the run queue, and a panicking worker takes the whole topology down rather than leave its peers sending to it.

### Writing the code

- **Comments are terse.** One line, saying the invariant or the reason, not narrating the code. Longer explanations go in doc comments or in the docs.
- **Doc comments say what a caller gets**: what the item does, what it guarantees, what it costs.
- **`unsafe` says why.** A new `unsafe` block carries a `// SAFETY:` comment where it asserts its condition, and an `unsafe fn` documents the contract its caller takes on.
- **Tests are named as sentences** about behaviour: `a_read_dropped_in_flight_leaves_nothing_behind`, `fewer_queues_than_steering_cpus_starve_the_others`.
- **Formatting** is `cargo fmt`; the edition is 2024.

### Documents and commits

- **The documents in `docs/` are for users.** They describe the ways to use the runtime and what can be chosen, in prose, without slab or completion-queue internals. This document is the exception.
- **A commit does one thing.** Its title is an imperative sentence saying what changes, its body says why, in prose, with the numbers when it is about cost.
