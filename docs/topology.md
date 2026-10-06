# Topology

`thread_per_core` runs the same worker on every core, each with the same kind of ring. That covers a server where every core does the same job. A `Topology` covers the rest: threads that do different jobs, each kind on its own CPUs and with its own ring settings, all able to [message each other](messaging.md).

A typical case is a database client. The connection handling runs one worker per core, while the database traffic goes through two threads of its own whose rings poll their submission queue, so issuing a query never costs a syscall.

## Declaring threads

A topology is a list of groups. A group is a set of threads running the same closure, with the same [provided buffer rings](io-model.md) and the same ring options. On top of the groups, you can add one worker per remaining core with `per_core`. Then `run` starts everything and blocks until every thread has returned.

```rust
use runtime::{RingOptions, Threads, Topology};

let mut topology = Topology::new();

let db = topology.group::<DbRings, _, _>(
    Threads::Auto(2),
    RingOptions { sqpoll: true, defer_taskrun: false, coop_taskrun: false, ..Default::default() },
    |ctx| db_loop(ctx),
);

topology.per_core::<HttpRings, _, _>(RingOptions::default(), move |ctx| serve(ctx, db.clone()));

topology.run();
```

The closure is called once on each thread of the group, on that thread, and returns the future the thread runs. Like under `thread_per_core`, the future does not need to be `Send`; only the closure does. `per_core` can be called once. Groups can be added in any number.

`thread_per_core` is itself a topology with no group and a `per_core` built with the default options.

The `topology` example runs a TCP server whose workers share a counter owned by a dedicated thread:

```sh
cargo run --release -p runtime --example topology -- 8080
nc 127.0.0.1 8080
```

## Where threads run

Every thread is pinned to one CPU of its own. No two threads of a topology ever share a CPU.

- **`Threads::Fixed(vec![2, 3])`** runs one thread on each listed CPU.
- **`Threads::Auto(n)`** runs `n` threads on CPUs nobody asked for by number. They are taken from the top of the CPUs the process may use, which leaves the low CPUs to the per-core workers.
- **`per_core`** gets every CPU left, lowest first. Under a cgroup CPU quota it gets fewer: the quota is shared with the groups, so a 4-CPU quota with 1 CPU taken by a group leaves 3 workers.

Only the CPUs the process may run on count. In a container restricted to `4-7`, `Auto` picks among those four, and `Fixed(vec![2])` is refused.

The placement is checked before any thread starts, and `run` panics with the reason if it cannot be satisfied: a CPU the process may not use, the same CPU asked for twice, more threads than CPUs, or no CPU left for `per_core`.

## Ring options

Each group, and `per_core`, gets a `RingOptions`. The fields are plain switches over the io_uring setup flags:

| Field | Default | Effect |
|-------|---------|--------|
| `defer_taskrun` | `true` | The kernel finishes operations only when the thread asks for completions, never in the middle of its work. |
| `coop_taskrun` | `true` | The kernel does not interrupt the thread to finish an operation. It waits for the thread's next system call of any kind. |
| `sqpoll` | `false` | A kernel thread picks up submissions by itself, so submitting I/O needs no system call while it is awake. |
| `sqpoll_idle` | 1 s | How long that kernel thread keeps polling after the last submission before it goes to sleep. Only read with `sqpoll`. |

The defaults suit almost every thread: completions are handled in batches, at the moment the thread is ready for them. Turn `defer_taskrun` off and operations are finished at any system call the thread makes. Turn both off and the kernel interrupts the thread whenever an operation completes.

`sqpoll` suits a thread that submits constantly, a database or a log shipper, where the syscall per submission is the cost to remove. It is not free:

- **It takes a second CPU.** The kernel's polling thread is pinned to a CPU of its own, chosen like an `Auto` thread. A group of 2 threads with `sqpoll` takes 4 CPUs. Under `per_core`, each worker takes two CPUs, so there are half as many workers.
- **That CPU spins.** It stays at 100% for `sqpoll_idle` after every submission. On a quota-bound or burstable machine, keep `sqpoll_idle` short or leave `sqpoll` off.
- **The first submission after a sleep pays a wakeup**, which costs a system call.

`sqpoll` excludes the other two flags; the kernel refuses the combination. Set `defer_taskrun` and `coop_taskrun` to `false` along with it. Declaring a group or `per_core` with both panics on the spot, before anything starts.

## Addressing threads

Every thread of a topology has a peer id, which is what [messaging](messaging.md) addresses. Ids are handed out in declaration order: the first group gets `0..n`, the next group follows, and the per-core workers come last.

`group` returns the range of ids of the group it declares, so a later closure can capture it and send to the group. Each thread also receives a `WorkerContext`:

| Field | Meaning |
|-------|---------|
| `thread_id` | This thread's peer id. |
| `num_threads` | The number of threads in the topology, groups and workers together. |
| `group` | The peer ids of this thread's own group. |
| `workers` | The peer ids of the per-core workers, empty without `per_core`. |

A thread that answers requests from anywhere does not need to know who asked: [replies](messaging.md#four-ways-to-send) find their way back on their own.

## Telemetry and failures

`Topology::traced(collector)` is the topology counterpart of `thread_per_core_traced`: each thread builds its own collector with the closure, as described in [telemetry](telemetry.md).

A panic on any thread aborts the whole process, as under `thread_per_core`. A topology never keeps running with a thread missing that the others may be sending to.
