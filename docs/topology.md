# Topology

`thread_per_core` runs the same worker on every core, each with the same kind of ring. That covers a server where every core does the same job. A `Topology` covers the rest: threads that do different jobs, each kind on its own CPUs and with its own ring settings, all able to [message each other](messaging.md).

A typical case is a database client. The connection handling runs one worker per core, while the database traffic goes through two threads of its own whose rings poll their submission queue, so issuing a query never costs a syscall.

## Declaring threads

A topology is a list of groups. A group is a set of threads running the same closure, with the same [provided buffer rings](io-model.md) and the same options. On top of the groups, you can add one worker per remaining core with `per_core`. Then `run` starts everything and blocks until every thread has returned.

```rust
use runtime::{NetOptions, RingOptions, RuntimeOptions, Threads, Topology};

let mut topology = Topology::new();

let db = topology.group::<DbRings, _, _>(
    Threads::Auto(2),
    RingOptions { sqpoll: true, defer_taskrun: false, coop_taskrun: false, ..Default::default() },
    |ctx| db_loop(ctx),
);

let http = RuntimeOptions { net: NetOptions { steer_connections: true }, ..Default::default() };
topology.per_core::<HttpRings, _, _>(http, move |ctx| serve(ctx, db.clone()));

topology.run();
```

The closure is called once on each thread of the group, on that thread once its runtime is up, and returns the future the thread runs. Like under `thread_per_core`, the future does not need to be `Send`; only the closure does. `per_core` can be called once. Groups can be added in any number.

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

## Connections to the CPU that receives them

A group built with `net.steer_connections` binds its listeners with `SO_INCOMING_CPU` set to each thread's own CPU. Several threads listening on one port share it through `SO_REUSEPORT`, and the kernel then hands each new connection to the listener of the CPU that received its first packet, rather than to one its hash picks. When no listener's CPU matches, the hash decides, as without the option. UDP sockets bound by the same threads carry it too, and the kernel picks among them the same way for each datagram.

The CPU that receives a connection's packets is the one its NIC queue interrupts. With one queue per CPU, each queue's interrupt on the CPU of a thread, and XPS sending from each CPU through that same queue, a connection's packets are decoded by the kernel on the core whose thread serves it: no wakeup from another core, nothing to fetch from another core's cache.

Interrupt affinity and XPS are the system's to configure, and `irqbalance` undoes them. With the queues' interrupts away from the threads, the option changes nothing. With fewer queues than threads, it is worse than nothing: the threads whose CPU takes no interrupt get almost no connections. It is therefore off by default, and decided when the group is declared, by whoever knows where the machine's queues are. It needs a thread pinned to a CPU: `main_thread_with_options` refuses it.

When it starts, `run` prints what it resolved to stdout, in one block before any thread starts: each group and the per-core workers with their thread ids, the CPU of each thread and of each SQ thread, and their ring and net options. It adds where the NICs' packets are processed, read from `/sys` and `/proc`, and, when some group steers, whether each steering CPU processes packets or what is in the way. That can be a CPU with no queue, a queue on a CPU outside the steering threads, an interrupt allowed on several CPUs, RPS, RFS, threaded NAPI, or `irqbalance` running. It needs no privilege and changes nothing: the block is for whoever reads the logs.

`runtime::topology::Machine::read()` gives the same reading to the application, to decide where to put its threads: the CPUs the process may use, the isolated and `nohz_full` ones, the cores with their SMT siblings, the NUMA nodes, each NIC with its receive queues, the CPU of each queue's interrupt and its RPS CPUs, and whether RFS or `irqbalance` are in the way. `packet_cpus()` sums it up as the CPUs where the NICs' packets are processed: a queue's RPS CPUs when RPS is on, its interrupt's CPU otherwise. It only reads; nothing in the topology uses it to place a thread. `cargo run --example machine` prints it.

While it runs, each steering thread counts the connections it accepts, and how many of them the kernel received on the thread's own CPU. `runtime::net::steering_stats()` returns the thread's two counts; export them the way the application exports its other metrics. A share well below one means the alignment does not hold, or no longer holds.

## Options

Each group, and `per_core`, gets a `RuntimeOptions`: `ring`, the ring's setup, and `net`, how the thread's sockets are set up. A plain `RingOptions` is accepted where a `RuntimeOptions` is expected, with the default `net`.

| `net` field | Default | Effect |
|-------------|---------|--------|
| `steer_connections` | `false` | `SO_INCOMING_CPU` set to the thread's CPU on every listener and UDP socket it binds, see above. |

The options are fixed once the thread starts. Code running on it reads them with `runtime::thread_options()`: its peer id, the CPU it is pinned to, and the options it was built with.

The `ring` fields are plain switches over the io_uring setup flags:

| Field | Default | Effect |
|-------|---------|--------|
| `defer_taskrun` | `true` | The kernel finishes operations only when the thread asks for completions, never in the middle of its work. |
| `coop_taskrun` | `true` | The kernel does not interrupt the thread to finish an operation. It waits for the thread's next system call of any kind. |
| `sqpoll` | `false` | A kernel thread picks up submissions by itself, so submitting I/O needs no system call while it is awake. |
| `sqpoll_idle` | 1 s | How long that kernel thread keeps polling after the last submission before it goes to sleep. Only read with `sqpoll`. |
| `napi_busy_poll` | `None` | NAPI busy polling (Linux 6.9): while the thread waits for completions, it polls the NIC queues its sockets' packets arrive on for up to this long, instead of sleeping until an interrupt. |
| `napi_prefer_busy_poll` | `false` | With `napi_busy_poll`, the kernel defers the queue's interrupts while the thread polls it. Needs the interface's `napi_defer_hard_irqs` and `gro_flush_timeout` set. |

The defaults suit almost every thread: completions are handled in batches, at the moment the thread is ready for them. Turn `defer_taskrun` off and operations are finished at any system call the thread makes. Turn both off and the kernel interrupts the thread whenever an operation completes.

`sqpoll` suits a thread that submits constantly, a database or a log shipper, where the syscall per submission is the cost to remove. It is not free:

- **It takes a second CPU.** The kernel's polling thread is pinned to a CPU of its own, chosen like an `Auto` thread. A group of 2 threads with `sqpoll` takes 4 CPUs. Under `per_core`, each worker takes two CPUs, so there are half as many workers.
- **That CPU spins.** It stays at 100% for `sqpoll_idle` after every submission. On a quota-bound or burstable machine, keep `sqpoll_idle` short or leave `sqpoll` off.
- **The first submission after a sleep pays a wakeup**, which costs a system call.

`napi_busy_poll` turns waiting into spinning: a thread with nothing to do keeps its core busy for up to the budget each time it waits. It pays on cores given to the workers, under sustained network load, and costs on shared cores, under a CPU quota or on a burstable instance. It is worth most with one NIC queue per CPU and connections steered to the CPU that receives them (below), so that each thread polls its own core's queue.

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
