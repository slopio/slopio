# Potential improvements

Ideas that are worked out but not implemented. They share one theme: making the runtime's one-thread-one-ring model coincide with the machine's one-queue-one-core-one-memory model, all the way down. Each trades CPU, operational constraints or portability for latency, and on most hardware the trade is not worth making. They would ship as options, off unless asked for, and each comes with the measurement that says whether to turn it on.

## Aligning NIC queues, cores and rings

The runtime pins one worker and one ring per core. A modern NIC has one receive queue per core, each with its own interrupt. Nothing today ensures that the connections a worker serves are the ones whose packets arrive on that worker's core: they land on a worker chosen by the kernel's reuseport hash, which has no relation to the queue that received them. Every packet then crosses cores, decoded on one CPU and consumed on another, with a cross-CPU wakeup in between.

The option would make three links line up:

- **Queue to core.** Interrupt affinity, one queue's IRQ per CPU, and XPS for the transmit side. System configuration rather than runtime code, but the runtime can verify it and refuse to enable the option when it is absent.
- **Core to worker.** Already done by `thread_per_core`.
- **Connection to worker.** For TCP, the reuseport eBPF program selects the listener of the current CPU at SYN time, hash as fallback. For QUIC, the worker index encoded in the connection ID is chosen from the CPU that received the Initial, so the CID keeps every later datagram on the same core, migration included.

A per-worker counter of misplaced connections, from `SO_INCOMING_CPU` on each accepted socket, would report in telemetry whether the alignment holds or whether `irqbalance` has undone it.

**Pays when:** eight or more cores, many connections with small messages, tail latency matters. **Does not pay when:** few cores, few long-lived bulk connections, or when the instance's network allowance saturates first, which is the case on small cloud instances. With fewer queues than cores, only the cores that own a queue should run workers.

## NAPI busy polling on the ring

With the alignment in place, one seam remains: the kernel's softirq, triggered by the interrupt, drains the queue and preempts the worker to do so. `IORING_REGISTER_NAPI` (kernel 6.7, static tracking in 6.13) lets the ring poll its queue itself for a bounded time before sleeping. The worker becomes the thread that empties its NIC queue: no interrupt, no softirq, no wakeup on the hot path, receive latency down to a few microseconds.

The option would register NAPI on each worker's ring at startup, in static mode with the NAPI id of the core's queue, and expose the busy-poll budget. `prefer_busy_poll` needs two per-interface sysfs settings, `napi_defer_hard_irqs` and `gro_flush_timeout`, which the runtime should check and document but not set.

**Pays when:** cores are dedicated to workers, load is sustained at tens of thousands of packets per second per core, and the target is tail latency. **Does not pay when:** traffic is idle or bursty, cores are shared, or the process runs under a CPU quota or on a burstable instance, where spinning burns credits and triggers throttling that costs far more latency than it saves. Never on `t4g`, never under a Kubernetes CPU limit.

## Hardware layout: cpuset and SMT

A small module reading sysfs at startup, no libnuma, no hwloc: the allowed CPU mask from `sched_getaffinity`, SMT siblings from `thread_siblings_list`, node per CPU from `/sys/devices/system/node`, the NIC's node from its device entry.

Workers and [topology](topology.md) threads are already placed on the allowed CPU mask, so a container restricted to CPUs `4-7` works. One use left that does not depend on NUMA:

- **Skip SMT siblings** on x86, where a vCPU is one of two hardware threads sharing a core. One worker per physical core, not per thread, so two workers never compete for the same execution units. Irrelevant on ARM, where a vCPU is a core.

## NUMA

On a machine with two sockets, each socket has its own memory. A core reads its local memory fast and the other socket's memory two to three times slower, across the interconnect. Cloud instances that span a full server expose this, `.24xlarge` and up and `.metal` on x86, the largest Graviton4 sizes on ARM. Everything smaller is a single node and none of this applies.

The runtime is already NUMA-friendly by accident: it pins before it allocates, so first-touch places each worker's ring, buffer rings and pools on its own node. What is missing turns that default into a design:

- **Node-major placement.** Order workers node by node, so that worker ids shard by node. An application that partitions by worker id then partitions by node without knowing it, and `WorkerContext` gains `node` and the list of local peers.
- **Bound memory.** `mbind(MPOL_BIND)` on the provided buffer rings and the fixed send pool right after `mmap`, so their placement is guaranteed rather than inherited from first-touch, and immune to the kernel's automatic NUMA balancing migrating pages.
- **NIC locality.** The NIC sits on one node. Combined with the queue alignment above, connections are served on the node whose queues receive them; without it, half the workers pay the interconnect on every packet.
- **Messaging.** `MSG_RING` between sockets crosses the interconnect. The runtime cannot choose who talks to whom, but exposing which peers are local lets the application prefer them.

The remaining slow drift to watch: memory freed by a worker other than the one that allocated it, which is what happens when a message is dropped by its receiver, returns to the receiver's heap. Over time some allocations end up remote. That is measured with `numa_maps` before it is worth fixing.

**Pays when:** two-socket machines running a network service at high rate. **Does not pay when:** single-socket, which is every small and medium instance and all of ARM up to the largest sizes. Testing needs either such a machine or a fake topology: the `numa=fake=2` kernel parameter on x86, or a QEMU guest with two `-numa` nodes.

## Allocator tuning

The global allocator is mimalloc, with per-thread heaps, which is what makes allocation lock-free in a thread-per-core process. Three of its environment variables are worth trying on a dedicated machine, none of them is a default:

```sh
MIMALLOC_LARGE_OS_PAGES=1   # back heaps with 2 MiB pages: fewer TLB misses on large buffer pools
MIMALLOC_PAGE_RESET=0       # never return freed pages to the OS: no page faults on the next burst
MIMALLOC_EAGER_COMMIT=1     # commit memory up front instead of on first touch
```

All three trade memory for latency predictability. Large pages need hugepages available on the host and pay off mainly on the fixed send pool and buffer rings. Disabling page reset is what you want when the process owns the machine, and what you do not want under a memory limit.

**Pays when:** dedicated hosts, latency targets, large pools. **Does not pay when:** memory is constrained, or the process shares the machine.

## Buffer alignment

Buffers are allocated with the allocator's default alignment. Two reasons to align them on 64 bytes, one cache line:

- **Vectorisation.** Parsers and copies over a buffer that starts on a cache line boundary let the compiler use aligned wide loads without a scalar prologue. On payloads of a few hundred bytes the prologue is a measurable share.
- **No false sharing at the edges.** Two buffers that share a cache line at their boundary, each written by a different worker, make that line bounce between cores. Aligning every buffer to 64 bytes means no line ever belongs to two buffers.

This applies to the provided buffer rings, where the buffer size is already a power of two but the base address is whatever `mmap` returned, and to the fixed send pool. For anything shared across workers, the messaging headers in particular, 64 is the minimum and 128 is safer on Intel, whose prefetcher pulls lines in pairs.

## Linked write operations

io_uring can chain submissions with `IOSQE_IO_LINK`: the next entry only starts once the previous one has completed successfully, and a failure cancels the rest of the chain. Two uses for the runtime:

- **Ordered writes on one socket** without a round trip through the task between them. A response made of several buffers today is either a vectored send or a sequence of awaited sends. A linked chain submits all of them at once, in order, and the task is woken once.
- **Write then close**, or **write then shutdown**, as one submission: the close is guaranteed to run after the write has completed, without the task having to wait for the write first.

The design question is the API: a chain is only safe if every buffer in it stays alive until the last completion, which the current single-op ownership model already handles per operation but not per chain. A `LinkedSend` builder that owns all its buffers and hands them back together is the natural shape. The unused `LinkedSent` variant in the driver is the remnant of a first attempt.

## Smaller items

- **Graceful shutdown.** The shutdown flag is observed by `repeat` and `retry`, but a task parked on I/O at exit is leaked, and nothing drains in-flight connections. A proper design needs a per-worker drain phase, probably driven by a `MSG_RING` broadcast, before the rings are torn down.
- **Timeouts on socket operations.** Today a deadline is `select` against a `Sleep`. io_uring has linked timeouts, `IORING_OP_LINK_TIMEOUT`, which cancel the operation in the kernel without a second future. Cheaper, and cancellation-safe by construction.
- **N-ary `select` and `join`.** Both are binary today. A macro or a const-generic version over a tuple of futures, following the same rule as the existing combinators: re-read readiness on every poll, never count wakeups.
- **Incremental buffer consumption.** `IORING_CQE_F_BUF_MORE` (kernel 6.12) lets a multishot receive fill a large ring buffer across several completions instead of consuming one buffer per completion. Fewer, larger buffers for streaming workloads, at the cost of tracking the offset within a buffer.
- **NUMA monitoring.** A `numastat`-style per-node view of the process's memory in telemetry, once NUMA placement exists to be monitored.

## The order and the measurements

Each step only makes sense once the previous one holds, and each has a number that says whether to take it:

1. Cpuset-aware pinning. No measurement needed, it fixes a crash.
2. Queue alignment. Cross-CPU interrupts, the `RES` line in `/proc/interrupts`, rising with connection count.
3. NAPI busy polling. Interrupt rate per core and worker idle time from `mpstat`.
4. NUMA placement. `numa_maps` and `numastat` for the process, `node-load-misses` from `perf`.

Measure first, enable second, per deployment, never by default.
