# TODO

What is left from the 2026-09-23 audit and the review of its fixes (2026-09-24).
Line numbers are as of commit 505fc55 (branch `timewheel-hlist`).

## Regressions from the audit fixes

Found by the review of commits a9a5d14..505fc55. None is high severity.

- [ ] **Channel wait lists search linearly.** `WaitList::park` and `leave`
  (`slopio-core/src/channels/wait_list.rs`) scan the whole list for an id, so
  with N parked futures every message costs O(N), O(N²) for a burst; the old
  single-waker code was O(1). Ids are handed out increasing and removals keep
  the order, so the list stays sorted: binary search, or answer `Woken` at once
  when the list is empty or the id is below the front one.
- [x] **`detach` takes `&mut` over a task whose future may be mid-poll.**
  `typed_task.rs:48` does `self.inner.as_mut().waker.waker.take()`, a second
  `&mut` over the future's bytes when a task drops its own `Handle` while it
  runs (or `reap` drops it). Stacked Borrows UB, Miri-visible. Reach the tail
  through a raw pointer (`addr_of_mut!`), like `wake_by_ref` does.
- [ ] **`allowed_cpus` panics above 1024 CPUs.** `slopio/src/lib.rs`
  passes a fixed `cpu_set_t` to `sched_getaffinity`, which returns `EINVAL` when
  the kernel has more possible CPUs than `CPU_SETSIZE`; `thread_per_core` then
  panics before any worker starts, where it used to work. Retry with a larger
  heap mask on `EINVAL`, or fall back to `0..available_parallelism`.
- [ ] **File and pipe fds close at the next submit, not at drop.** Since
  `FdInner` (`io_driver/operations/fd.rs`), dropping an `OpenedFile` or
  `PipeFd` only queues the close. A task that calls `close_stdin()` and then
  blocks synchronously (a blocking `waitpid`, a std channel) never lets it go
  out: the child never sees EOF. Same for releasing a `flock`. Async code is
  unaffected. Decide: document it, or close synchronously when no op holds the
  fd (strong count 1), at least in `close_stdin`.
- [ ] **Stale doc line on `PeerMessage`.** `completion_events.rs:20` still
  carries the removed `SendBuffer` variant's comment. Delete it.
- [ ] **UDP multishot header grew from 32 to 44 bytes per buffer**
  (`msg_namelen` = `sizeof(sockaddr_in6)`, `udp/multi_recv.rs`). A datagram
  that exactly filled a buffer now comes back truncated (flagged), and a ring
  with buffers under 44 bytes ends the stream with `EFAULT`. Document the
  minimum, or `const`-assert `S::BUFFER_SIZE` in `UdpMultiRecv`.
- [ ] **`send_control_record` keeps the payload.** It owns `B: IoBuf` but its
  output is `io::Result<usize>`, unlike `socket_send`, which hands the buffer
  back. Return `(io::Result<usize>, B)` if callers pool their buffers.

## Pre-existing bugs found during the review

- [ ] **`Process::wait` does not close stdin before waiting**
  (`processes/spawn.rs`): it binds `..`, so `stdin` stays in the partly moved
  `self` and drops only after the `await`. `wait()` on `cat` without
  `close_stdin()` hangs, although the doc says it sends EOF.
- [ ] **`OpenedFile::fstat` double-closes on error** (`files/mod.rs`): if
  `metadata()?` fails, the temporary `File` closes the fd, then `FdInner`
  closes it again.

## Open audit items

### Structural
- [ ] **Clean shutdown (A5 + C1).** `IODriver` drops the ring before the slab,
  so keepalive buffers are freed while an io-wq op (a buffered file read) may
  still write into them; and tasks still parked when `block_on` returns leak
  (repro: ignored test `a_task_still_parked_on_a_read_leaks_it`). Cancel every
  in-flight op and drain the CQ before closing, reaching parked tasks through
  the wakers the driver already holds. No per-task registry.
  Part done: `block_on` now waits out ops already cancelled (their keepalive
  and parked fd) before returning, found by `dst_echo`. Still open: ops of
  tasks still parked, which nobody has cancelled.

### Bugs
- [x] **B1** `submit` / `submit_and_wait` panic on `EAGAIN` / `EBUSY`, which
  are transient: drain the CQ and retry.
- [ ] **B2** `has_deferred_completions` ignores `IORING_SQ_CQ_OVERFLOW`, so
  overflowed CQEs wait until the task queue empties (latency, not loss).
  Consider a larger `setup_cqsize`.
- [ ] **B3** A buffer ring installs once per thread: a second
  `main_thread_with` on the same thread panics. Clear the cell when the
  driver drops.
- [ ] **B4** `main_thread` called from inside a task is UB (outer wakers push
  onto the inner task list). Assert `TASK_LIST` is unset in `block_on`.
- [ ] **B5** A detached, running task with no live waker (`spawn(pending())`)
  is never freed.
- [ ] **B6** `Process::from_command` spawns an OS thread for nothing, and a
  `Process` dropped without `wait()` leaves a zombie.
- [ ] **B7** `send` / `push` on an unbounded or vec channel whose receiver is
  gone drops the item silently. Return a `Result` or `bool`.
- [ ] **B8** eBPF `quic_reuseport.c` returns `SK_DROP` for non-linear skbs
  (GRO) and when no socket is registered: use `SK_PASS` or
  `bpf_skb_load_bytes`.
- [ ] **B9** The eBPF loader always asks for a 64 KB verifier log; a longer
  log fails a valid load with `ENOSPC`. Load without a log, retry with one on
  failure.
- [ ] **B10** Loader robustness: hard-coded `.maps` offsets, a slice index
  that can panic on a malformed ELF, program attached before every socket is
  registered.
- [ ] **B11** `id_generation.rs`: panics if `cid_len < 4`; with `cid_len == 4`
  the 24-bit counter makes CIDs sequential and repeating (linkable).
- [ ] **B12** `Sleep` docs: say it can fire up to ~10 ms early (a tick is taken
  off at arm) and that the duration counts from the first poll.

### Performance
- [ ] **D4** Vectored sends allocate a `Box<VectoredMsg>` and a `Vec<iovec>`
  per submission, again on every short write: inline a small iovec array,
  reuse it across `send_all`.
- [ ] **D5** UDP send/recv and `send_control_record` box their state per op:
  pool per socket, or keep it inline.
- [ ] **D11** Set `IOSQE_CQE_SKIP_SUCCESS` on MSG_RING sends: one CQE fewer per
  message. Failures stay ignored (callers use timeouts).
- [ ] **D12** The mid-pass flush in `Ring::add` skips `refill_buf_rings`, so an
  inline-issued recv can see `ENOBUFS` while the reserve still has buffers.
- [ ] **D13** `set_nodelay` on every accepted socket; accepted sockets likely
  inherit it from the listener (check with a test, then set it once).
- [ ] **D14** Dropping a zero-copy send already in `Notif` still pushes an
  AsyncCancel that can only return `ENOENT`.
- [ ] **D15** `send_all_zc` waits for the notification before each short-write
  retry: about one RTT per retry (rare with `MSG_WAITALL`).
- [ ] **`HeaderMap` is quadratic on many distinct names**
  (`slopio-http/src/header/map.rs`). It is a flat `Vec` scanned per lookup and
  per append, so a peer sending many names makes decoding O(n²). Measured in
  release: 2000 distinct appends 3.6 ms, 2000 lookups in a 2000-entry map
  7.3 ms. Under h2's default 16 KiB header list (≤ ~480 fields) the worst case
  is ~0.2 ms per request; a server raising the limit to 64 KiB gets ~3-4 ms.
  Fix: keep the `Vec` and its runs, add a hash index (name → positions) built
  only past ~32 entries, so small maps keep the scan.

## Performance notes

TCP hot path allocations, measured 2026-09-11 with a counting global
allocator on the runtime thread only (harness not kept). Steady-state echo on
an established connection is 0 alloc / 0 B per round-trip. What is left costs
per connection or per timeout:

- [ ] **Two `MultiOpQueue` allocations per multishot op**, 336 B each
  (`VecDeque::with_capacity(pause_threshold + 10)` × 8 B/event): one in
  `MultiRecv`, one built driver-side at the `Init -> Sent` transition. A full
  connection is 4 allocs / 896 B, `[24, 200, 336, 336]` = `Rc<SocketInner>`,
  task box, then the two queues: 75% of the bytes and half the allocations of
  a short-lived connection. `QUEUE_SIZE` is already a const generic on
  `MultiRecv`, so its queue can be an inline ring; the driver-side one needs
  `MultiOpState` to carry a fixed capacity (or a `SmallVec`). Tradeoff: inline
  in the multishot slab, the memory stays sized to the peak.
- [ ] **A cancelled five-second sleep keeps its entry until its deadline.**
  `Timers::cancel` only clears a bit unless the entry is at the head, so the
  live set is `rate × 5 s`, not the timeouts in flight: a `timeout(5s, ..)`
  per request at 100k req/s is ~500k dead entries. The wheel frees a
  cancelled timer at once since the linked-list rewrite. Fix, if that profile
  matters: sweep dead entries without waiting for the clock. See
  `slopio-timers/DESIGN.md`.
- [ ] **`socket_send_vectored` allocates a `Vec<libc::iovec>` per call**
  (16 B per chunk) on top of the caller's `Vec<B>`, the only send path that
  allocates on the hot path; `send_vectored_fixed_zc` does the same. Fix: a
  reusable per-socket iovec scratch buffer.
- [ ] **A formatted tracing event allocates twice** when its level is
  enabled: `info!("x = {}", val)` builds a `String` with `format!`, then
  `event()` copies it into the span's arena. Fix: an `event_fmt(level,
  fmt::Arguments)` that `write!`s straight into the arena, called by the
  macros.

## Outside this repository

- [ ] popple-h2 and popple-tls must adapt to the API changes: `MultiAccept`
  and `UdpMultiRecv` yield `Result`, `send_control_record<B: IoBuf>` /
  `WriteControlRecord<B>`, `Process` pipes are `PipeFd`,
  `socket_managed_recv` resolves to `Option`, `OpenedFile::fd()`.

## Constraints to keep in mind

- A waker must never cancel a timer from inside its own `wake` or drop: the
  wheel fires while it walks a slot's list. Task wakers only queue the task,
  so this holds today.

## Decided, not to do

- A7: a failed MSG_RING is ignored; callers put timeouts on requests.
- Cross-thread wakes do not exist in this runtime; wakers need no thread
  safety.
- D6: one Timeout SQE per idle wait instead of `EXT_ARG`; not worth reworking
  the simulator.
- D9 was a false positive (`..Default::default()` keeps the default driver).
