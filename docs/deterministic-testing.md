# Deterministic simulation testing

The runtime can run against a simulated io_uring instead of the kernel. The driver, the tasks, the wakers, the timers and the buffer rings all run unmodified; only the ring underneath is replaced by one whose completions a test declares in advance. A test therefore exercises the real runtime against a kernel it controls, and the same test produces the same run every time.

This is behind the `simulation` feature and lives in `runtime_core::deterministic_testing`. It is a test tool, not a production mode: with the feature off, the simulator is not compiled.

## Writing a test

A simulated test has four steps: build a ring, script what the world does, run a future on the runtime, assert.

```rust
use runtime_core::deterministic_testing::{FakeRing, ReadEvent, simulated_socket};
use runtime_core::io_driver::operations::sockets::recv::socket_recv;
use runtime_core::runtime::RuntimeInner;

let (fake, handle) = FakeRing::builder().virtual_time().build();
let socket = simulated_socket();

handle.on_read(socket.fd(), ReadEvent::data(b"hello"));

let (result, buf) = RuntimeInner::simulated(fake).block_on::<(), _>(
    async move { socket_recv(&socket, vec![0u8; 32]).await },
    None,
    |run| run(),
);

assert_eq!(&buf[..result.unwrap()], b"hello");
assert_eq!(handle.scenario_remaining(), 0);
```

The ring is consumed by the runtime; the handle stays with the test to script events beforehand and inspect results afterwards. Sockets come from `simulated_socket()`, which gives an unconnected TCP descriptor invented by the simulator. Nothing about it reaches the kernel: socket options, local addresses, shutdown and close are answered by the simulator, so everything the runtime does to a socket outside the ring still works. Descriptors the ring hands out itself, on an accept or an open, are listed by `handle.opened_fds()`; check each with `is_open` at the end of a test to catch a leak.

## What you can script

Scripts are keyed by what an operation is, not by when it is submitted: a read on this descriptor, an accept on that listener. Responses queue per descriptor and are consumed in order.

- **Reads**: `ReadEvent::data(bytes)`, `Eof`, `Bytes(n)` for a count without caring about contents, `Error(errno)`. A `socket_peek` sees the next read without consuming it: the same data or EOF is delivered again to the read that follows, immediately, as a kernel would from its receive queue. An error is consumed by a peek, as it is by a kernel.
- **Writes**: `WriteEvent::All`, `Short(n)`, `Error(errno)`. Unscripted writes succeed in full. The handle records what each descriptor was sent, so you can assert on `handle.writes(fd)`; `handle.reads(fd)` is the other direction, every chunk a read was handed.
- **Accepts**: `AcceptEvent::Connection` mints a new socket, `On(fd)` hands back one you created in advance so you can script what it then does, `Error(errno)`.
- **Connects**: `ConnectEvent::Ok` or `Error(errno)`.
- **Multishot streams** are declared whole with `on_multishot_read` and `on_multishot_accept`: the full sequence the operation will produce, in order. A sequence ending on EOF or an error closes the stream; an open one leaves it armed.
- **Flows**: inside a multishot read, `ReadEvent::flow(bytes)` delivers bytes in order, split across as many completions as the seed draws, each no larger than the buffer it lands in, sometimes several in one drain. `.spaced(min, max)` holds each later drain back by a delay drawn from that range, while chunks drawn into the same drain still land together. A single byte and a full buffer come up often, since that is where readers get lengths wrong. If the ring runs out of buffers partway, the bytes not yet delivered wait for the re-armed read, as a socket would keep them. `.cut(Cut::Reset)` or `.cut(Cut::Timeout)` kills the connection partway through: the bytes up to an offset drawn by the seed arrive, the next read fails with `ECONNRESET` or `ETIMEDOUT`, and the connection is reset from then on. A flow on a single-shot read is refused.

Errnos are given positive. Operations the kernel finishes on its own, such as sends, writes and opens, complete without being scripted. Operations that wait on the outside world, such as reads, accepts and connects, never complete unless scripted.

A simulated socket also remembers what its far end did, so a connection that ended stays ended. Once a read on it reports EOF, the peer has closed its side: every later read sees EOF, while writes still go through. Once a read or a write reports `ECONNRESET`, `ETIMEDOUT` or `EPIPE`, the connection is reset: later reads see EOF and later writes `EPIPE`. This matches what Linux does after the reset has been reported once. Either way, operations already waiting complete from that state, and whatever the script still said about the dead side is dropped and counted by `handle.voided()` rather than left owed. `handle.peer(fd)` tells you the state. Files have no far end: a read of zero bytes is just the end of the file.

## Choosing when things happen

By default a completion lands as soon as the runtime next waits on the ring, in submission order. Nothing invents latency. When a test needs an interleaving, it says so:

- **`.after(duration)`** holds a completion back. This is how you write "the read lands after the timeout" or "the second chunk arrives ten seconds later".
- **`.after_between(min, max)`** draws the delay from a range. Along with how a flow splits and spaces its drains, this is the only randomness in the simulator. Both happen only where a test asks for them, and both are fixed by the seed given to the builder, so a failing run can be replayed exactly.
- **Batches.** Completions declared together in one `vec![...]` inside a multishot sequence land in the same drain of the completion queue, so the task sees them without going back to the event loop. Completions declared apart land in separate drains, with the program making progress in between. The difference is observable and worth stating rather than leaving to chance.

## Virtual time

Build the ring with `.virtual_time()` and the timer stores read the same virtual clock as the simulated ring. A sleep is a jump: a test that sleeps for five seconds finishes in microseconds, and timers and completions fire in an order fully determined by the script. Without `.virtual_time()` the simulator sleeps for real on the monotonic clock, which is still deterministic but slow.

Time only moves when the program blocks. A program that spins, polling again at once instead of waiting, keeps virtual time where it is, so nothing due later ever lands. The ring treats a hundred thousand waits in a row at one instant as that, and panics instead of hanging.

## Running a scenario many times

A test can build and run several simulated runtimes one after another on the same thread, for instance one per seed. Each run starts from the same state: building a ring restarts descriptor numbering, and a run hands its buffer rings back as they were built. In exchange, a run must give back what it took. Building a ring while a simulated descriptor is still open panics, and so does ending a run while a buffer from a ring is still held, such as one returned out of `block_on`.

## What it is good for

The simulator is for the paths a real kernel makes rare or unreproducible: a read cancelled while its data is in flight, a stream dropped mid-batch, a ring that runs out of buffers, a short write followed by a broken pipe, a timeout that wins by one tick. The existing suites under `runtime-core/tests/` cover cancellation, buffer recycling and the runtime's event loop this way, and are the best place to look for patterns.

It also makes the runtime runnable under Miri, which a real io_uring never can be. `just test-sim` runs every simulated suite natively, `just miri` runs them under Miri, and `just miri-one <suite> [filter]` runs one; the flags and the list of suites live in the `justfile`.

## Limits

- Sends never block and never see backpressure. The simulator models the completion, not the network.
- The far end is only a state, closed or reset, not a simulated program. Data appears because you scripted it, not because someone sent it.
- The simulator is reachable from `runtime_core` only. The `runtime` facade's entry points always build a real ring.
