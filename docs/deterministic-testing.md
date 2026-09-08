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
let socket = simulated_socket().unwrap();

handle.on_read(socket.fd(), ReadEvent::data(b"hello"));

let (result, buf) = RuntimeInner::simulated(fake).block_on::<(), _>(
    async move { socket_recv(&socket, vec![0u8; 32]).await },
    None,
    |run| run(),
);

assert_eq!(&buf[..result.unwrap()], b"hello");
assert_eq!(handle.scenario_remaining(), 0);
```

The ring is consumed by the runtime; the handle stays with the test to script events beforehand and inspect results afterwards. Sockets come from `simulated_socket()`, which gives a real but unconnected TCP descriptor, so everything the runtime does to a socket outside the ring still works and every fd you see is one that genuinely exists.

## What you can script

Scripts are keyed by what an operation is, not by when it is submitted: a read on this descriptor, an accept on that listener. Responses queue per descriptor and are consumed in order.

- **Reads**: `ReadEvent::data(bytes)`, `Eof`, `Bytes(n)` for a count without caring about contents, `Error(errno)`. A `socket_peek` sees the next read without consuming it: the same data or EOF is delivered again to the read that follows, immediately, as a kernel would from its receive queue. An error is consumed by a peek, as it is by a kernel.
- **Writes**: `WriteEvent::All`, `Short(n)`, `Error(errno)`. Unscripted writes succeed in full. The handle records what each descriptor was sent, so you can assert on `handle.writes(fd)`.
- **Accepts**: `AcceptEvent::Connection` mints a new socket, `On(fd)` hands back one you created in advance so you can script what it then does, `Error(errno)`.
- **Connects**: `ConnectEvent::Ok` or `Error(errno)`.
- **Multishot streams** are declared whole with `on_multishot_read` and `on_multishot_accept`: the full sequence the operation will produce, in order. A sequence ending on EOF or an error closes the stream; an open one leaves it armed.

Errnos are given positive. Operations the kernel finishes on its own, such as sends, writes and opens, complete without being scripted. Operations that wait on the outside world, such as reads, accepts and connects, never complete unless scripted.

## Choosing when things happen

By default a completion lands as soon as the runtime next waits on the ring, in submission order. Nothing invents latency. When a test needs an interleaving, it says so:

- **`.after(duration)`** holds a completion back. This is how you write "the read lands after the timeout" or "the second chunk arrives ten seconds later".
- **`.after_between(min, max)`** draws the delay from a range. This is the only randomness in the simulator, it is only used where a test asks for it, and it is fixed by the seed given to the builder, so a failing run can be replayed exactly.
- **Batches.** Completions declared together in one `vec![...]` inside a multishot sequence land in the same drain of the completion queue, so the task sees them without going back to the event loop. Completions declared apart land in separate drains, with the program making progress in between. The difference is observable and worth stating rather than leaving to chance.

## Virtual time

Build the ring with `.virtual_time()` and the timing wheel reads the same virtual clock as the simulated ring. A sleep is a jump: a test that sleeps for an hour finishes in microseconds, and timers and completions fire in an order fully determined by the script. Without `.virtual_time()` the simulator sleeps for real on the monotonic clock, which is still deterministic but slow.

## What it is good for

The simulator is for the paths a real kernel makes rare or unreproducible: a read cancelled while its data is in flight, a stream dropped mid-batch, a ring that runs out of buffers, a short write followed by a broken pipe, a timeout that wins by one tick. The existing suites under `runtime-core/tests/` cover cancellation, buffer recycling and the runtime's event loop this way, and are the best place to look for patterns.

It also makes the runtime runnable under Miri, which a real io_uring never can be. The exact flags are in the header of each test file that supports it.

## Limits

- Sends never block and never see backpressure. The simulator models the completion, not the network.
- There is no simulated peer. Data appears because you scripted it, not because someone sent it.
- The simulator is reachable from `runtime_core` only. The `runtime` facade's entry points always build a real ring.
