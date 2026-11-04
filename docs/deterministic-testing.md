# Deterministic simulation testing

The runtime can run against a simulated io_uring instead of the kernel. The driver, the tasks, the wakers, the timers and the buffer rings all run unmodified; only the ring underneath is replaced by one whose completions a test declares in advance. A test therefore exercises the real runtime against a kernel it controls, and the same test produces the same run every time.

This is behind the `simulation` feature and lives in `slopio::deterministic_testing`. It is a test tool, not a production mode: with the feature off, the simulator is not compiled.

## Writing a test

A simulated test has four steps: build a ring, script what the world does, run a future on the runtime, assert.

```rust
use slopio::deterministic_testing::{FakeRing, ReadEvent, simulated_socket};
use slopio::io_driver::operations::sockets::recv::socket_recv;
use slopio::runtime::RuntimeInner;

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
- **Flows**: inside a multishot read, `ReadEvent::flow(bytes)` delivers bytes in order, split across as many completions as the seed draws, each no larger than the buffer it lands in, sometimes several in one drain. `.spaced(min, max)` holds each later drain back by a delay drawn from that range, while chunks drawn into the same drain still land together. A single byte and a full buffer come up often, since that is where readers get lengths wrong. If the ring runs out of buffers partway, the bytes not yet delivered wait for the re-armed read, as a socket would keep them. `.cut(Cut::Reset)` or `.cut(Cut::Timeout)` kills the connection partway through: the bytes up to an offset drawn by the seed arrive, the next read fails with `ECONNRESET` or `ETIMEDOUT`, and the connection is reset from then on. `.hiccup(Hiccup::Interrupted)` or `.hiccup(Hiccup::Ended)` makes the read stumble at a drawn offset without hurting the connection, with either an `EINTR` or a chunk that arrives without `IORING_CQE_F_MORE`, as when the kernel ends a multishot on its own. Either way the multishot is over, and the rest of the flow waits for the read that re-arms. Call it more than once for more stumbles. A flow on a single-shot read is refused.

Errnos are given positive. Operations the kernel finishes on its own, such as sends, writes and opens, complete without being scripted. Operations that wait on the outside world, such as reads, accepts and connects, never complete unless scripted.

A simulated socket also remembers what its far end did, so a connection that ended stays ended. Once a read on it reports EOF, the peer has closed its side: every later read sees EOF, while writes still go through. Once a read or a write reports `ECONNRESET`, `ETIMEDOUT` or `EPIPE`, the connection is reset: later reads see EOF and later writes `EPIPE`. This matches what Linux does after the reset has been reported once. Either way, operations already waiting complete from that state, and whatever the script still said about the dead side is dropped and counted by `handle.voided()` rather than left owed. `handle.peer(fd)` tells you the state. Files have no far end: a read of zero bytes is just the end of the file.

## Peers written as code

A script is fixed before the run, so it cannot answer what the program does. An actor can. Attach one to a simulated socket and every read and write on that descriptor is asked of it at the moment the completion is posted:

```rust
use slopio::deterministic_testing::{Actor, ActorCx, ReadReply, WriteReply};

struct Echoed { pending: Vec<u8> }

impl Actor for Echoed {
    fn read(&mut self, max: usize, _cx: &mut ActorCx) -> ReadReply {
        if self.pending.is_empty() { return ReadReply::Pending; }
        let n = max.min(self.pending.len());
        ReadReply::Data(self.pending.drain(..n).collect())
    }

    fn write(&mut self, bytes: &[u8], _cx: &mut ActorCx) -> WriteReply {
        self.pending.extend_from_slice(bytes);
        WriteReply::Accepted(bytes.len())
    }
}

handle.attach(socket.fd(), Echoed { pending: Vec::new() });
```

- **`read(max)`** returns `Data` (one byte up to `max`, which is the size of the buffer the read lands in), `Eof`, `Error(errno)`, or `Pending` to make the read wait.
- **`write(bytes)`** returns `Accepted(n)`, where fewer than offered is a short write, `AcceptedAfter(n, delay)` to take the bytes now but report the send only later, `Error(errno)`, or `Pending` to hold the write as a full send buffer would. Sends on one descriptor complete in order whatever their delays, and a send already taken cannot be cancelled: the cancel gets `EALREADY` and the send completes as it was going to.
- **`on_open`** runs when an accept hands the descriptor out, or at its first operation otherwise. **`on_closed`** runs when the program closes it, and nothing the actor does reaches the program after that.
- **Timers**: `cx.timer(after, tag)` calls `on_timer(tag)` later in virtual time; arming a tag again replaces it, and `cx.cancel(tag)` disarms it. `cx.wake_at(at)` asks again for the operations waiting on the actor without a callback, for state that becomes ready on its own.
- **`cx.now()`** is virtual time since the ring was built, and **`cx.random()`** and **`cx.between(lo, hi)`** draw from a stream of the actor's own, settled by the seed.

An operation that got `Pending` waits, and is asked again after the actor's next timer, wake-up or write, so an actor never has to say it is ready. Bytes an actor hands over that no buffer could take, because the ring was dry, are kept and served to the next read, as a socket keeps them. A descriptor is answered either by an actor or by a script, never both: mixing them panics.

Because the run stops on a program that blocks for good, a peer that waits for an answer turns a server that never gives one into a failing test, with no assertion to write.

For the common case of a peer that plays a conversation, `Dialogue` is an actor built from steps:

```rust
use slopio::deterministic_testing::{Check, Dialogue};

let (peer, report) = Dialogue::new()
    .send(b"PING\r\n".to_vec())
    .expect_bytes(b"+PONG\r\n".to_vec())
    .wait(Duration::from_millis(5))
    .send(request)
    .expect("a framed reply", |got: &[u8]| {
        // Everything the program wrote and no step consumed yet, called
        // again each time more arrives.
        match parse_frame(got) {
            None => Check::More,
            Some((frame, len)) if frame.is_ok() => Check::Pass(len),
            Some((frame, _)) => Check::Fail(format!("{frame:?}")),
        }
    })
    .close()
    .build();
handle.attach(socket.fd(), peer);
// … run …
report.borrow().assert_finished();
```

An `expect` step is a closure over the bytes received so far. It returns `Check::More` to wait for more, `Check::Pass(n)` to consume the first `n` and move on (the rest goes to the next step), or `Check::Fail(reason)`. Because it always sees everything not yet consumed, it checks a reply that arrived split across writes, or glued to the next one, without reassembling it. `expect_bytes`, `expect_len` and `expect_until` cover the simple cases. `send` makes bytes readable, delivered in pieces the seed decides; `wait` pauses; `close` and `reset` end the connection with a FIN or an RST. `write_latency(min, max)` holds each of the program's sends for a drawn delay before it completes, and `short_writes(odds)` takes only part of some of them, so the program has to send the rest again.

A failed check resets the connection, so the program sees the peer go away and the run ends normally; the report then names the step and the reason. `assert_finished` also fails when the program stopped before the last step, or wrote bytes no step expected.

The runs worth having are the ones where the peer misbehaves, so a dialogue can misbehave on purpose:

- **`cut_send(bytes)`** sends the bytes up to a byte the seed picks, then resets. As on Linux, what got out is still read before the reset is reported.
- **`vanish()`** makes the peer go silent: no FIN and no RST. Reads wait for ever; sends go into a buffer nobody drains until it is full, then wait too. A program with no idle timeout stays stuck on such a peer, and the simulator reports it as a run that would never wake up.
- **`reset_after(delay)`** resets the connection at that time, whatever step the dialogue is on.
- **`fail_write(errno)`** makes the program's next send fail, and ends the connection.
- **`faults(Faults { send_error, cut_in_send, vanish })`** draws those faults from the seed instead, at the given odds per send or per step. At most one strikes a dialogue.

Faults that come from the kernel rather than the peer are the ring's to draw: `FakeRing::builder().kernel_faults(KernelFaults { interrupted, ended })` makes multishot reads of descriptors with actors fail with `EINTR`, or end after a chunk without `IORING_CQE_F_MORE`, at those odds. Neither hurts the connection; the read that re-arms carries on where the last one stopped.

The report names the fault that struck in `fault`. `assert_ok` accepts a dialogue that either finished or was cut short by a fault, but never one that saw a wrong reply, so a test can hold healthy connections to the whole conversation and faulted ones to ending cleanly.

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

## When a seed fails

A run is a function of its seed, so nothing is recorded while a suite runs: the way to look at a failing run is to run it again, watching. `FakeRing::builder().journal()` makes the ring keep a journal of everything it sees: each submission, each completion as the driver reads it, what each actor was asked and answered, timers, the kernel's stumbles, connections closing or resetting, and an actor's own notes (`cx.note(|| …)`, only evaluated when a journal is kept). Keeping it draws nothing and orders nothing, so the replay is the same run.

```text
      2.014ms fd 1073741824  note     fault: CutInSend { delivered: 12, of: 96 }
      2.014ms fd 1073741824  actor    read(16) -> Data(8)
      2.014ms fd 1073741824  actor    read(16) -> Error(ECONNRESET)
      2.014ms fd 1073741824  actor    write(4) -> Error(EPIPE)
```

`handle.journal()` hands back the events, and `handle.journal_text(fds, last)` renders them, only those about the given descriptors and only the last few if asked. `Dialogue` notes each step it completes and each fault that strikes.

`dst_echo` does this on its own: a failing seed is replayed by itself with the journal on, on a fresh thread so nothing the failed run left behind gets in the way. The whole journal goes to a file under `target/tmp/`, and the last events about the connections the failure names go into the failure message. `DST_SEED=<n> DST_TRACE=1` keeps the journal of a seed that passes too.

## What it is good for

The simulator is for the paths a real kernel makes rare or unreproducible: a read cancelled while its data is in flight, a stream dropped mid-batch, a ring that runs out of buffers, a short write followed by a broken pipe, a timeout that wins by one tick. The existing suites under `slopio/tests/` cover cancellation, buffer recycling and the runtime's event loop this way, and are the best place to look for patterns.

It also makes the runtime runnable under Miri, which a real io_uring never can be. `just test-sim` runs every simulated suite natively, `just miri` runs them under Miri, and `just miri-one <suite> [filter]` runs one; the flags and the list of suites live in the `justfile`.

## Limits

- Sends never block and never see backpressure. The simulator models the completion, not the network.
- A scripted descriptor's far end is only a state, closed or reset; for a peer that answers, attach an actor.
- The simulator is reachable from `slopio` only. The `slopio` facade's entry points always build a real ring.
