# Time and timers

The runtime has no timer thread. Time is one more thing the event loop drives: on every turn it advances its timer stores, and when it has nothing else to do it asks them how long until the next timer and sleeps in the kernel for exactly that long. An idle runtime with no timers pending does not wake up at all.

## What you get

- **`Sleep::during(duration)`** is the primitive. It is a future; drop it to cancel. See [limits](#limits) for the durations it accepts.
- **`timeout(duration, future)`** races a future against a sleep and gives you `Ok(output)` or `Err(())`. The losing side is dropped, so a timed-out I/O operation is cancelled the way described in [cancellation](cancellation.md).
- **`repeat(interval, f)`** runs an async closure forever with a sleep between runs, and stops on shutdown.
- **`retry(max, delay, f, should_retry)`** re-runs a fallible async closure with a fixed delay between attempts, up to a maximum, and also stops on shutdown.
- **`runtime::now()`** returns the current wall-clock time as a `SystemTime`, see below.

## Reading the time is free

Every turn of the event loop reads the OS clock once, to advance its timer stores, and keeps the value. `runtime::now()` hands that value back: it is a read of a thread-local, no syscall, no vDSO call, no atomic. You can call it on every request, on every log line, in every loop iteration, at zero cost.

The trade-off is that it is only as fresh as the last turn of the loop. Between two turns it does not move, and a task that runs for a long time without yielding sees the same timestamp throughout. Use it for timestamps and coarse deadlines; use `std::time::Instant::now()` when you are measuring something.

## Resolution

The clock ticks every 10 ms, and that is the resolution of every timer, whichever duration you asked for. A duration is rounded down to whole ticks, and the timer fires on the tick that closes its window, so a sleep of `d` milliseconds completes somewhere between `d` rounded down to 10 ms and 10 ms after that, plus whatever the event loop was doing at the time. In practice:

- A sleep of 1 to 9 ms fires on the next tick, anywhere from immediately to 10 ms later.
- Sleeps under 50 ms are usable for pacing but jitter visibly. Do not assert tight bounds on them.
- From about a second upward the 10 ms window is noise, and timers land where you asked.

Timers are cooperative, like everything else in the runtime. A task that runs for 200 ms without yielding delays every timer on that thread by 200 ms.

## Limits

A sleep must be one of:

- **any duration up to one hour**, or
- **exactly five seconds**.

Anything else panics on the first poll. This applies to everything above, not just `Sleep::during` — `timeout`, `repeat` and `retry` all sleep, so their durations are bound by the same set. This is deliberate: rather than one structure stretched to cover everything from a millisecond to a day, each class of duration is served by the structure that suits it, and a duration nothing covers fails loudly instead of quietly firing at the wrong time.

Five seconds is in the first set too, and still gets its own store: a duration a whole fleet of timers shares is served more cheaply by a structure that knows they all share it.

The set is meant to grow as the runtime needs it to. Adding a fixed duration is a localised change in `runtime-core/src/runtime/time.rs`: a constant, a store to hold it, and a routing arm. Adding one is cheap; needing one you did not add is a panic you will see immediately.

Until the duration you want exists, build it out of what does — a loop of one-hour sleeps, or a counter over a repeating five-second one.

## Cost

Arming a timer, cancelling it and finding the next deadline are all constant time, regardless of how many timers exist. Holding hundreds of thousands of pending sleeps is fine.

Cancelling a sleep frees it at once, so a timeout per request costs only the timeouts still pending, whatever their duration. The one exception is a five-second sleep: its store keeps a small entry for a cancelled sleep until the five seconds are up, so its live set is the rate of sleeps armed times five seconds. That store is still the cheaper one to arm and cancel.

## Time in simulation

Under the `simulation` feature the timer stores read time from a virtual clock instead of the OS, and the simulated ring advances that clock instead of sleeping. A test that spans thousands of timers runs in microseconds, and the order in which timers and I/O completions fire is fully determined by the scenario. Nothing in the timer code changes between the two modes; only where it reads the clock.
