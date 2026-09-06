# Time and the timing wheel

The runtime has no timer thread. Time is one more thing the event loop drives: on every turn it advances a timing wheel, and when it has nothing else to do it asks the wheel how long until the next timer and sleeps in the kernel for exactly that long. An idle runtime with no timers pending does not wake up at all.

## What you get

- **`Sleep::during(duration)`** is the primitive. It is a future; drop it to cancel.
- **`timeout(duration, future)`** races a future against a sleep and gives you `Ok(output)` or `Err(())`. The losing side is dropped, so a timed-out I/O operation is cancelled the way described in [cancellation](cancellation.md).
- **`repeat(interval, f)`** runs an async closure forever with a sleep between runs, and stops on shutdown.
- **`retry(max, delay, f, should_retry)`** re-runs a fallible async closure with a fixed delay between attempts, up to a maximum, and also stops on shutdown.
- **`runtime::now()`** returns the current wall-clock time as a `SystemTime`, see below.

## Reading the time is free

Every turn of the event loop reads the OS clock once, to advance the wheel, and keeps the value. `runtime::now()` hands that value back: it is a read of a thread-local, no syscall, no vDSO call, no atomic. You can call it on every request, on every log line, in every loop iteration, at zero cost.

The trade-off is that it is only as fresh as the last turn of the loop. Between two turns it does not move, and a task that runs for a long time without yielding sees the same timestamp throughout. Use it for timestamps and coarse deadlines; use `std::time::Instant::now()` when you are measuring something.

## Resolution

The wheel ticks every 10 ms, and that is the resolution of every timer. A duration is rounded down to whole ticks, and the timer fires on the tick that closes its window, so a sleep of `d` milliseconds completes somewhere between `d` rounded down to 10 ms and 10 ms after that, plus whatever the event loop was doing at the time. In practice:

- A sleep of 1 to 9 ms fires on the next tick, anywhere from immediately to 10 ms later.
- Sleeps under 50 ms are usable for pacing but jitter visibly. Do not assert tight bounds on them.
- From about a second upward, timers are exact to a tick.

Timers are cooperative, like everything else in the runtime. A task that runs for 200 ms without yielding delays every timer on that thread by 200 ms.

## Limits

Durations of 24 hours or more are refused. Split anything longer into a loop of shorter sleeps.

## Cost

Registering a timer, cancelling it and finding the next deadline are all constant time, regardless of how many timers exist. Holding hundreds of thousands of pending sleeps is fine. The wheel has four levels, milliseconds, seconds, minutes and hours, and a timer moves from a coarse level to a finer one as its deadline approaches. That is what keeps long timers from costing anything until they are close to firing. Cancelling a sleep frees its slot lazily, the next time the wheel sweeps its bucket, so a burst of dropped timers is absorbed without a spike.

## Time in simulation

Under the `simulation` feature the wheel reads time from a virtual clock instead of the OS, and the simulated ring advances that clock instead of sleeping. A test that spans an hour of timers runs in microseconds, and the order in which timers and I/O completions fire is fully determined by the scenario. Nothing in the timer code changes between the two modes; only where it reads the clock.
