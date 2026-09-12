# The two timer stores, as built

A record of how `async_timers` works as of 2026-09-12. Internals only — the
user-facing description of timers lives in `docs/time.md`.

The crate holds two stores that share no code, not even a constant. Each is
built for one shape of workload, and `runtime-core` routes a sleep to whichever
one can hold it.

```
src/wheel/{mod,slab}.rs          arbitrary durations, up to 1 min
src/timers/{mod,active_field}.rs Timers<MILLIS>, one fixed duration per store
```

Both count in **ticks of 10 ms**. Neither reads a clock: the caller supplies
the tick, which is what lets a simulation drive them from a clock it controls.
`TimeDriver` owns the conversion and drives the fixed stores off
`TimeWheel::ticks()`, so everything armed and everything fired reads one clock.

## The wheel — `src/wheel/`

Two levels, ms → s, and one cascade between them.

```
TimeWheel
├─ ticks: u64            monotonic count of 10 ms ticks since creation
├─ storage: TimerStorage slab::Slab<Timer>, the slot index is the id handed out
├─ buckets: BucketLevels
│  ├─ ms_level: [SmallVec<[TimerId; 8]>; 100]   one slot per tick of a second
│  ├─ s_level:  [SmallVec<[TimerId; 8]>; 61]    one slot per second of a minute
│  ├─ ms_occupied: Bitset<u128>
│  └─ s_occupied:  Bitset<u64>
├─ last_tick: Instant
└─ last_tick_wall: SystemTime
```

**61 seconds slots, not 60.** The window is `MAX_DURATION` = 1 min = 60
seconds, and a deadline exactly that far out lands 60 seconds-slots ahead of
the one the clock is standing on. That slot has already cascaded this second,
so an aliased timer would go off a minute early. One spare slot separates the
two ends of the window — the same `+1` the deadline ring below uses, for the
same reason.

**100 ms slots, not 101.** The ms level only ever holds deadlines strictly
under a second out, so its 100 slots never alias: the slot the clock stands on
is reachable only by a deadline of *now*, which means the next tick, which is
what a deadline of now should mean.

A tick fires one ms slot, and cascades one seconds slot per second:

```
process_single_tick():
  1. fire ms_level[ticks % 100] → storage.wake(id) for each
  2. ticks += 1
  3. if ticks % 100 == 0: cascade s_level[(ticks / 100) % 61]
                          → place(id, storage.deadline(id)) for each
```

The cascade runs *after* the increment, so what it re-places lands in ms slots
that have not fired yet this tick — the one just fired is a tick behind.

`place` picks the finest level whose window still holds the deadline: under a
second out to `ms_level[deadline % 100]`, otherwise to
`s_level[(deadline / 100) % 61]`. A deadline a second or more out is always a
whole seconds slot ahead of the current one, so the slot that has already
cascaded is never targeted. `init_timer` has already rejected anything past the
window, comparing `Duration`s rather than milliseconds: `as_millis() as u64`
truncates a large enough duration into a small one that would otherwise be
accepted.

`tick_at` advances by **whole ticks only**, keeping the sub-tick remainder in
`last_tick`. Resetting `last_tick` to `now` would discard up to `MS_TICK` per
call, and a busy event loop calling faster than once per 10 ms would never
accumulate a full tick.

`next_deadline` finds the next occupied slot by rotating the bitset and taking
`trailing_zeros`, ms level first. It answers the distance to the slot, which is
**one tick short** of when that slot fires — a lower bound, so the loop wakes
once early and re-sleeps. Never late. A seconds slot answers with the distance
to its *cascade*, which is earlier still: the loop wakes at the second
boundary, the cascade moves the timer down to the ms level, and the next answer
is exact.

### Timer lifecycle

```
   init_timer ─→ Waiting ──── bucket fires ────→ Done ──── poll() ────→ slot freed
                    │                             ▲
                    │ Sleep::drop                 │ (drop after firing
                    ▼                             │  frees it too)
                Cancelled ───────────────────────┘
                    │
                    └─ slot RETAINED until its bucket fires (wake())
                       or cascades (deadline())
```

| call | `Waiting` | `Done` | `Cancelled` |
|---|---|---|---|
| `drop(id)` | → `Cancelled`, slot kept | remove | no-op |
| `poll(id, waker)` | refresh waker if `!will_wake`, `Pending` | remove, `Ready` | `Ready`, slot kept |
| `wake(id)` (fire) | `wake_by_ref`, → `Done` | unreachable | remove |
| `deadline(id)` (cascade) | its deadline, re-placed | unreachable | remove, not re-placed |

A bucket names a timer by **bare slab index**. Freeing the slot on cancel would
let the slab hand that index to a new timer while the old bucket entry still
names it, and the sweep would wake or destroy the wrong one. The `Cancelled`
tombstone is the price of a bucket that stores a bare index.

The cascade reads a timer's deadline back after placement, which is the only
reason the slab stores one: `Waiting(Waker, u64)`. A cancelled timer is freed
there instead of re-placed — the seconds bucket it was named by is being
drained, and no other bucket has it yet.

## The fixed-duration store — `src/timers/`

`Timers<const MILLIS: u64>`: every timer in one store runs for the same
duration. That single fact is what makes all three of its queues FIFOs —
arming later can only end later, so deadlines are non-decreasing by
construction and nothing is ever sorted or searched.

```
Timers<MILLIS>
├─ deadlines: Deadlines<MILLIS>  counting ring, MILLIS/10 + 1 slots of u32
├─ wakers:    VecDeque<Waker>
├─ active:    ActiveField        one bit per entry, cleared on cancel
└─ popped:    u64                entries retired, the base for token → index
```

A `Token<MILLIS>` is the absolute position of an entry in the arm sequence;
`index_of` is `token - popped`, and a token that falls below `popped` has
fired or been cancelled. The duration is in the token's type, so a token
cannot be replayed against a store of another duration.

`Deadlines` is a **counting ring**: one `u32` per tick, indexed by
`at % SLOTS`, holding how many timers are due on that tick. Live deadlines
never span more than one duration, so `MILLIS/10 + 1` slots hold them all
without wrapping onto a live one — hence the `debug_assert` and the clamp in
`push`, which is the one precondition this structure has and the wheel does
not. It replaced a run-length-encoded `VecDeque<{at, count}>` (through
19faecf): the deque's footprint tracked the number of distinct occupied ticks
and grew with traffic, the ring's is fixed at construction and allocates once.
The trade is that sweeping costs a slot per tick elapsed, empty or not, rather
than a step per distinct deadline — a win while timers are armed densely,
which is what this store is for.

`next_deadline` is **exact**, not a bound: it reads the first entry still
active, stepping over cancelled ones, and `tick` fires what is strictly behind
the clock, so the answer is `at - now + 1`.

Cancelling clears the entry's bit. If the entry is at the head, the whole dead
prefix behind it is reclaimed at once; otherwise it stays in place, costing its
slot until the clock reaches it.

## Invariants

1. **Wheel.** A slab slot is released only once no bucket names it. This is the
   one that forces the tombstone.
2. **Wheel.** `last_tick` advances by whole ticks only.
3. **Wheel.** An occupancy bit is set on insert and cleared only when the bucket
   fires or cascades. A bucket holding nothing but tombstones therefore keeps
   its bit set, and `next_deadline` can name it — a wakeup for no live timer.
4. **Wheel.** The seconds slot the clock stands on is never a placement target:
   it has already cascaded, and a deadline that would name it is under a second
   out, which the ms level takes instead.
5. **Fixed store.** Every timer in a store has the same duration, so deadlines
   are non-decreasing and the live window never exceeds `SLOTS`.
6. **Fixed store.** `push` and `tick` are driven by the same clock. Arming from
   a clock that has run ahead of the one that sweeps overruns the window; the
   clamp turns that into an early fire rather than a wrap onto a live slot.
7. **Both.** A cancelled timer keeps its place until the clock reaches its
   deadline. The live set is `arm rate × duration`, not `timers in flight`.

## Known costs

- **Cancelled timers are retained for the full duration**, in both stores. A
  guard timeout cancelled microseconds after being armed still occupies memory
  until its deadline comes up — at most 1 min in the wheel, 5 s in the
  `Timers<5_000>` store. The wheel's bound is the duration *asked for*, so a
  minute-long timer armed per request would hold a slab slot per request for a
  minute. That is not the intended traffic: a timeout armed at request rate
  belongs in a fixed store, and the wheel's reach is for the occasional long
  sleep.
- **Invariant 3 costs wakeups.** The wheel's `next_deadline` can name a slot
  holding only tombstones. The fixed store does not have this problem: it steps
  over cancelled entries.
- **The wheel's `next_deadline` is a tick short**, so the loop wakes once per
  deadline for nothing before the real one. A timer still at the seconds level
  costs one such wakeup per second boundary it waits through, not per tick.
- **A cascade is O(timers in the slot)** and runs on the tick the second opens,
  so a slot holding thousands of timers pays for all of them on one tick. They
  were going to fire within that second anyway; the cost is that the work lands
  in one place rather than spread over the hundred ticks that follow.
- **`Timers::new` allocates its ring up front** — `MILLIS/10 + 1` × 4 bytes,
  about 2 KB for a 5 s store — per worker thread, whether or not anything is
  ever armed in it.

## Not covered

`Sleep` panics on any duration that is neither ≤ 1 min nor exactly 5 s.

The `against_the_wheel` benchmark in `src/timers/mod.rs` runs again now that
the wheel takes a 5 s timer — it arms 1.5 M of them over 3000 ticks, 95%
cancelled, and the fixed store still wins on every axis (release, this machine,
500 arms/tick):

```
TimeWheel   arm  6.7 ns  cancel  2.7 ns  tick  1.9 ns  peak 312283
Timers      arm  3.1 ns  cancel  1.9 ns  tick  1.0 ns  peak 250500
```

That is the whole case for keeping a store per fixed duration rather than
routing everything through the wheel now that the wheel could hold it.

Measured numbers for the runtime side of this — the allocation cost of the
`timeout()` idiom on the TCP hot path — and the open items are in `notes.md`.
