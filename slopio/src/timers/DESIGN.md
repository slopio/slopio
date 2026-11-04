# The two timer stores, as built

A record of how `slopio::timers` works as of 2026-09-24. Internals only — the
user-facing description of timers lives in `docs/time.md`.

The module holds two stores that share no code, not even a constant. Each is
built for one shape of workload, and the time driver routes a sleep to whichever
one can hold it.

```
wheel/{mod,nodes}.rs         arbitrary durations, up to 1 h
fixed/{mod,active_field}.rs Timers<MILLIS>, one fixed duration per store
```

Both count in **ticks of 10 ms**. Neither reads a clock: the caller supplies
the tick, which is what lets a simulation drive them from a clock it controls.
`TimeDriver` owns the conversion and drives the fixed stores off
`TimeWheel::ticks()`, so everything armed and everything fired reads one clock.

## The wheel — `wheel/`

Three levels, ms → s → min, and a cascade from each coarse level into the finer
ones. Every slot is a kernel-style intrusive doubly linked list threaded through
the timers' nodes.

```
TimeWheel
├─ ticks: u64            monotonic count of 10 ms ticks since creation
├─ nodes: Nodes          arena of every timer; the index is the id handed out
│  ├─ nodes: Vec<Node>   32 bytes each: Option<Waker>, deadline: u64, next/prev: u32
│  ├─ free: u32          head of the LIFO free list, threaded through `next`
│  └─ live: usize        nodes not on the free list
├─ buckets: BucketLevels
│  ├─ heads: [u32; 222]  first node of each slot, or NIL
│  │                     100 ms slots, then 61 seconds slots, then 61 minutes slots
│  ├─ ms_occupied: Bitset<u128>
│  ├─ s_occupied:  Bitset<u64>
│  └─ m_occupied:  Bitset<u64>
└─ last_tick: Instant
```

A node's `prev` says where it is:

| `prev` | meaning |
|---|---|
| a node id | linked in a slot, after that node |
| `BUCKET \| slot` (bit 31 set) | linked in a slot, first in it |
| `DONE` (`u32::MAX - 2`) | fired, waiting for its last poll |
| `FREE` (`u32::MAX - 1`) | on the free list |

The `BUCKET` encoding is the kernel's `pprev`: an unlink rewrites either the
predecessor's `next` or the slot's head without having to find out which slot
the node is in. Ids stay below `2³¹ − 3` so they never collide with the tags;
`alloc` panics past that, around two billion timers alive at once.

**100 ms slots, not 101.** The ms level only ever holds deadlines strictly
under a second out — at most 99 ticks, one short of the ring — so its 100 slots
never alias. The slot the clock stands on is reachable, by any duration under
two ticks, and that is correct: it is drained by the next tick, which is the
soonest the wheel can fire at all.

**61 seconds slots and 61 minutes slots.** A level holds deadlines up to one
full ring out, and a deadline exactly a ring out, armed part-way into a period,
lands 60 slots ahead: with 60 slots, that is the slot the clock stands on,
which has already cascaded. The timer would not fire early — a cascade re-places
by remaining time, and that slot comes round again exactly when the deadline's
window opens — but the spare slot keeps the current slot out of reach of
placement altogether (invariant 4). Replacing it with 60 still passes every
test in `tests/timewheel.rs`.

`MAX_DURATION` is the minutes ring's reach, 60 × 1 min = 1 h, inclusive.

A tick fires one ms slot, and cascades one seconds slot per second and one
minutes slot per minute:

```
process_single_tick():
  1. fire ms slot [ticks % 100]: detach the list, then for each node
     read `next`, mark it DONE, take and wake its waker
  2. ticks += 1
  3. if ticks % 100 == 0:
       if ticks % 6000 == 0: cascade minutes slot [(ticks / 6000) % 61]
       cascade seconds slot [(ticks / 100) % 61]
```

A cascade detaches the whole list and re-places each node by its stored
deadline; nothing is copied or freed, a node is only relinked. It runs *after*
the increment, so what it re-places lands in ms slots that have not fired yet
this tick — the one just fired is a tick behind. The minutes slot goes first,
and never into the seconds slot about to cascade: at a minute boundary, what it
holds is under a minute out, so at most 59 seconds slots ahead.

`place` picks the finest level whose window still holds the deadline:

| `deadline − now` | slot |
|---|---|
| `< 100` | ms `[deadline % 100]` |
| `< 6000` | seconds `[(deadline / 100) % 61]` |
| otherwise | minutes `[(deadline / 6000) % 61]` |

A deadline a second (a minute) or more out is always a whole slot ahead of the
current one, so the slot that has already cascaded is never targeted. Linking
is at the head, so a slot fires **newest first**. `init_timer` has already
rejected anything past the window, comparing `Duration`s rather than
milliseconds: `as_millis() as u64` truncates a large enough duration into a
small one that would otherwise be accepted.

`tick_at` advances by **whole ticks only**, keeping the sub-tick remainder in
`last_tick`. Resetting `last_tick` to `now` would discard up to `MS_TICK` per
call, and a busy event loop calling faster than once per 10 ms would never
accumulate a full tick.

`next_deadline` finds the next occupied slot on each level by rotating its
bitset and taking `trailing_zeros`, takes the soonest of the three, and answers
the distance **plus one tick** — a slot `i` ahead is drained by the i+1-th
tick, because a tick fires the slot it stands on before stepping off it.
Answering `i` instead sent the loop back to sleep a tick short of every firing,
where it found nothing to run and fell to the event loop's fixed 10 ms
re-sleep: a wait rounded up to the next tick, twice, for one timer. A seconds
or minutes slot answers with the distance to its *cascade*, which is never past
the earliest deadline it holds: the loop wakes at that boundary, the cascade
moves the timer down, and the next answer is finer. The occupied bits are exact
— set iff the list is non-empty — so the answer never names an empty slot.

**A duration of `n` ticks is served by the n-th tick**, and `init_timer` picks
its slot one short to say so. The slot is drained by the tick that reaches it,
so naming the n-th slot would be served by the n+1-th tick — the duration plus
however much of the current tick had already gone when the timer was armed.
Neither the duration itself nor a tick past it is reachable: that would need to
know how far into the current tick we are, and the wheel is the only clock in
the runtime, so nobody can tell it. The choice is which side to miss on, and
the near side wins — under a load that arms timers right after a tick, which is
when the event loop polls, the remainder is a few microseconds and a wait comes
back on time instead of a tick late.

### Timer lifecycle

```
   init_timer ─→ Waiting ──── slot fires ────→ Done ──── poll() → Ready ───→ Free
                    │                            │
                    │ drop(id): unlink           │ drop(id)
                    ▼                            ▼
                  Free                         Free
```

| call | `Waiting` | `Done` | `Free` |
|---|---|---|---|
| `drop(id)` | unlink, free | free | unreachable |
| `poll(id, waker)` | refresh waker if `!will_wake`, `Pending` | free, `Ready` | unreachable |
| fire (tick) | take waker, wake, → `Done` | — | — |
| cascade (tick) | relink by deadline | — | — |

There is no tombstone: a cancel unlinks in O(1), wherever the node sits, and
puts it back on the free list at once, clearing the slot's bit if the list
empties. A slot only ever holds `Waiting` nodes, because firing detaches the
whole list before touching any node, and a node's `next` is read before it is
marked `Done`.

An id is **dead after `drop` or after `poll` returned `Ready`**: the node is
free and may be handed to the next timer armed. `Sleep` never touches an id
after either. The free list is LIFO, so the node reused is the one most
recently touched, still in cache.

## The fixed-duration store — `fixed/`

`Timers<const MILLIS: u64>`: every timer in one store runs for the same
duration. That single fact is what makes all three of its queues FIFOs —
arming later can only end later, so deadlines are non-decreasing by
construction and nothing is ever sorted or searched.

```
Timers<MILLIS>
├─ deadlines: Deadlines<MILLIS>  counting ring, MILLIS/10 + 1 slots of u32
├─ wakers:    Wakers             VecDeque<Option<Waker>>, None once cancelled
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

Cancelling clears the entry's bit and drops its waker. If the entry is at the
head, the whole dead prefix behind it is reclaimed at once; otherwise it stays
in place, costing its slot until the clock reaches it.

## Invariants

1. **Wheel.** A node is linked in exactly one slot iff it is `Waiting`; `Done`
   and `Free` nodes are in no slot. Cancel and cascade rely on this to unlink
   and relink without searching.
2. **Wheel.** `last_tick` advances by whole ticks only.
3. **Wheel.** An occupancy bit is set iff its slot's list is non-empty, so
   `next_deadline` never names an empty slot.
4. **Wheel.** The seconds (minutes) slot the clock stands on is never a
   placement target: it has already cascaded, and a deadline that would name
   it is under a second (a minute) out, which a finer level takes instead.
5. **Fixed store.** Every timer in a store has the same duration, so deadlines
   are non-decreasing and the live window never exceeds `SLOTS`.
6. **Fixed store.** `push` and `tick` are driven by the same clock. Arming from
   a clock that has run ahead of the one that sweeps overruns the window; the
   clamp turns that into an early fire rather than a wrap onto a live slot.
7. **Fixed store.** A cancelled timer not at the head keeps its entry until the
   clock reaches its deadline. Its live set is `arm rate × duration`, not
   `timers in flight`; the wheel's is `timers in flight`.

## Known costs

- **The wheel's arena never shrinks.** `Vec<Node>` stays at its high-water
  mark, 32 bytes per timer alive at the peak; freed nodes are reused, never
  returned.
- **A slot fires newest first**, not in arming order. Timers sharing a 10 ms
  tick have no order to keep, but a test that expects FIFO wakes will see them
  reversed.
- **A long timer costs a wakeup per level it crosses.** `next_deadline` answers
  a seconds or minutes slot with its cascade time, so an hour-long timer wakes
  the loop at its minutes cascade, then at its seconds cascade, then fires.
- **A cascade is O(timers in the slot)** and runs on the tick its window opens,
  so a slot holding thousands of timers pays for all of them on one tick. They
  were going to fire within that window anyway; the cost is that the work lands
  in one place rather than spread over the ticks that follow. A relink touches
  the node and its old neighbour, nothing is copied.
- **The fixed store retains cancelled entries** behind a live head for up to
  its duration — 5 s for `Timers<5_000>` — though without their wakers.
- **`Timers::new` allocates its ring up front** — `MILLIS/10 + 1` × 4 bytes,
  about 2 KB for a 5 s store — per worker thread, whether or not anything is
  ever armed in it.

## Not covered

`Sleep` panics on any duration that is neither ≤ 1 h nor exactly 5 s.

The `against_the_wheel` benchmark in `fixed/mod.rs` arms 5 s timers into
both stores, 95% cancelled, 3000 ticks (release, this machine, 2026-09-24):

```
                      arm     cancel  tick    peak
 10/tick  TimeWheel   9.4 ns  5.5 ns  4.3 ns    1738
          Timers      6.0 ns  4.6 ns  6.0 ns    5010
100/tick  TimeWheel   5.7 ns  4.0 ns  1.0 ns   16898
          Timers      4.0 ns  2.9 ns  1.0 ns   50100
500/tick  TimeWheel   7.4 ns  4.3 ns  0.9 ns   84878
          Timers      3.3 ns  2.7 ns  0.5 ns  250500
```

The fixed store is still faster to arm, cancel and, under load, tick; the wheel
now holds about a third of the entries, because it frees a cancel at once where
the fixed store keeps it until the clock reaches it. Before the linked-list
rewrite the wheel lost on every axis, peak included (312283 at 500/tick).

Measured numbers for the runtime side of this — the allocation cost of the
`timeout()` idiom on the TCP hot path — and the open items are in `TODO.md`.
