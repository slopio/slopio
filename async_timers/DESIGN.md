# The hierarchical timing wheel, as built

A record of how `async_timers` works as of b817464 (2026-09-11), written down
so a replacement can be compared against it. Internals only — the user-facing
description of timers lives in `docs/time.md`.

## Shape

```
TimeWheel
├─ ticks: u64            monotonic count of 10 ms ticks since creation;
│                        every level index derives from it
├─ storage: TimerStorage the data — slab::Slab<Timer>, one slot per timer,
│                        the slot index is the TimerId handed to the caller
├─ buckets: BucketLevels the ordering — 4 levels of SmallVec<[TimerId; 8]>,
│                        plus one occupancy bitset per level
├─ last_tick: Instant
└─ last_tick_wall: SystemTime
```

A bucket holds **bare slab indices**. Everything awkward about the design
follows from that one fact.

## Levels

```
              resolution   slots   window     current index
  ms_level       10 ms      100      1 s      ticks % 100
  s_level         1 s        60     60 s      (ticks / 100) % 60
  m_level         1 min      60     60 min    (ticks / 6 000) % 60
  h_level         1 h        25     25 h      (ticks / 360 000) % 25

  ms_occupied: Bitset<u128>   s_occupied: Bitset<u64>
  m_occupied:  Bitset<u64>    h_occupied: Bitset<u32>
```

`H_BUCKETS` is 25, not 24: a deadline up to 24 h out can land 24 hour-slots
ahead of `ticks`. `init_timer` rejects durations of 24 h or more with
`DurationTooLong`.

Each level's period is a whole multiple of the level below, so a slot cascades
exactly when its window opens. `TICKS_PER_S = 100`, `TICKS_PER_M = 6 000`,
`TICKS_PER_H = 360 000`.

The bitsets exist so `next_deadline` can find the next occupied slot without
walking 245 buckets. `next_set(from, len)` rotates the word by `from` and takes
`trailing_zeros`.

## Placement

`init_timer` rounds the duration down to whole ticks
(`deadline = ticks + total_ms / MS_TICK`), inserts into the slab, then calls
`place`. `place` walks down and stops at the **finest level whose window still
contains the deadline**:

```
  deadline - now < 100                → ms_level[deadline % 100]           (< 1 s)
  deadline/100  - now/100  < 60       → s_level[(deadline / 100) % 60]     (< 1 min)
  deadline/6000 - now/6000 < 60       → m_level[(deadline / 6000) % 60]    (< 1 h)
  otherwise                           → h_level[(deadline / 360000) % 25]
```

A timer is therefore not placed once. It **descends** level by level as its
deadline approaches, re-placed by `cascade` each time its current slot opens.

## A tick

```
process_single_tick():

  1. fire the current ms slot     ms_level[ticks % 100] → storage.wake(id) for each
  2. ticks += 1
  3. if ticks % 100     == 0  →  cascade s_level[s_idx]     ─┐ coarse before fine:
     if ticks % 6 000   == 0  →  cascade m_level[m_idx]      │ a cascaded timer may
     if ticks % 360 000 == 0  →  cascade h_level[h_idx]     ─┘ land in a finer slot
                                                               opening on this tick
```

`cascade` takes the bucket with `mem::take`, reads each timer's deadline back
from the slab and re-`place`s it against the now-current `ticks`.

`tick_at` advances by **whole ticks only**, keeping the sub-tick remainder in
`last_tick`. Resetting `last_tick` to `now` would discard up to `MS_TICK` per
call, and a busy event loop calling faster than once per 10 ms would never
accumulate a full tick.

Worked example — a 5 s timer armed at `ticks = 0`, deadline `500`:

```
  t=0     place: 500-0 = 500 >= 100, but 5-0 = 5 < 60   → s_level[5]
          ......... sits in one slot for 5 s, no per-tick work
  t=499   tick: fires ms_level[99], ticks → 500, 500 % 100 == 0
          cascade s_level[5] → re-place: 500-500 = 0 < 100 → ms_level[0]
  t=500   tick: fires ms_level[0] → wake()              ← exact to the tick
```

Hence the documented behaviour: exact to a tick from about a second upward,
10 ms floor below that.

## Timer lifecycle

```
   init_timer ─→ Waiting ──── bucket fires ────→ Done ──── poll() ────→ slot freed
                    │                             ▲
                    │ Sleep::drop                 │ (drop after firing
                    ▼                             │  frees it too)
                Cancelled ───────────────────────┘
                    │
                    └─ slot RETAINED until its bucket is swept
                       (wake() on a fire, or deadline() on a cascade)
```

`TimerStorage` operations:

| call | `Waiting` | `Done` | `Cancelled` |
|---|---|---|---|
| `drop(id)` | → `Cancelled`, slot kept | remove | no-op |
| `poll(id, waker)` | refresh waker if `!will_wake`, `Pending` | remove, `Ready` | `Ready`, slot kept |
| `wake(id)` (fire) | `wake_by_ref`, → `Done` | unreachable | remove |
| `deadline(id)` (cascade) | `Some(deadline)` | unreachable | remove, `None` |

## Why `Cancelled` cannot free its slot

The bucket names the timer by slab index alone. Freeing the slot on cancel
would let the slab hand that index to a new timer while the old bucket entry
still names it — the sweep would then wake or destroy the wrong timer. The
`Cancelled` tombstone is the price of a bucket that stores a bare index.

## Invariants

1. A slab slot is released only once no bucket names it. This is the one that
   forces the tombstone.
2. Each level's period is a whole multiple of the level below.
3. Within a tick, coarse levels cascade before fine ones.
4. `last_tick` advances by whole ticks only.
5. An occupancy bit is set on insert and cleared only when the bucket fires or
   cascades. A bucket holding nothing but tombstones therefore keeps its bit
   set.

## Known costs

- **Cancelled timers are retained for the full timeout duration.** A guard
  timeout cancelled microseconds after being armed sits in its coarse bucket
  until that bucket's slot comes up. The live set is `rate × timeout duration`,
  not `timeouts in flight`: at 100k req/s with a read and a write
  `timeout(5s, ..)` per request, ~1M dead entries ≈ 40 MB (32 B slab entry plus
  8 B bucket entry), with the per-slot `SmallVec`s well past their 8 inline
  entries. A timer cancelled immediately is still counted by `active_timers`
  until its bucket is swept: 1000 cancelled 5 s timeouts are all still there
  after a second, and gone after six.
- **Invariant 5 costs wakeups.** `next_deadline` can name a slot that holds
  only tombstones, so the event loop wakes for a bucket with no live timer.
- **`cascade`'s `mem::take`** drops a spilled `SmallVec` buffer on every sweep
  and reallocates it the next time the slot fills.
- **`docs/time.md` describes the retention as lazy freeing that absorbs a burst
  of dropped timers without a spike.** True as far as it goes, but it does not
  say that the footprint scales with the timeout duration; revisit that
  paragraph alongside any replacement.

Measured numbers for the runtime side of this — the allocation cost of the
`timeout()` idiom on the TCP hot path — and the open items are in `notes.md`.
