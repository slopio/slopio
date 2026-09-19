// -*- mode: Bluespec -*-
// The driver's op slab (io_driver/mod.rs). `OneshotSlab` and `MultishotSlab`
// wrap the same `slab::Slab` under two key types, so one model covers both.
//
// Three operations — insert hands out an index, lookup resolves one, remove
// gives the slot back — and the two things that can go wrong with them: two
// live indices naming the same slot, and one slot freed twice.
module slab {
  /// Whatever the slab is holding: `SingleOpState`, `MultiOpState`, a
  /// `ReplySlot`. Generic because none of the properties below read it.
  type Slot[a] =
    | Vacant
    | Occupied(a)

  // --------------------------------------------------------------------
  // State
  // --------------------------------------------------------------------

  /// Only indices already handed out are keys: the slab has no size, it grows.
  var slots: int -> Slot[int]

  /// Vacant slots. The crate threads this list through the vacant entries
  /// themselves and pushes a freed index at the head.
  var freeList: List[int]

  /// The next never-used index. The crate's `len` frontier.
  var nextIndex: int

  /// The index the last `insert` returned, so a run can name it.
  var lastIndex: int

  action init = all {
    slots' = Map(),
    freeList' = List(),
    nextIndex' = 0,
    lastIndex' = -1,
  }

  // --------------------------------------------------------------------
  // The three operations
  // --------------------------------------------------------------------

  /// `Slab::get_mut`. A pure read, so a def and not an action.
  ///
  /// Note what it takes: an index, and nothing beside it. Nothing here can
  /// tell a fresh index from one handed out, freed, and handed out again.
  pure def lookup(s: int -> Slot[a], id: int): Slot[a] =
    if (s.keys().contains(id)) s.get(id) else Vacant

  /// The index the next `insert` will hand out. An action cannot return one —
  /// its result is already the transition's enabledness — so the index is
  /// published as a function of the state it is read from. A caller that needs
  /// it in the same step as the insert, to stamp a user_data with it, reads it
  /// here rather than from `lastIndex`, which only holds the previous one.
  pure def nextId(free: List[int], frontier: int): int =
    if (free == List()) frontier else free.head()

  /// `Slab::insert`: hand back a vacant slot's index, or a fresh one. Never
  /// guarded — the slab has no capacity to run out of, so insert always fires.
  action insert(v: int): bool = any { insertReusing(v), insertGrowing(v) }

  action insertReusing(v: int): bool = all {
    freeList != List(),
    val id = freeList.head()
    all {
      slots' = slots.set(id, Occupied(v)),
      freeList' = freeList.tail(),
      nextIndex' = nextIndex,
      lastIndex' = id,
    },
  }

  /// Only with nothing free: the slab never grows over a reusable slot.
  action insertGrowing(v: int): bool = all {
    freeList == List(),
    slots' = slots.put(nextIndex, Occupied(v)),
    freeList' = freeList,
    nextIndex' = nextIndex + 1,
    lastIndex' = nextIndex,
  }

  /// `Slab::remove`: the slot goes back on the free list, and its value goes
  /// back to the caller — in the driver, dropping that value is what frees a
  /// keepalive buffer and posts a parked fd's close.
  action removeAt(id: int): bool = all {
    lookup(slots, id) != Vacant,
    slots' = slots.set(id, Vacant),
    freeList' = List(id).concat(freeList),
    nextIndex' = nextIndex,
    lastIndex' = lastIndex,
  }

  pure def occupied(s: int -> Slot[a]): Set[int] =
    s.keys().filter(i => s.get(i) != Vacant)

  action step = any {
    nondet v = oneOf(Set(1, 2)) insert(v),
    all {
      occupied(slots) != Set(),
      nondet id = oneOf(occupied(slots)) removeAt(id),
    },
  }

  // --------------------------------------------------------------------
  // Invariants
  // --------------------------------------------------------------------

  pure def asSet(l: List[int]): Set[int] =
    l.foldl(Set(), (acc, x) => acc.union(Set(x)))

  /// An index handed out is on the free list exactly when its slot is vacant.
  /// Hand out an index twice, or free an occupied slot, and this breaks.
  val freeListMatchesVacancy =
    slots.keys().forall(i => asSet(freeList).contains(i) == (lookup(slots, i) == Vacant))

  /// The free list never repeats an index: no slot is freed twice.
  val freeListHasNoDuplicates =
    asSet(freeList).size() == freeList.length()

  /// Growth only ever moves forward: every key is an index already handed out.
  val frontierIsFresh =
    slots.keys().forall(i => i < nextIndex)

  val slabInvariants = all {
    freeListMatchesVacancy,
    freeListHasNoDuplicates,
    frontierIsFresh,
  }

  // --------------------------------------------------------------------
  // Runs
  // --------------------------------------------------------------------

  /// One entry's life: insert hands out an index, lookup resolves it, remove
  /// puts it back, lookup says nothing is there.
  run insertLookupRemoveTest =
    init
      .then(insert(7))
      .expect(lastIndex == 0 and lookup(slots, 0) == Occupied(7))
      .then(removeAt(0))
      .expect(lookup(slots, 0) == Vacant)

  /// Nothing free means a fresh index, every time. No bound to reach.
  run growsWithoutLimitTest =
    init
      .then(insert(1))
      .then(insert(2))
      .then(insert(3))
      .expect(lastIndex == 2 and nextIndex == 3 and freeList == List())

  /// Indices are recycled: the index a removed entry had comes back naming
  /// another entry. A token kept past its remove is therefore not detectably
  /// stale — `lookup` answers, and answers about someone else's op. What rules
  /// that out is the driver's discipline of reaping each op exactly once, not
  /// anything the slab does.
  run recycledIndexAliasesTest =
    init
      .then(insert(7))
      .then(removeAt(0))
      .then(insert(9))
      .expect(lastIndex == 0 and lookup(slots, 0) == Occupied(9))
}
