module op_lifecycle {
  import slab as sl from "./datastructures/slab"

  /// A submitted SQE: the user_data the kernel will echo, and the op it names.
  /// `ud` is the slab index — for a single-shot op the encoding is the identity
  /// (user_data.rs), so the word and the index are the same number.
  type InFlight = { ud: int, op: int }

  /// SQEs the kernel still owes a completion for.
  var inflight: Set[InFlight]

  /// Distinguishes ops. Nothing reads it but the model.
  var nextOp: int

  action init = all {
    sl::init,
    inflight' = Set(),
    nextOp' = 0,
  }

  // --------------------------------------------------------------------
  // Actions
  // --------------------------------------------------------------------

  /// `push_op`: take a slab slot, stamp its index on the SQE. The index has to
  /// be read off the pre-state — an action cannot hand one back.
  action pushOp = all {
    val id = sl::nextId(sl::freeList, sl::nextIndex)
    all {
      sl::insert(nextOp),
      inflight' = inflight.union(Set({ ud: id, op: nextOp })),
      nextOp' = nextOp + 1,
    },
  }

  /// The completion lands and the op is reaped: `poll_op` finds `Ended` and
  /// removes the slot. The CQE and the entry go together — that is the whole
  /// discipline.
  action reap = all {
    inflight != Set(),
    nondet e = oneOf(inflight)
    all {
      sl::removeAt(e.ud),
      inflight' = inflight.exclude(Set(e)),
      nextOp' = nextOp,
    },
  }

  /// The mistake: free the slot while the kernel still owes a completion —
  /// what a cancel would do if it dropped the entry instead of parking it in
  /// `Cancelled`. Not part of `step`; `stepWithEarlyReap` is where it lives.
  action reapEarly = all {
    inflight != Set(),
    nondet e = oneOf(inflight)
    all {
      sl::removeAt(e.ud),
      inflight' = inflight,
      nextOp' = nextOp,
    },
  }

  action step = any { pushOp, reap }

  action stepWithEarlyReap = any { pushOp, reap, reapEarly }

  // --------------------------------------------------------------------
  // Invariants
  // --------------------------------------------------------------------

  /// Every completion still owed resolves to the op it was submitted for.
  /// Routing a CQE is a slab lookup and nothing else, so this is exactly the
  /// condition under which routing is correct.
  val cqeFindsItsOwnOp =
    inflight.forall(e => sl::lookup(sl::slots, e.ud) == sl::Occupied(e.op))

  /// Holds under `step`. Under `stepWithEarlyReap` the checker finds a trace
  /// where it does not — see `lateCqeAliasesTest`.
  val opInvariants = all {
    cqeFindsItsOwnOp,
    sl::slabInvariants,
  }

  // --------------------------------------------------------------------
  // Runs
  // --------------------------------------------------------------------

  /// Two ops in flight, each routed to its own entry, each reaped once.
  run lifecycleTest =
    init
      .then(pushOp)
      .then(pushOp)
      .expect(inflight == Set({ ud: 0, op: 0 }, { ud: 1, op: 1 }) and cqeFindsItsOwnOp)
      .then(reap)
      .then(reap)
      .expect(inflight == Set() and sl::occupied(sl::slots) == Set())

  /// Why the driver parks a cancelled op instead of removing it. Free the slot
  /// with a completion still owed, insert once more, and the index comes back:
  /// the in-flight CQE now resolves — to the op that took the slot after it.
  run lateCqeAliasesTest =
    init
      .then(pushOp)
      .then(reapEarly)
      .expect(inflight == Set({ ud: 0, op: 0 }) and not(cqeFindsItsOwnOp))
      .then(pushOp)
      // The old CQE still names index 0, and index 0 is now op 1.
      .expect(sl::lookup(sl::slots, 0) == sl::Occupied(1) and not(cqeFindsItsOwnOp))
}
