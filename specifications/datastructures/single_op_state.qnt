// -*- mode: Bluespec -*-
// What one slot of the one-shot slab holds: `SingleOpState` (io_driver/mod.rs).
// `slab.qnt` models the slot, this models the value inside it, and the line
// between them is drawn at removal: an entry here says when it *may* be given
// back, never that it was. `Vacant` is the slab's word for that, and there is
// no second one here.
module single_op_state {
  /// `LinkedSent(u64)` is declared in Rust and never constructed — the
  /// compiler says so — and an unreachable state proves nothing, so it is
  /// left out. The rest is the enum as written.
  type SingleOpState =
    | Init
    | Sent
    // Zero-copy: the byte count is in, the kernel may still be reading the
    // buffer. Carries the stashed result.
    | Notif(int)
    // The result, waiting for the poll that will take it.
    | Ended(int)
    // The shutdown eventfd read. Goes nowhere.
    | Signal
    // Owns the keepalive buffer and the parked fd until the late CQE.
    | Cancelled

  pure def isNotif(s: SingleOpState): bool =
    match s { | Notif(_) => true | _ => false }

  pure def isEnded(s: SingleOpState): bool =
    match s { | Ended(_) => true | _ => false }

  /// The byte count a `Notif` is holding for the notification to deliver.
  pure def stashed(s: SingleOpState): int =
    match s { | Notif(n) => n | _ => 0 }

  /// The states `poll_op` has an arm for. Everything else falls into its
  /// `unreachable!("this should never get polled")`.
  pure def pollable(s: SingleOpState): bool =
    match s {
      | Init => true
      | Sent => true
      | Notif(_) => true
      | Ended(_) => true
      | _ => false
    }

  /// The states a still-held future can cancel from. `Ended` is not one: the
  /// completion beat the drop, so there is nothing to cancel.
  pure def cancellable(s: SingleOpState): bool =
    s == Init or s == Sent or isNotif(s)

  /// Whether the slot may be handed back to the slab — the guard on a
  /// `remove`, and the only thing this module has to say about removal.
  ///
  /// Never while the kernel owes a completion: that is what `Cancelled` is
  /// for, and what makes freeing on cancel a use-after-free rather than a
  /// tidy-up.
  pure def reapable(s: SingleOpState, stillOwed: bool): bool =
    not(stillOwed) and (isEnded(s) or s == Cancelled)

  // --------------------------------------------------------------------
  // State
  // --------------------------------------------------------------------

  var state: SingleOpState

  /// The kernel still owes a completion for this op.
  var owed: bool

  /// Times the waker fired. The zero-copy detour exists to not fire it.
  var woken: int

  action init = all { state' = Init, owed' = true, woken' = 0 }

  /// `push_signal_op` starts its slot here instead.
  action initSignal = all { state' = Signal, owed' = true, woken' = 0 }

  // --------------------------------------------------------------------
  // Events
  //
  // One guarded action per arm rather than one `match`: a match whose arms
  // assign is not an action in Quint, its arms have to agree on their effect.
  // --------------------------------------------------------------------

  /// `poll_op`: `Init` leaves the waker, `Sent` and `Notif` re-clone it. The
  /// fourth arm, the one that reads `Ended`, is a removal — so it is
  /// `reapable` here and a `remove` in whoever owns the slab.
  action poll = all {
    pollable(state),
    not(isEnded(state)),
    state' = if (state == Init) Sent else state,
    owed' = owed,
    woken' = woken,
  }

  /// `process_single`: the completion lands.
  action cqe(more: bool, result: int): bool = any {
    cqeOnSent(more, result),
    cqeOnNotif,
    cqeOnCancelled,
    cqeOnSignal,
  }

  /// `more` is `IORING_CQE_F_MORE` — a zero-copy send whose buffer the kernel
  /// has not released yet, so the result is stashed and the waker left alone.
  action cqeOnSent(more: bool, result: int): bool = all {
    owed,
    state == Sent,
    state' = if (more) Notif(result) else Ended(result),
    woken' = if (more) woken else woken + 1,
    owed' = more,
  }

  /// The notification: the buffer is free, and the count from the first CQE
  /// is what the future is woken with.
  action cqeOnNotif = all {
    owed,
    isNotif(state),
    state' = Ended(stashed(state)),
    woken' = woken + 1,
    owed' = false,
  }

  /// The late CQE for a cancelled op. No waker is left to fire; what it
  /// settles is the debt, which is what makes the slot reapable.
  action cqeOnCancelled = all {
    owed,
    state == Cancelled,
    state' = Cancelled,
    woken' = woken,
    owed' = false,
  }

  /// `SingleProcessResult::Shutdown`: the driver flips a flag and nothing
  /// removes the slot. One entry, held for the life of the process.
  action cqeOnSignal = all {
    owed,
    state == Signal,
    state' = Signal,
    woken' = woken,
    owed' = false,
  }

  /// The future was dropped with the op still in flight —
  /// `cancel_single_op_keep`. The slot stays and takes ownership of what the
  /// kernel may still be reading.
  action cancel = all {
    cancellable(state),
    state' = Cancelled,
    owed' = owed,
    woken' = woken,
  }

  action step = any {
    poll,
    cancel,
    nondet more = oneOf(Set(false, true))
    nondet result = oneOf(Set(1, 2))
    cqe(more, result),
  }

  // --------------------------------------------------------------------
  // Invariants
  // --------------------------------------------------------------------

  /// A result is only final once the kernel owes nothing more — the zero-copy
  /// half-completion goes to `Notif`, never straight to `Ended`.
  val endedMeansNothingIsOwed = isEnded(state) implies not(owed)

  /// Nothing is woken before there is a result to take, which is what the
  /// detour through `Notif` is for.
  val noWakeBeforeAResult =
    match state {
      | Init => woken == 0
      | Sent => woken == 0
      | Notif(_) => woken == 0
      | _ => true
    }

  /// Mirrors the `unreachable!` in `poll_op`: the states with no arm are
  /// exactly the ones no future can reach with a token in hand.
  pure val unpollableStatesAreTheUnreachableOnes =
    not(pollable(Cancelled)) and not(pollable(Signal))

  val opStateInvariants = all {
    endedMeansNothingIsOwed,
    noWakeBeforeAResult,
    unpollableStatesAreTheUnreachableOnes,
  }

  // --------------------------------------------------------------------
  // Runs
  // --------------------------------------------------------------------

  /// The ordinary life: polled once to leave the waker, completed, and the
  /// entry is ready for the poll that removes it.
  run normalLifeTest =
    init
      .then(poll)
      .expect(state == Sent and not(reapable(state, owed)))
      .then(cqe(false, 5))
      .expect(state == Ended(5) and woken == 1 and reapable(state, owed))

  /// Zero-copy: the first completion carries the byte count but not the
  /// buffer's release, so it stashes and does not wake. The notification is
  /// what wakes, with the count from the first.
  run zeroCopyTest =
    init
      .then(poll)
      .then(cqe(true, 5))
      .expect(state == Notif(5) and woken == 0 and owed)
      .then(cqe(false, 0))
      .expect(state == Ended(5) and woken == 1)

  /// Dropping the future does not make the slot reapable: it parks it, still
  /// owed a completion, holding the buffer the kernel may be reading. The
  /// late CQE is what settles the debt.
  run cancelParksTheSlotTest =
    init
      .then(poll)
      .then(cancel)
      .expect(state == Cancelled and owed and not(reapable(state, owed)))
      .then(cqe(false, 1))
      .expect(state == Cancelled and reapable(state, owed))

  /// The other branch of the same cancel: the completion beat the drop, so
  /// there is nothing to cancel and the entry is already reapable.
  run cqeBeatsTheDropTest =
    init
      .then(poll)
      .then(cqe(false, 5))
      .expect(not(cancellable(state)) and reapable(state, owed))

  /// The shutdown slot completes and is never reapable: nothing removes it.
  run signalSlotIsNeverReapedTest =
    initSignal
      .then(cqe(false, 8))
      .expect(state == Signal and not(owed) and not(reapable(state, owed)))
}
