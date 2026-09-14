// -*- mode: Bluespec -*-
// Step 1: just the submission ring of io_uring.
// We can insert an SQE (`Ring::add`) and flush the ring (`IODriver::submit`).
module io_driver {
  /// How many entries the ring holds. The real one is built with 4096.
  pure val DEPTH = 3

  /// Sockets the operations can name.
  pure val FDS = Set(1, 2)

  /// The listening socket a multishot accept is armed on.
  pure val LISTENER = 9

  /// Slots of the fixed send buffer pool.
  pure val POOL_SLOTS = Set(0, 1)

  /// Other worker threads we can post a MSG_RING to.
  pure val PEERS = Set(1)

  // --------------------------------------------------------------------
  // user_data (io_driver/user_data.rs)
  //
  // Every SQE carries a u64 the kernel copies verbatim into the completion.
  // Its top 2 bits are a tag saying how the remaining 62 bits are read:
  //
  //   bits  0..=59 : slab index      bits 60..=61 : io_kind      62..=63 : tag
  //
  // Same layout here but narrowed to 12/2/2 bits: the widths do not change any
  // of the properties below, and a u64 tag bit overflows the checker's ints.
  // --------------------------------------------------------------------

  pure val KIND_SHIFT = 4096   // 1 << 12, where the kind bits start
  pure val TAG_SHIFT = 16384   // 1 << 14, where the tag bits start
  pure val MAX_INDEX = 4095    // (1 << 12) - 1

  pure val TAG_IO = 0
  pure val TAG_MESSAGE = 1
  pure val TAG_IGNORED = 2
  pure val TAG_MSG_PTR = 3

  /// Kind of an IO op, bits 60..=61, only meaningful under TAG_IO.
  type IoKind =
    // One SQE, one CQE: recv, send, connect...
    | IoSingle
    // One SQE, many CQEs until the kernel drops the F_MORE flag.
    | IoMulti
    // Send from the fixed pool: the index is a pool slot, not a slab token.
    | IoSendBuffer
    // Pointer-free reply-slot wakeup from a peer. Not an IO op at all.
    | IoReplySignal

  /// The tag is what the 62 remaining bits mean, so the whole word is a
  /// variant rather than a record with a spare tag field.
  type UserData =
    // TAG_IO: routed to a slab entry by `index`.
    | Io({ kind: IoKind, index: int })
    // TAG_MESSAGE: 62-bit caller-defined payload from a peer worker.
    | PeerPayload(int)
    // TAG_IGNORED: completion dropped on arrival. Carries whatever bits were
    // there, because `cancel_echo` only overwrites the tag.
    | Ignored(int)
    // TAG_MSG_PTR: 62-bit thin pointer to a heap message we then own.
    | MsgPtr(int)

  pure def kindBits(k: IoKind): int =
    match k {
      | IoSingle => 0
      | IoMulti => 1
      | IoSendBuffer => 2
      | IoReplySignal => 3
    }

  pure def kindOf(bits: int): IoKind =
    if (bits == 0) IoSingle
    else if (bits == 1) IoMulti
    else if (bits == 2) IoSendBuffer
    else IoReplySignal

  /// Pack a user_data into the u64 the SQE carries.
  pure def encode(ud: UserData): int =
    match ud {
      | Io(r) => r.index + kindBits(r.kind) * KIND_SHIFT + TAG_IO * TAG_SHIFT
      | PeerPayload(p) => p + TAG_MESSAGE * TAG_SHIFT
      | Ignored(low) => low + TAG_IGNORED * TAG_SHIFT
      | MsgPtr(p) => p + TAG_MSG_PTR * TAG_SHIFT
    }

  /// What the driver does to every CQE before anything else: read the tag.
  pure def decode(w: int): UserData =
    val low = w % TAG_SHIFT
    val tag = w / TAG_SHIFT
    if (tag == TAG_IO) Io({ kind: kindOf(low / KIND_SHIFT), index: low % KIND_SHIFT })
    else if (tag == TAG_MESSAGE) PeerPayload(low)
    else if (tag == TAG_IGNORED) Ignored(low)
    else MsgPtr(low)

  // The constructors, one per `UserData::*` in Rust.
  pure def single(i: int): UserData = Io({ kind: IoSingle, index: i })
  pure def multi(i: int): UserData = Io({ kind: IoMulti, index: i })
  pure def sendBufferOp(slot: int): UserData = Io({ kind: IoSendBuffer, index: slot })
  pure def replySignal(slot: int): UserData = Io({ kind: IoReplySignal, index: slot })
  pure val ignoredOnly: UserData = Ignored(0)

  /// An AsyncCancel keeps the cancelled op's bits and only flips the tag, so
  /// its own completion is dropped.
  pure def cancelEcho(target: UserData): UserData = Ignored(encode(target) % TAG_SHIFT)

  // --------------------------------------------------------------------
  // SQEs
  // --------------------------------------------------------------------

  /// The operations the driver builds SQEs for.
  type Op =
    | Recv(int)
    | Send(int)
    // Send from the fixed pool: the SQE names the pool slot it borrowed.
    | SendPooled(int)
    // One SQE, a CQE per incoming connection.
    | AcceptMulti(int)
    // IORING_OP_CLOSE, queued by `close_fd`, flushed at the next submit.
    | Close(int)
    // AsyncCancel against another SQE's user_data.
    | Cancel(UserData)
    // MSG_RING: posts `deliver` into another worker's completion ring. Our own
    // send completion is ignored, so the SQE carries two user_data.
    | MsgRing({ peer: int, deliver: UserData })

  /// One submission queue entry: what to do, plus the user_data the kernel
  /// copies back into the completion so the driver knows who it belongs to.
  type Sqe = { ud: UserData, op: Op }

  /// Only these take a slab index, so only these advance the token counter.
  pure def takesSlabIndex(op: Op): bool =
    match op {
      | Recv(_) => true
      | Send(_) => true
      | AcceptMulti(_) => true
      | _ => false
    }

  /// The user_data the driver stamps on each op.
  pure def udFor(op: Op, index: int): UserData =
    match op {
      | Recv(_) => single(index)
      | Send(_) => single(index)
      | AcceptMulti(_) => multi(index)
      | SendPooled(slot) => sendBufferOp(slot)
      | Close(_) => ignoredOnly
      | Cancel(target) => cancelEcho(target)
      | MsgRing(_) => ignoredOnly
    }

  // --------------------------------------------------------------------
  // State
  // --------------------------------------------------------------------

  /// SQEs written into the ring, not submitted yet: the driver's `ring.pending`.
  var sq: List[Sqe]

  /// Everything handed to the kernel so far, in submission order.
  var flushed: List[Sqe]

  /// Next slab index to hand out.
  var nextUd: int

  /// Every SQE ever written, to check flush drops nothing.
  var inserted: int

  action init = all {
    sq' = List(),
    flushed' = List(),
    nextUd' = 0,
    inserted' = 0,
  }

  /// `Ring::add`: write one entry. The ring must have room — `add` unwraps the
  /// push, so a full ring is a panic, not backpressure.
  action insert(op: Op): bool = all {
    sq.length() < DEPTH,
    sq' = sq.append({ ud: udFor(op, nextUd), op: op }),
    nextUd' = if (takesSlabIndex(op)) nextUd + 1 else nextUd,
    inserted' = inserted + 1,
    flushed' = flushed,
  }

  action insertRecv = nondet fd = oneOf(FDS) insert(Recv(fd))
  action insertSend = nondet fd = oneOf(FDS) insert(Send(fd))
  action insertSendPooled = nondet slot = oneOf(POOL_SLOTS) insert(SendPooled(slot))
  action insertAccept = insert(AcceptMulti(LISTENER))
  action insertClose = nondet fd = oneOf(FDS) insert(Close(fd))

  action insertMsgRing =
    nondet peer = oneOf(PEERS)
    nondet payload = oneOf(Set(1, 2))
    insert(MsgRing({ peer: peer, deliver: PeerPayload(payload) }))

  /// Cancel an op we have already written, named by its user_data.
  action insertCancel = all {
    slabUds(sq.concat(flushed)) != Set(),
    nondet target = oneOf(slabUds(sq.concat(flushed)))
    insert(Cancel(target)),
  }

  /// `IODriver::submit`: one syscall hands every pending entry to the kernel
  /// and empties the ring.
  action flush = all {
    sq' = List(),
    flushed' = flushed.concat(sq),
    nextUd' = nextUd,
    inserted' = inserted,
  }

  action step = any {
    insertRecv,
    insertSend,
    insertSendPooled,
    insertAccept,
    insertClose,
    insertCancel,
    insertMsgRing,
    flush,
  }

  // --------------------------------------------------------------------
  // Invariants
  // --------------------------------------------------------------------

  /// The user_data of the ops that own a slab entry: single and multishot.
  pure def slabUds(l: List[Sqe]): Set[UserData] =
    l.foldl(Set(), (acc, e) =>
      match e.ud {
        | Io(r) => if (r.kind == IoSingle or r.kind == IoMulti) acc.union(Set(e.ud)) else acc
        | _ => acc
      })

  pure def slabCount(l: List[Sqe]): int =
    l.foldl(0, (acc, e) => if (takesSlabIndex(e.op)) acc + 1 else acc)

  /// The ring never holds more than it can.
  val ringNeverOverflows = sq.length() <= DEPTH

  /// Nothing is dropped on the way to the kernel: flush moves the whole ring.
  val noEntryLost = sq.length() + flushed.length() == inserted

  /// Two slab-routed SQEs never share a token: a CQE must name exactly one op.
  val udsAreUnique =
    slabUds(sq.concat(flushed)).size() == slabCount(sq.concat(flushed))

  /// A sample of every shape a user_data can take.
  pure val SAMPLE_UDS = Set(
    single(0), single(7), single(MAX_INDEX),
    multi(3), sendBufferOp(1), replySignal(2),
    ignoredOnly, cancelEcho(single(7)), cancelEcho(multi(3)),
    PeerPayload(42), MsgPtr(4096)
  )

  /// Packing then reading the tag back gives the same word.
  pure val userDataRoundTrips =
    SAMPLE_UDS.forall(ud => decode(encode(ud)) == ud)

  /// `send_singleop_cancel_event` cancels by the bare slab token, which only
  /// matches the submitted SQE because TAG_IO and IO_SINGLE are both 0. This is
  /// a compile-time assert in user_data.rs.
  pure val singleOpEncodingIsIdentity =
    Set(0, 1, 7, MAX_INDEX).forall(i => encode(single(i)) == i)

  /// A cancel echo differs from its target in the tag alone.
  pure val cancelEchoKeepsTheBits =
    SAMPLE_UDS.forall(ud =>
      encode(cancelEcho(ud)) % TAG_SHIFT == encode(ud) % TAG_SHIFT)

  /// A mixed batch: a recv, a multishot accept and a close leave together,
  /// and only the first two took a slab index.
  run mixedBatchTest =
    init
      .then(insertRecv)
      .then(insertAccept)
      .then(insertClose)
      .expect(nextUd == 2 and sq.length() == 3)
      .expect(sq[0].ud == single(0) and sq[1].ud == multi(1) and sq[2].ud == ignoredOnly)
      .then(flush)
      .expect(sq == List() and flushed.length() == 3)

  /// Cancelling a recv: the AsyncCancel names the recv's token, and because a
  /// single-op user_data is the identity, that is the bare slab index.
  run cancelEchoTest =
    init
      .then(insertRecv)
      .then(insertCancel)
      .expect(sq[1].ud == Ignored(0) and sq[1].op == Cancel(single(0)))
      .expect(encode(single(0)) == 0)
}
