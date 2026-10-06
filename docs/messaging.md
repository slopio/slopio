# Messaging between workers

Under `thread_per_core` each worker owns its tasks, its sockets and its buffers, and shares none of it. Workers talk by sending messages to each other through io_uring itself: a message is posted straight onto the destination worker's completion queue, so the receiver wakes up the same way it does for any I/O completion, even if it was blocked in the kernel with nothing else to do. There are no locks, no shared queues and no atomics for you to manage.

Messaging only exists under `thread_per_core` or a [topology](topology.md), where every thread of every group is a peer. Calling any of these functions under `main_thread` panics.

## Addressing

Every worker has an index from `0` to `num_threads - 1`. Your worker closure receives both in its `WorkerContext`, and `thread_id()` returns the current one from anywhere inside a task. Under a topology, the groups take the first indices in declaration order and the per-core workers the last ones; see [addressing threads](topology.md#addressing-threads). A message is addressed to a worker, not to a task: each worker has one inbox, and whichever task on that worker is awaiting it gets the next message.

Messages from one worker to another arrive in the order they were sent.

## Four ways to send

**A raw message.** `send_to_peer(peer, data, payload)` carries an `i32` and a 62-bit integer, nothing else, and allocates nothing. The peer gets them with `recv_from_peer().await`. For signals, counters and indices into something the peer already owns.

**A typed message, handed over.** `send_msg_to_peer(peer, value)` sends any `Send + 'static` value. The receiver gets it as an `Arc<Message<T>>` with `recv_msg_from_peer::<T>().await` and, since the sender kept nothing, can take the payload out with `Arc::into_inner`. The value itself is never copied: one allocation, one pointer across.

**A typed message, shared.** Build the message yourself with `Message::new`, keep a clone, and send with `send_shared_to_peer`. The sender can keep reading the payload after it has crossed, and can fan the same allocation out to several peers without allocating per peer. Only `Send` is required of the payload, so the runtime does not check that sharing it is sound. It is your job to make it so, either by sending a `Sync` type or by arranging your own discipline over a `!Sync` one.

**A request with a reply.** `request_msg_from_peer::<Req, Rep>(peer, req)` sends a request and returns a future for the reply. On the other side, `reply_msg_to_peer(&req.header, rep)` sends the answer back. The reply is routed straight to the waiting future and never touches the peer's inbox, so request and reply traffic do not interfere with whatever else the worker receives.

```rust
// worker 0
let reply = unsafe { request_msg_from_peer::<Req, Rep>(1, Req { n: 7 }) }.await;

// worker 1
let req = unsafe { recv_msg_from_peer::<Req>() }.await;
reply_msg_to_peer(&req.header, Rep { n: req.payload.n + 1 });
```

Replying with a signal instead of a message is possible when the requester still holds the data it asked about: reserve a `ReplySignal`, send a shared message carrying its slot in the header, and have the peer answer with `signal_reply_to_peer(&header, status)`. The round trip back allocates nothing and carries only an `i32`.

## Why receiving is `unsafe`

The inbox is untyped. A worker receives a pointer and casts it to the type it expects; nothing at runtime checks that the sender agreed. Receiving a `Message<T>` when a `Message<U>` was sent is undefined behaviour, which is why `recv_msg_from_peer` and `request_msg_from_peer` are `unsafe`: calling them asserts that every worker in the program follows the same protocol.

This is not a gap in the runtime, it is the design. The check is pushed to the program because a runtime tag would cost every message and would still only turn a bug into a panic. In practice the discipline is simple:

- Decide one message type per worker inbox, typically an `enum`, and receive only that type on that worker.
- Never answer a request with `send_msg_to_peer`. It lands in the inbox, not in the reply slot, and the requester waits forever with no error.
- Pass the header you received to `reply_msg_to_peer`, never one you built.

## Waiting and giving up

A reply future has no timeout. If the peer never answers it stays pending forever, so race it against a `Sleep` with `select` when the peer may be busy or gone. Dropping the future early is safe: the slot stays reserved until the late reply lands, and the reply is then freed rather than leaked or misdelivered to a new request that reused the slot.

## Cost

Sending is one entry on the local submission queue; nothing is copied, nothing is locked, and the receiver is woken by the kernel as part of its normal completion processing. A message that would otherwise need a channel, a mutex and a condition variable costs one submission and one completion.
