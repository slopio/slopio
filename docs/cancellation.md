# Cancelling I/O

Every I/O operation in the runtime is a future, and dropping that future cancels the operation. There is no separate cancel call: wrapping an operation in `timeout`, racing it with `select`, or simply letting it go out of scope is enough. Whatever the kernel was still doing with it is cancelled and cleaned up.

What you can rely on when an operation is dropped mid-flight:

- **Your memory is never touched after you let go of it.** The buffer you handed to the operation is kept alive by the runtime until the kernel has confirmed it is done with it, then freed. This is why send and receive take buffers by value: ownership is what makes dropping safe.
- **Ring buffers go back to their ring.** A receive on a provided buffer ring, whether single or streaming, returns every buffer it was holding, including results that had arrived but that you had not read yet. Nothing leaks out of the pool.
- **Streams stop cleanly.** Dropping a multishot accept or receive stops the stream. Connections accepted between the drop and the kernel acknowledging it are closed rather than lost.
- **The socket stays open as long as needed.** Dropping an operation and then the socket does not close the descriptor while the kernel might still report on it. The close happens once every operation on that socket has been fully accounted for, so no other socket can be handed the same descriptor while an old operation is still in flight.
- **Cancellation is a no-op if the operation already completed.** A future whose result has landed but that you never polled drops without side effects, the same way as one that never started.

What cancellation does not give you:

- **The buffer is not returned to you.** A buffer is handed back only when the operation completes normally. If you drop the future early, the runtime takes ownership of it and frees it once the kernel is done with it. Nothing dangles and nothing leaks; you simply do not get the allocation back for reuse, so allocate a new one.
- **Partial progress is unknown.** A cancelled send may have transmitted some, all or none of its bytes. If you need to know, wait for the completion instead of cancelling, or use a protocol that tolerates it.
- **Cancellation is asynchronous.** Dropping the future submits the cancel; the kernel acknowledges it on a later turn of the event loop. You do not need to wait for anything, but the resources are released slightly after the drop, not at the drop.

## A known gap

A task that is parked on an I/O operation when the runtime shuts down is not cancelled: its operation and its buffer are leaked. The runtime abandons them rather than closing descriptors the kernel may still be using. Close your streams and let your tasks finish before returning from the root future if this matters to you.
