# Streams and backpressure

Accept and receive have streaming forms: `MultiAccept` for incoming connections, `MultiRecv` for bytes on a TCP socket, `UdpMultiRecv` for datagrams. Each is a `Stream` you consume with `next().await`, backed by a single multishot submission that the kernel keeps completing on its own. You never resubmit anything, and you never see the mechanism below.

## What happens when the consumer is slower than the producer

A stream has a bounded queue of completions, whose depth you choose at construction and which defaults to 32. Results land in that queue as the kernel produces them, and your task pulls from it as it polls.

- **While the queue has room**, the kernel keeps going and the stream hands results out as fast as you take them.
- **When the queue fills up**, the runtime pauses the multishot. Nothing is dropped and no error is raised: the operation is simply stopped at the kernel level, so the socket's receive buffer starts filling instead, the TCP window closes, and the peer is throttled by the network itself. For accept, connections wait in the listen backlog.
- **When you drain the queue**, the stream re-arms the multishot by itself on the next poll. Reading resumes where it stopped.

So a slow consumer never causes unbounded memory growth on the receiving side. The pressure goes back to where it belongs: the sender.

## Sizing

The queue depth bounds how many completions can be waiting for you, and for receive streams each completion holds one ring buffer. A stream with a depth of 32 on a ring of 4 KiB buffers can therefore hold at most about 128 KiB on your behalf before pausing. Pick the depth per stream with `with_queue_size` when the default does not fit: deeper for a consumer that processes in bursts, shallower when many streams share one ring.

## Buffers you hold are buffers the kernel cannot use

Receive streams deliver borrowed ring buffers, and a buffer goes back to the ring only when you drop it. The queue protects the ring from a slow consumer, but not from a consumer that keeps the buffers it has already taken. If every buffer of a ring is held by your tasks, the kernel has nowhere to put the next bytes, and the stream ends with an `ENOBUFS` error rather than pausing.

Two rules keep this from happening:

- **Drop or copy out quickly.** Treat a borrowed buffer as something to parse and release, not to store. If you need the bytes later, copy them into your own allocation.
- **Size the ring for the streams sharing it.** The ring must have more buffers than the sum of the queue depths of the streams reading from it, plus whatever your tasks hold between polls.
