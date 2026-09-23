# Streams and backpressure

Accept and receive have streaming forms: `MultiAccept` for incoming connections, `MultiRecv` for bytes on a TCP socket, `UdpMultiRecv` for datagrams. Each is a `Stream` you consume with `next().await`, backed by a single multishot submission that the kernel keeps completing on its own. You never resubmit anything, and you never see the mechanism below.

## Choosing what a listener listens on

`MultiAccept::new(port)` listens on `0.0.0.0:port`, IPv4 only. To pick the address yourself, use `MultiAccept::bind(addr)` with any `SocketAddr`.

A listener bound to a v6 address serves IPv6 only, whatever the host's `bindv6only` setting: peers arrive as real `SocketAddr::V6`, and the same port stays free for a separate v4 listener. Bind two listeners if you want to serve both families.

If you would rather have one socket for both, `MultiAccept::bind_dual_stack(port)` listens on `[::]:port` and accepts v4 clients too — but their `peer_addr()` is then a v4-mapped address such as `::ffff:127.0.0.1`, so compare addresses with `to_canonical()` rather than against an `Ipv4Addr`.

Binding to port 0 lets the kernel pick a free port; `local_addr()` tells you which one. Each of these has a `*_with_queue_size` form when you also want to set the queue depth described below.

## What happens when the consumer is slower than the producer

A stream has a bounded queue of completions, whose depth you choose at construction and which defaults to 32. Results land in that queue as the kernel produces them, and your task pulls from it as it polls.

- **While the queue has room**, the kernel keeps going and the stream hands results out as fast as you take them.
- **When the queue fills up**, the runtime pauses the multishot. Nothing is dropped and no error is raised: the operation is simply stopped at the kernel level, so the socket's receive buffer starts filling instead, the TCP window closes, and the peer is throttled by the network itself. For accept, connections wait in the listen backlog.
- **When you drain the queue**, the stream re-arms the multishot by itself on the next poll. Reading resumes where it stopped.

So a slow consumer never causes unbounded memory growth on the receiving side. The pressure goes back to where it belongs: the sender.

## Sizing

The queue depth bounds how many completions can be waiting for you, and for receive streams each completion holds one ring buffer. A stream with a depth of 32 on a ring of 4 KiB buffers can therefore hold at most about 128 KiB on your behalf before pausing. Pick the depth per stream with `with_queue_size` when the default does not fit: deeper for a consumer that processes in bursts, shallower when many streams share one ring.

## Accept errors

`MultiAccept` yields `Result<SocketWrapper, AcceptError>`. Errors that concern one connection only, such as a client that gave up before being accepted, are skipped and never reach you. Running out of descriptors or memory (`EMFILE`, `ENFILE`, `ENOBUFS`, `ENOMEM`) is reported, and the listener stays open: the next `next().await` accepts again. Back off a little before polling after one of these, since the process has to release something first. Any other error is reported once, and the stream then ends.

## Buffers you hold are buffers the kernel cannot use

Receive streams deliver borrowed ring buffers, and a buffer goes back to the ring only when you drop it. The queue protects the ring from a slow consumer, but not from a consumer that keeps the buffers it has already taken. If every buffer of a ring is held by your tasks, the kernel has nowhere to put the next bytes, and the stream yields an `ENOBUFS` error rather than pausing. The stream is not over: poll it again once you have released buffers and it resumes where it stopped.

Two rules keep this from happening:

- **Drop or copy out quickly.** Treat a borrowed buffer as something to parse and release, not to store. If you need the bytes later, copy them into your own allocation.
- **Size the ring for the streams sharing it.** The ring must have more buffers than the sum of the queue depths of the streams reading from it, plus whatever your tasks hold between polls.
