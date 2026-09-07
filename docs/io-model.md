# Ways of doing I/O

The runtime offers several ways to do I/O. You pick one per call, not per socket, and they all work on TCP, UDP and files wherever the underlying io_uring operation exists.

- **Bring your own buffer.** Pass a `Vec<u8>`, a `Box<[u8]>` or a static slice to a send or receive call and get it back when the operation completes. Nothing is copied, and you decide how buffers are allocated and reused.
- **Provided buffer rings.** Declare a ring with `define_buf_ring!`, giving it an id, a buffer size and a ring size. Receive calls made against that ring let the kernel pick the buffer, and you get a borrowed buffer that returns to the ring when you drop it. Several rings with different sizes can coexist, so small control messages and large payloads each get their own pool. A borrowed buffer is also a valid send buffer, so an echo or a proxy can send received bytes straight back out without a copy. This is a niche case: a buffer being sent is a buffer the kernel cannot receive into, so keep such sends short-lived and size the ring for them, and copy out instead whenever the bytes have to be held for any length of time.
- **Multishot.** Accept and receive have streaming variants where a single submission keeps delivering results as they arrive, instead of one submission per result.
- **Registered buffers.** A pool of memory is pinned with the ring once, and sends from it are issued as fixed or zero-copy operations with no per-call setup.
- **Zero-copy send.** Any owned buffer can also be sent zero-copy, for large payloads where the copy into kernel memory is what costs.

Everything is chosen at the call site: which ring, which buffer type, whether to stream or receive once, whether to copy or not.

## Why there is no single I/O interface

A single `AsyncRead`/`AsyncWrite`-style trait would have to settle on one contract for who owns the buffer and when it is handed back. That contract is the lowest common denominator, a borrowed `&mut [u8]` filled by a copy, and it hides precisely what io_uring makes possible: kernel-chosen buffers, operations that outlive a single poll, memory registered once and reused. Each way of doing I/O above has different ownership and different costs, and the runtime's rule is that neither a copy nor an allocation should ever happen without the caller asking for it. So the choice is explicit in the function you call, and the type you get back tells you exactly what you own.
