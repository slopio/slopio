# slopio-ebpf-balancer

An eBPF `SO_REUSEPORT` program that keeps each QUIC connection on the core that owns it, for the [slopio](https://crates.io/crates/slopio) runtime.

Each worker encodes its index in the connection IDs it hands out; the program reads it from every datagram and hands the datagram to that worker's socket, so a connection stays on one core, migration included.

Use it through slopio's `ebpf_balancer` feature rather than directly. Building it needs `clang` and the libbpf headers; loading the program needs `CAP_BPF` and `CAP_NET_ADMIN`.

MIT, see [LICENSE](LICENSE).
