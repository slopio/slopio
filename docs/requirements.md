# Requirements

The runtime is Linux only. It is built on io_uring and has no fallback path: there is no epoll mode, and it does not run on macOS, Windows or WSL1. WSL2 works if its kernel is recent enough.

## Kernel

The ring is opened with `COOP_TASKRUN`, `SINGLE_ISSUER` and `DEFER_TASKRUN`, so the runtime does not start at all on a kernel that lacks them.

| Kernel | What it unlocks |
|--------|-----------------|
| 6.1 | Baseline. Ring setup, owned-buffer send and receive, provided buffer rings, multishot accept and receive, zero-copy send, registered buffers, cross-worker messaging, timers, file I/O, TCP connect. |
| 6.5 | Waiting on child processes. |

Everything works on 6.5 or later. On 6.1 to 6.4 the runtime works, but waiting on a process fails at submission with `EINVAL` rather than at compile time. Distributions shipping a suitable kernel out of the box include Debian 12 with backports or Debian 13, Ubuntu 24.04, Fedora 40 and later, and any rolling release.

## Permissions and containers

- **io_uring in containers.** Docker and containerd block the io_uring syscalls in their default seccomp profile. Run with a custom profile that allows `io_uring_setup`, `io_uring_enter` and `io_uring_register`, or with `--security-opt seccomp=unconfined`. Kubernetes inherits the container runtime's behaviour.
- **Registered buffers** are pinned memory. Since 5.12 they are charged to the cgroup memory limit rather than `RLIMIT_MEMLOCK`, so no `ulimit` change is needed on a supported kernel, but a tight cgroup limit will refuse large pools.
- **The eBPF balancer** (optional feature) loads a `SK_REUSEPORT` program and needs `CAP_BPF` and `CAP_NET_ADMIN`, or root. Unprivileged BPF is disabled by default on most distributions.

## Toolchain

- **Rust 1.85 or later**, for edition 2024. No nightly needed to build; nightly is only used for the Miri runs described in the test files.
- **clang** with BPF target support, only if the eBPF balancer feature is enabled. It compiles the program at build time.
- The global allocator is mimalloc, pulled in and built by cargo, no system package needed.
