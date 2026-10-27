//! `SO_INCOMING_CPU` on listeners: what a thread sets, the listeners it binds
//! carry, and a topology sets it to the CPU it pins each thread to unless
//! told not to.

use std::net::{Ipv4Addr, SocketAddr};
use std::os::fd::RawFd;
use std::sync::{Arc, Mutex};

use runtime::net::{MultiAccept, incoming_cpu, set_incoming_cpu};
use runtime::{RingOptions, Topology, main_thread};

/// The kernel's value for the socket, -1 when nothing set it.
fn so_incoming_cpu(fd: RawFd) -> i32 {
    let mut value: libc::c_int = 0;
    let mut len = size_of::<libc::c_int>() as libc::socklen_t;
    let result = unsafe {
        libc::getsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_INCOMING_CPU,
            (&mut value as *mut libc::c_int).cast(),
            &mut len,
        )
    };
    assert_eq!(result, 0, "getsockopt: {}", std::io::Error::last_os_error());
    value
}

fn any_port() -> SocketAddr {
    SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
}

/// The CPUs the calling thread may run on.
fn affinity() -> Vec<usize> {
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        assert_eq!(
            libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set),
            0
        );
        (0..libc::CPU_SETSIZE as usize)
            .filter(|&c| libc::CPU_ISSET(c, &set))
            .collect()
    }
}

#[test]
fn a_listener_carries_its_threads_incoming_cpu() {
    std::thread::spawn(|| {
        main_thread(async {
            // Off unless set: a thread the runtime did not pin.
            assert_eq!(incoming_cpu(), None);
            let plain = MultiAccept::bind(any_port()).unwrap();
            assert_eq!(so_incoming_cpu(plain.socket().fd()), -1);

            set_incoming_cpu(Some(1));
            let steered = MultiAccept::bind(any_port()).unwrap();
            assert_eq!(so_incoming_cpu(steered.socket().fd()), 1);

            set_incoming_cpu(None);
            let again = MultiAccept::bind(any_port()).unwrap();
            assert_eq!(so_incoming_cpu(again.socket().fd()), -1);
        })
    })
    .join()
    .unwrap();
}

/// Each worker's listener: the CPU it was pinned to, and that CPU's value of
/// `SO_INCOMING_CPU`.
fn workers_listeners(steer: bool) -> Vec<(Vec<usize>, i32)> {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let mut topology = Topology::new();
    topology.steer_connections(steer);
    topology.per_core::<(), _, _>(RingOptions::default(), move |_ctx| {
        let log = log.clone();
        async move {
            let listener = MultiAccept::bind(any_port()).unwrap();
            let value = so_incoming_cpu(listener.socket().fd());
            log.lock().unwrap().push((affinity(), value));
        }
    });
    topology.run();
    let seen = seen.lock().unwrap().clone();
    assert!(!seen.is_empty(), "no worker ran");
    seen
}

#[test]
fn a_topology_steers_each_workers_listener_to_its_cpu() {
    for (pinned, value) in workers_listeners(true) {
        assert_eq!(pinned.len(), 1, "a worker runs on one CPU: {pinned:?}");
        assert_eq!(
            value, pinned[0] as i32,
            "the listener asks for its worker's CPU"
        );
    }
}

#[test]
fn a_topology_told_not_to_steer_leaves_the_kernels_hash() {
    for (_, value) in workers_listeners(false) {
        assert_eq!(value, -1);
    }
}
