//! `SO_INCOMING_CPU` on listeners and UDP sockets: a topology's thread built
//! with `steer_connections` puts its CPU on every one it binds and counts the
//! connections it accepts; any other thread puts nothing.

use std::net::{Ipv4Addr, SocketAddr};
use std::os::fd::RawFd;
use std::sync::{Arc, Mutex};

use slopio::net::{MultiAccept, SteeringStats, UdpSocket, steering_stats};
use slopio::streams::StreamExt;
use slopio::{
    NetOptions, RuntimeOptions, Threads, Topology, WorkerContext, main_thread,
    main_thread_with_options, thread_options,
};

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

fn steered() -> RuntimeOptions {
    RuntimeOptions {
        net: NetOptions {
            steer_connections: true,
        },
        ..Default::default()
    }
}

#[test]
fn a_thread_outside_a_topology_leaves_the_kernels_hash() {
    std::thread::spawn(|| {
        assert_eq!(thread_options(), None, "no runtime yet");
        main_thread(async {
            let options = thread_options().expect("readable while the runtime runs");
            assert_eq!((options.thread_id, options.cpu), (None, None));
            assert!(!options.net.steer_connections);
            let listener = MultiAccept::bind(any_port()).unwrap();
            assert_eq!(so_incoming_cpu(listener.socket().fd()), -1);
            let udp = UdpSocket::bind_addr(any_port()).unwrap();
            assert_eq!(so_incoming_cpu(udp.fd()), -1);
        });
        assert_eq!(thread_options(), None, "cleared once the runtime returns");
    })
    .join()
    .unwrap();
}

#[test]
fn steering_is_refused_on_a_thread_pinned_to_no_cpu() {
    let result =
        std::thread::spawn(|| main_thread_with_options::<(), _>(steered(), async {})).join();
    assert!(result.is_err(), "main_thread has no CPU to steer to");
}

/// A listener a topology's thread bound.
#[derive(Clone, Debug)]
struct Seen {
    /// Whether the thread's group was built with `steer_connections`.
    steered: bool,
    /// The CPUs the thread may run on.
    pinned: Vec<usize>,
    /// The CPU the thread reads in its own options.
    cpu: Option<usize>,
    /// The listener's `SO_INCOMING_CPU`.
    value: i32,
    /// A UDP socket's, bound by the same thread.
    udp: i32,
}

type Log = Arc<Mutex<Vec<Seen>>>;

fn binds_one(log: &Log, steered: bool) -> impl Fn(WorkerContext) -> std::future::Ready<()> + use<> {
    let log = log.clone();
    move |_| {
        let listener = MultiAccept::bind(any_port()).unwrap();
        let udp = UdpSocket::bind_addr(any_port()).unwrap();
        log.lock().unwrap().push(Seen {
            steered,
            pinned: affinity(),
            cpu: thread_options().unwrap().cpu,
            value: so_incoming_cpu(listener.socket().fd()),
            udp: so_incoming_cpu(udp.fd()),
        });
        std::future::ready(())
    }
}

/// Every listener bound by one steered thread in a group, if `group`, and one
/// per worker built with `per_core`.
fn listeners(group: bool, per_core: RuntimeOptions) -> Vec<Seen> {
    let log = Log::default();
    let mut topology = Topology::new();
    if group {
        topology.group::<(), _, _>(Threads::Auto(1), steered(), binds_one(&log, true));
    }
    let steer = per_core.net.steer_connections;
    topology.per_core::<(), _, _>(per_core, binds_one(&log, steer));
    topology.run();
    let seen = log.lock().unwrap().clone();
    assert!(!seen.is_empty(), "no thread ran");
    seen
}

fn assert_steered(seen: &Seen) {
    assert_eq!(seen.pinned.len(), 1, "a thread runs on one CPU: {seen:?}");
    assert_eq!(seen.cpu, Some(seen.pinned[0]), "its options name its CPU");
    assert_eq!(
        seen.value, seen.pinned[0] as i32,
        "the listener asks for it"
    );
    assert_eq!(seen.udp, seen.pinned[0] as i32, "so does the UDP socket");
}

fn assert_not_steered(seen: &Seen) {
    assert_eq!((seen.value, seen.udp), (-1, -1), "{seen:?}");
}

#[test]
fn a_steered_topology_puts_each_threads_cpu_on_its_listener() {
    listeners(false, steered()).iter().for_each(assert_steered);
}

#[test]
fn a_topology_leaves_the_kernels_hash_by_default() {
    listeners(false, RuntimeOptions::default())
        .iter()
        .for_each(assert_not_steered);
}

#[test]
fn each_group_steers_or_not_by_its_own_options() {
    let seen = listeners(true, RuntimeOptions::default());
    assert_eq!(seen.iter().filter(|s| s.steered).count(), 1, "{seen:?}");
    for seen in &seen {
        if seen.steered {
            assert_steered(seen);
        } else {
            assert_not_steered(seen);
        }
    }
}

/// What each worker counts after accepting one connection from itself.
fn stats_after_one_accept(options: RuntimeOptions) -> Vec<SteeringStats> {
    let log = Arc::new(Mutex::new(Vec::new()));
    let seen = log.clone();
    let mut topology = Topology::new();
    topology.per_core::<(), _, _>(options, move |_| {
        let log = log.clone();
        async move {
            let mut listener = MultiAccept::bind(any_port()).unwrap();
            // Over loopback the handshake is processed on the connecting CPU: this one.
            let _client = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
            let _accepted = listener.next().await.unwrap().unwrap();
            log.lock().unwrap().push(steering_stats());
        }
    });
    topology.run();
    let seen = seen.lock().unwrap().clone();
    assert!(!seen.is_empty(), "no worker ran");
    seen
}

#[test]
fn a_steering_thread_counts_the_connections_received_on_its_cpu() {
    let one = SteeringStats {
        accepted: 1,
        on_cpu: 1,
    };
    for stats in stats_after_one_accept(steered()) {
        assert_eq!(stats, one);
    }
}

#[test]
fn a_thread_that_does_not_steer_counts_nothing() {
    for stats in stats_after_one_accept(RuntimeOptions::default()) {
        assert_eq!(stats, SteeringStats::default());
    }
}
