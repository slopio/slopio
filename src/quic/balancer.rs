use std::os::fd::RawFd;
use std::sync::Mutex;

use rand::RngCore;
use tquic::{ConnectionId, ConnectionIdGenerator, MAX_CID_LEN};

static QUIC_BALANCER: Mutex<QuicBalancerState> = Mutex::new(QuicBalancerState::Uninit);

enum QuicBalancerState {
    Uninit,
    Registered {
        inner: runtime_ebpf_balancer::loader::QuicBalancer,
        threads: u32,
    },
}

/// Initialise the global eBPF QUIC balancer.
/// Call **once** from the main thread before spawning workers.
pub fn init_quic_balancer(
    num_threads: u32,
    first_socket_fd: RawFd,
) -> Result<(), Box<dyn std::error::Error>> {
    let balancer = runtime_ebpf_balancer::loader::QuicBalancer::new(num_threads, first_socket_fd)?;
    let mut state = QUIC_BALANCER.lock().unwrap();
    match *state {
        QuicBalancerState::Uninit => {
            *state = QuicBalancerState::Registered {
                inner: balancer,
                threads: 0,
            };
            Ok(())
        }
        QuicBalancerState::Registered { .. } => Err("QuicBalancer already initialised".into()),
    }
}

/// Register a thread's socket with the balancer.
/// Returns the current number of registered threads (after this registration).
pub fn register_socket(thread_id: u32, fd: RawFd) -> Result<u32, Box<dyn std::error::Error>> {
    let mut state = QUIC_BALANCER.lock().unwrap();
    match *state {
        QuicBalancerState::Uninit => Err("QuicBalancer not initialised".into()),
        QuicBalancerState::Registered {
            ref inner,
            ref mut threads,
        } => {
            inner.add_socket(thread_id, fd)?;
            *threads += 1;
            Ok(*threads)
        }
    }
}

/// Get the number of threads that have registered their socket.
/// Returns `None` if the balancer has not been initialised.
pub fn registered_threads() -> Option<u32> {
    let state = QUIC_BALANCER.lock().unwrap();
    match *state {
        QuicBalancerState::Uninit => None,
        QuicBalancerState::Registered { threads, .. } => Some(threads),
    }
}

/// Returns `true` if the balancer has been initialised.
pub fn is_initialised() -> bool {
    let state = QUIC_BALANCER.lock().unwrap();
    matches!(*state, QuicBalancerState::Registered { .. })
}

pub(super) struct CidGenerator(pub runtime_ebpf_balancer::id_generation::ThreadAwareCidGenerator);

impl ConnectionIdGenerator for CidGenerator {
    fn generate(&mut self) -> ConnectionId {
        let mut buf = [0u8; MAX_CID_LEN];
        let mut random = [0u8; MAX_CID_LEN];
        rand::thread_rng().fill_bytes(&mut random);
        self.0.generate(&mut buf, &random);
        ConnectionId::new(&buf[..self.0.cid_len()])
    }

    fn cid_len(&self) -> usize {
        self.0.cid_len()
    }
}
