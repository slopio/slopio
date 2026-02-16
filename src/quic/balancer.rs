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

/// Register a thread's socket with the balancer.
/// The first call initialises the eBPF program and attaches it to the socket.
/// Subsequent calls add sockets to the existing balancer.
pub fn register_socket(
    thread_id: u32,
    num_threads: u32,
    fd: RawFd,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut state = QUIC_BALANCER.lock().unwrap();
    match *state {
        QuicBalancerState::Uninit => {
            let balancer = runtime_ebpf_balancer::loader::QuicBalancer::new(num_threads, fd)?;
            *state = QuicBalancerState::Registered {
                inner: balancer,
                threads: 1,
            };
            Ok(())
        }
        QuicBalancerState::Registered {
            ref inner,
            ref mut threads,
        } => {
            inner.add_socket(thread_id, fd)?;
            *threads += 1;
            Ok(())
        }
    }
}

/// Returns `true` if the eBPF balancer has been initialised successfully.
pub(super) fn is_initialised() -> bool {
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
