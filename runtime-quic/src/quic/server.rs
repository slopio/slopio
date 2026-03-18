use std::{cell::UnsafeCell, rc::Rc};

use tquic::h3::Http3Config;

use crate::{
    quic::config::{QuicOutput, Tls},
    quic::{Http3ServerConfig, HttpHandler, stream::StreamState, stream::stream_slab},
};
use runtime_core::net::UdpSocket;

use super::connection::{H3Handler, HandlerBridge};

pub(super) struct H3State {
    pub(super) handler: H3Handler,
    pub(super) endpoint: Option<tquic::Endpoint>,
    pub(super) request_handler: Option<Box<dyn HttpHandler>>,
}

impl H3State {
    fn new() -> Self {
        Self {
            handler: H3Handler::new(),
            endpoint: None,
            request_handler: None,
        }
    }
}

thread_local! {
    static H3_STATE: UnsafeCell<H3State> = UnsafeCell::new(H3State::new());
}

pub(super) fn with_state<F, R>(f: F) -> R
where
    F: FnOnce(&mut H3State) -> R,
{
    H3_STATE.with(|cell| f(unsafe { &mut *cell.get() }))
}

pub struct H3Server {
    socket: UdpSocket,
    config: Rc<Http3ServerConfig>,
}

impl H3Server {
    pub fn build(server_config: Http3ServerConfig) -> Result<Self, super::Http3ServerError> {
        let udp = UdpSocket::bind_addr(server_config.bind_addr)?;

        let multithread = server_config.num_threads > 1;

        if let Err(e) = super::balancer::register_socket(
            server_config.thread_id as u32,
            server_config.num_threads as u32,
            udp.fd(),
        ) && multithread
        {
            eprintln!("eBPF balancer unavailable, falling back to SO_REUSEPORT: {e}");
        }

        let mut config = Box::new(tquic::Config::new()?);
        let tls = Tls::new(&server_config.cert_path, &server_config.key_path)?;

        config.set_max_idle_timeout(server_config.idle_timeout.as_millis() as u64);
        config.set_initial_max_streams_bidi(server_config.max_concurrent_streams);
        config.set_cid_len(8);
        config.set_tls_config_selector(std::sync::Arc::new(tls));

        let server_config = Rc::new(server_config);

        let h3_config = Http3Config::new().map_err(super::Http3ServerError::from)?;
        with_state(|state| {
            state.handler.config = Some(server_config.clone());
            state.handler.h3_config = Some(h3_config);
        });

        let sender = Rc::new(QuicOutput { fd: udp.fd() });
        let mut endpoint = tquic::Endpoint::new(config, true, Box::new(HandlerBridge), sender);

        if multithread && super::balancer::is_initialised() {
            let cid_gen = super::balancer::CidGenerator(
                runtime_ebpf_balancer::id_generation::ThreadAwareCidGenerator::new(
                    server_config.thread_id as u8,
                    8,
                ),
            );
            endpoint.set_cid_generator(Box::new(cid_gen));
        }

        with_state(|state| {
            state.endpoint = Some(endpoint);
        });

        Ok(Self {
            socket: udp,
            config: server_config,
        })
    }

    pub async fn run<H>(self, handler: H)
    where
        H: HttpHandler,
    {
        with_state(|state| state.request_handler = Some(Box::new(handler)));
        self.run_inner().await;
    }

    async fn run_inner(self) {
        use runtime_streams::StreamExt;
        use std::time::Instant;
        use tquic::PacketInfo;

        use runtime_core::net::BufGroup;
        use runtime_core::net::UdpMultiRecv;
        use runtime_core::runtime::select::{SelectResult, select};
        use runtime_core::runtime::time::Sleep;

        let mut recv = UdpMultiRecv::new(self.socket.fd(), BufGroup::Small);
        loop {
            let timeout_dur =
                with_state(|state| state.endpoint.as_ref().and_then(|ep| ep.timeout()));

            let got_datagram = match timeout_dur {
                Some(dur) => match select(recv.next(), Sleep::during(dur)).await {
                    SelectResult::First(datagram) => datagram,
                    SelectResult::Second(()) => {
                        with_state(|state| {
                            let Some(endpoint) = &mut state.endpoint else {
                                return;
                            };
                            endpoint.on_timeout(Instant::now());
                            let _ = endpoint.process_connections();
                            state.handler.process_http3(endpoint);
                        });
                        continue;
                    }
                },
                None => recv.next().await,
            };

            let Some(datagram) = got_datagram else {
                break;
            };

            let info = PacketInfo {
                src: datagram.addr(),
                time: Instant::now(),
                dst: self.config.bind_addr,
            };

            with_state(|state| {
                let Some(endpoint) = &mut state.endpoint else {
                    return;
                };
                if let Err(e) = endpoint.recv(&mut datagram.payload().to_vec(), &info) {
                    eprintln!("QUIC recv error: {e}");
                    return;
                }
                if let Err(e) = endpoint.process_connections() {
                    eprintln!("QUIC process_connections error: {e}");
                }
                state.handler.process_http3(endpoint);
            });
        }
    }
}

pub(super) fn try_store_headers(headers: &[tquic::h3::Header], fin: bool) -> Option<usize> {
    let state = StreamState::try_from((headers, fin)).ok()?;
    Some(stream_slab(|slab| slab.insert(state)))
}
