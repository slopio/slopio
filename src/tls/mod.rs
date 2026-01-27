use rustls::{ClientConfig, ServerConfig};

use crate::tls::builder::{TLS_CLIENT_CONFIG, TLS_SERV_CONFIG};

pub mod builder;

#[inline(always)]
pub fn tls_serv_config() -> &'static ServerConfig {
    unsafe { &*TLS_SERV_CONFIG }
}

#[inline(always)]
pub fn tls_client_config() -> &'static ClientConfig {
    unsafe { &*TLS_CLIENT_CONFIG }
}
