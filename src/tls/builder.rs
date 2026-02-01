use std::{fs::File, io::BufReader, sync::Arc};

use rustls::{
    ClientConfig, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
};

static TLS_SERV_CONFIG: std::sync::OnceLock<Arc<ServerConfig>> = std::sync::OnceLock::new();
static TLS_CLIENT_CONFIG: std::sync::OnceLock<Arc<ClientConfig>> = std::sync::OnceLock::new();

pub fn tls_serv_config() -> Arc<ServerConfig> {
    TLS_SERV_CONFIG
        .get()
        .expect("TLS_SERV_CONFIG not initialized")
        .clone()
}

pub fn tls_client_config() -> Arc<ClientConfig> {
    TLS_CLIENT_CONFIG
        .get()
        .expect("TLS_CLIENT_CONFIG not initialized")
        .clone()
}

fn load_ca(path: &str) -> Vec<CertificateDer<'static>> {
    let file = File::open(path).expect("cannot open CA file");
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .map(|r| r.expect("invalid cert"))
        .collect()
}

fn load_key(path: &str) -> PrivateKeyDer<'static> {
    let file = File::open(path).expect("cannot open key file");
    let mut reader = BufReader::new(file);
    rustls_pemfile::private_key(&mut reader)
        .expect("cannot read key")
        .expect("no key found")
}
fn load_certs(path: &str) -> Vec<CertificateDer<'static>> {
    let file = File::open(path).expect("cannot open cert file");
    let mut reader = BufReader::new(file);
    rustls_pemfile::certs(&mut reader)
        .map(|r| r.expect("invalid cert"))
        .collect()
}

pub fn serv_static_init() {
    let certs = load_certs("certs/cert.pem");
    let key = load_key("certs/key.pem");

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .expect("bad cert/key");
    let _ = TLS_SERV_CONFIG.set(Arc::new(config));
}

pub fn client_static_init() {
    let ca_certs = load_ca("cert.pem");

    let mut root_store = rustls::RootCertStore::empty();
    for cert in ca_certs {
        root_store.add(cert).unwrap();
    }

    let config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let _ = TLS_CLIENT_CONFIG.set(Arc::new(config));
}
