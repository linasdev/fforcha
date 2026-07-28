use serde::Deserialize;
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::time::Duration;

#[derive(Deserialize, Debug)]
pub struct FForchaServerSettings {
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    #[serde(default = "default_shared_secret")]
    pub shared_secret: String,

    #[serde(default = "default_bind_ip")]
    pub bind_ip: IpAddr,

    #[serde(default = "default_bind_port")]
    pub bind_port: u16,

    #[serde(default)]
    pub tls_certificate_path: Option<PathBuf>,

    #[serde(default)]
    pub tls_key_path: Option<PathBuf>,

    #[serde(default = "default_asset_buffer_size")]
    pub asset_buffer_size: usize,

    #[serde(default = "default_worker_timeout")]
    pub worker_timeout: Duration,
}

impl Default for FForchaServerSettings {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            shared_secret: default_shared_secret(),
            bind_ip: default_bind_ip(),
            bind_port: default_bind_port(),
            tls_certificate_path: Default::default(),
            tls_key_path: Default::default(),
            asset_buffer_size: default_asset_buffer_size(),
            worker_timeout: default_worker_timeout(),
        }
    }
}

fn default_enabled() -> bool {
    true
}

fn default_shared_secret() -> String {
    "[CHANGE_THIS]".to_string()
}

fn default_bind_ip() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0))
}

fn default_bind_port() -> u16 {
    8408
}

fn default_asset_buffer_size() -> usize {
    1024
}

fn default_worker_timeout() -> Duration {
    Duration::from_secs(30)
}
