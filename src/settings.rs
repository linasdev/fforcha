use crate::server::settings::FForchaServerSettings;
use crate::watcher::settings::FForchaWatcherSettings;
use serde::Deserialize;

#[derive(Deserialize, Debug)]
pub struct FForchaSettings {
    #[serde(default)]
    pub server: FForchaServerSettings,

    #[serde(default)]
    pub watcher: FForchaWatcherSettings,
}
