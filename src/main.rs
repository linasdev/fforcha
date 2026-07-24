use config::Config;
use fforcha::error::FForchaError;
use fforcha::runner::FForchaRunner;
use fforcha::settings::FForchaSettings;
use log::info;

#[tokio::main]
async fn main() -> Result<(), FForchaError> {
    env_logger::init();

    let settings: FForchaSettings = Config::builder()
        .add_source(config::File::new("fforcha.toml", config::FileFormat::Toml).required(false))
        .add_source(config::Environment::with_prefix("FFORCHA"))
        .build()
        .expect("Failed to build config")
        .try_deserialize()
        .expect("Failed to deserialize config");

    info!(
        "Config loaded, starting FForcha v{}",
        env!("CARGO_PKG_VERSION")
    );

    FForchaRunner::new(settings).run().await
}
