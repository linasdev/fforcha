use crate::asset::FForchaAsset;
use crate::server::authenticator::FForchaServerAuthenticator;
use crate::server::error::FForchaServerError;
use crate::server::queue::runner::FForchaServerQueueRunner;
use crate::server::settings::FForchaServerSettings;
use crate::server::worker::FForchaServerWorker;
use crate::watcher::action::FForchaWatcherAction;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use log::{info, warn};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{broadcast, mpsc};
use tokio::task::spawn_blocking;
use tokio_rustls::TlsAcceptor;

pub struct FForchaServerRunner {
    settings: FForchaServerSettings,
    server_queue_runner: FForchaServerQueueRunner,
    authenticator: FForchaServerAuthenticator,
}

impl FForchaServerRunner {
    pub fn new(settings: FForchaServerSettings) -> Self {
        let server_queue_runner = FForchaServerQueueRunner::new();
        let authenticator = FForchaServerAuthenticator::new(settings.shared_secret.clone());

        Self {
            settings,
            server_queue_runner,
            authenticator,
        }
    }

    pub async fn run(
        self,
        watcher_action_receiver: mpsc::UnboundedReceiver<
            FForchaWatcherAction<Arc<dyn FForchaAsset>>,
        >,
        mut shutdown_receiver: broadcast::Receiver<()>,
    ) -> Result<(), FForchaServerError> {
        info!("Starting FForcha server runner");

        let server_runner = Arc::new(self);
        let mut workers = FuturesUnordered::new();
        let tls_acceptor = server_runner.clone().prepare_tls_acceptor().await?;
        let tcp_listener = TcpListener::bind((
            server_runner.settings.bind_ip,
            server_runner.settings.bind_port,
        ))
        .await?;

        let server_queue_future = server_runner
            .server_queue_runner
            .run(watcher_action_receiver);

        let server_future = async {
            loop {
                tokio::select! {
                    accept_result = tcp_listener.accept() => match accept_result {
                        Ok((tcp_stream, _)) => {
                            workers.push(FForchaServerWorker::new(tcp_stream, tls_acceptor.clone(), server_runner.server_queue_runner.clone(), server_runner.authenticator.clone(), server_runner.settings.worker));
                        }
                        Err(error) => {
                            warn!("Failed to accept worker connection: {:?}", error);
                        }
                    },
                    Some(Some(worker)) = workers.next(), if !workers.is_empty() => workers.push(worker),
                    _ = shutdown_receiver.recv() => {
                        info!("Received shutdown signal, exiting server runner");
                        break;
                    }
                }
            }
        };

        tokio::select! {
            _ = server_queue_future => Ok(()),
            _ = server_future => Ok(())
        }
    }

    async fn prepare_tls_acceptor(
        self: Arc<Self>,
    ) -> Result<Option<TlsAcceptor>, FForchaServerError> {
        match (
            self.settings.tls_certificate_chain_path.clone(),
            self.settings.tls_private_key_path.clone(),
        ) {
            (Some(certificate_chain_path), Some(private_key_path)) => {
                let (certificate_chain, private_key) = spawn_blocking(|| {
                    let certificate_chain =
                        Self::load_tls_certificate_chain(certificate_chain_path)?;
                    let private_key = Self::load_tls_private_key(private_key_path)?;
                    Ok::<_, FForchaServerError>((certificate_chain, private_key))
                })
                .await
                .expect("Failed to spawn blocking task")?;

                let server_config = rustls::ServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(certificate_chain, private_key)?;

                let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));
                Ok(Some(tls_acceptor))
            }
            (None, None) => Ok(None),
            _ => Err(FForchaServerError::MissingTlsDetails),
        }
    }

    fn load_tls_certificate_chain(
        path: PathBuf,
    ) -> Result<Vec<CertificateDer<'static>>, FForchaServerError> {
        let file = std::fs::File::open(path)?;
        let mut buffer_reader = std::io::BufReader::new(file);
        let certificate_chain =
            rustls_pemfile::certs(&mut buffer_reader).collect::<Result<Vec<_>, _>>()?;
        Ok(certificate_chain)
    }

    fn load_tls_private_key(path: PathBuf) -> Result<PrivateKeyDer<'static>, FForchaServerError> {
        let file = std::fs::File::open(path)?;
        let mut buffer_reader = std::io::BufReader::new(file);
        let private_key = rustls_pemfile::private_key(&mut buffer_reader)?
            .ok_or(FForchaServerError::FailedToLoadPrivateKey)?;
        Ok(private_key)
    }
}
