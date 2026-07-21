use crate::asset::FForchaAsset;
use crate::asset::file::FForchaFileAsset;
use crate::watcher::FForchaWatcher;
use crate::watcher::action::FForchaWatcherAction;
use crate::watcher::error::FForchaWatcherError;
use crate::watcher::file::settings::FForchaFileWatcherSettings;
use async_trait::async_trait;
use futures::stream::{BoxStream, try_unfold};
use futures::{StreamExt, TryStreamExt};
use inotify::{EventMask, Inotify, WatchMask};
use log::{debug, info, warn};
use std::path::PathBuf;
use tokio::fs;

pub mod settings;

pub struct FForchaFileWatcher {
    directory_path: PathBuf,
    file_extensions: Vec<String>,
}

impl FForchaFileWatcher {
    pub fn new(settings: &FForchaFileWatcherSettings) -> Self {
        info!(
            "Creating a new file watcher for (directory_path: {}, file_extensions: {:?})",
            settings.directory_path.display(),
            settings.file_extensions
        );

        Self {
            directory_path: settings.directory_path.clone(),
            file_extensions: settings.file_extensions.clone(),
        }
    }

    async fn get_chained_watcher_action_stream(
        &self,
    ) -> Result<
        BoxStream<'static, Result<FForchaWatcherAction<FForchaFileAsset>, FForchaWatcherError>>,
        FForchaWatcherError,
    > {
        let initial_watcher_action_stream = self
            .get_initial_watcher_action_stream()
            .await?
            .into_stream();
        let inotify_watcher_action_stream = self
            .get_inotify_watcher_action_stream()
            .await?
            .into_stream();
        let chained_watcher_action_stream =
            initial_watcher_action_stream.chain(inotify_watcher_action_stream);
        Ok(chained_watcher_action_stream.boxed())
    }

    async fn get_initial_watcher_action_stream(
        &self,
    ) -> Result<
        BoxStream<'static, Result<FForchaWatcherAction<FForchaFileAsset>, FForchaWatcherError>>,
        FForchaWatcherError,
    > {
        let initial_files = match fs::read_dir(self.directory_path.clone()).await {
            Ok(initial_files) => initial_files,
            Err(error) => {
                warn!("Failed to read directory: {}", error);
                return Err(error.into());
            }
        };

        let initial_watcher_action_stream =
            try_unfold(initial_files, |mut initial_files| async move {
                match initial_files.next_entry().await {
                    Ok(Some(initial_file)) => Ok(Some((
                        FForchaWatcherAction::Queue(FForchaFileAsset::new(initial_file.path())),
                        initial_files,
                    ))),
                    Ok(None) => Ok(None),
                    Err(error) => Err(error.into()),
                }
            });

        Ok(initial_watcher_action_stream.boxed())
    }

    async fn get_inotify_watcher_action_stream(
        &self,
    ) -> Result<
        BoxStream<'static, Result<FForchaWatcherAction<FForchaFileAsset>, FForchaWatcherError>>,
        FForchaWatcherError,
    > {
        let inotify = Inotify::init()?;
        inotify.watches().add(
            self.directory_path.clone(),
            WatchMask::CREATE
                | WatchMask::MODIFY
                | WatchMask::DELETE
                | WatchMask::DELETE_SELF
                | WatchMask::DONT_FOLLOW
                | WatchMask::ONLYDIR,
        )?;

        let inotify_event_stream = inotify.into_event_stream([0; 4096])?;
        let inotify_watcher_action_stream = inotify_event_stream
            .map_err(FForchaWatcherError::IOError)
            .try_filter_map(|event| async move {
                match event.name {
                    Some(event_name) => {
                        let file_asset = FForchaFileAsset::new(PathBuf::from(event_name));
                        match event.mask {
                            mask if mask.contains(EventMask::CREATE) => {
                                Ok(Some(FForchaWatcherAction::Queue(file_asset)))
                            }
                            mask if mask.contains(EventMask::MODIFY) => {
                                Ok(Some(FForchaWatcherAction::ReQueue(file_asset)))
                            }
                            mask if mask.contains(EventMask::DELETE) => {
                                Ok(Some(FForchaWatcherAction::DeQueue(file_asset)))
                            }
                            mask if mask.contains(EventMask::DELETE_SELF) => {
                                Err(FForchaWatcherError::WatchSourceVanished)
                            }
                            mask => {
                                warn!("Inotify event has unknown mask: {:#010X}", mask);
                                Ok(None)
                            }
                        }
                    }
                    None => {
                        warn!("Inotify event has no name");
                        Ok(None)
                    }
                }
            });

        Ok(inotify_watcher_action_stream.boxed())
    }
}

#[async_trait]
impl FForchaWatcher for FForchaFileWatcher {
    async fn watch(
        &self,
    ) -> Result<
        BoxStream<
            'static,
            Result<FForchaWatcherAction<Box<dyn FForchaAsset>>, FForchaWatcherError>,
        >,
        FForchaWatcherError,
    > {
        info!(
            "Starting file watcher for directory: {}",
            self.directory_path.display()
        );

        let file_extensions = self.file_extensions.clone();
        let chained_watcher_action_stream = self.get_chained_watcher_action_stream().await?;
        let filtered_watcher_action_stream =
            chained_watcher_action_stream.try_filter_map(move |watcher_action| {
                let file_extensions = file_extensions.clone();
                async move {
                    match &watcher_action {
                        FForchaWatcherAction::Queue(file_asset)
                        | FForchaWatcherAction::ReQueue(file_asset)
                        | FForchaWatcherAction::DeQueue(file_asset) => {
                            let extension = match file_asset.extension() {
                                Some(extension) => extension,
                                None => return Ok(None),
                            };

                            if !file_extensions.contains(&extension) {
                                debug!("File has a non-watched extension: {extension}");
                                return Ok(None);
                            }

                            Ok(Some(watcher_action.into_boxed()))
                        }
                    }
                }
            });

        Ok(filtered_watcher_action_stream.boxed())
    }
}
