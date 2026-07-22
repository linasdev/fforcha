use crate::asset::FForchaAsset;
use crate::asset::file::FForchaFileAsset;
use crate::watcher::FForchaWatcher;
use crate::watcher::action::FForchaWatcherAction;
use crate::watcher::error::FForchaWatcherError;
use crate::watcher::file::settings::FForchaFileWatcherSettings;
use async_trait::async_trait;
use async_walkdir::WalkDir;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use inotify::{EventMask, Inotify, WatchMask};
use log::{debug, info, warn};
use std::path::PathBuf;

pub mod settings;

pub struct FForchaFileWatcher {
    directory_path: PathBuf,
    file_extensions: Vec<String>,
    ignore_hidden_files: bool,
    follow_links: bool,
}

impl FForchaFileWatcher {
    pub fn new(settings: &FForchaFileWatcherSettings) -> Self {
        info!("Creating a new file watcher with settings: {settings:?}");

        Self {
            directory_path: settings.directory_path.clone(),
            file_extensions: settings.file_extensions.clone(),
            ignore_hidden_files: settings.ignore_hidden_files,
            follow_links: settings.follow_links,
        }
    }

    async fn get_chained_watcher_action_stream(
        &self,
    ) -> Result<
        BoxStream<'static, Result<FForchaWatcherAction<FForchaFileAsset>, FForchaWatcherError>>,
        FForchaWatcherError,
    > {
        let initial_watcher_action_stream = self.get_initial_watcher_action_stream().await;
        let inotify_watcher_action_stream = self.get_inotify_watcher_action_stream().await?;
        let chained_watcher_action_stream =
            initial_watcher_action_stream.chain(inotify_watcher_action_stream);
        Ok(chained_watcher_action_stream.boxed())
    }

    async fn get_initial_watcher_action_stream(
        &self,
    ) -> BoxStream<'static, Result<FForchaWatcherAction<FForchaFileAsset>, FForchaWatcherError>>
    {
        WalkDir::new(self.directory_path.clone())
            .map_err(FForchaWatcherError::WalkDir)
            .try_filter_map(|dir_entry| async move {
                let file_asset_path = dir_entry.path().to_path_buf();

                Ok(Some(FForchaWatcherAction::Queue(FForchaFileAsset::new(
                    file_asset_path,
                ))))
            })
            .boxed()
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

        let directory_path = self.directory_path.clone();
        let inotify_event_stream = inotify.into_event_stream([0; 4096])?;
        let inotify_watcher_action_stream = inotify_event_stream
            .map_err(FForchaWatcherError::IO)
            .try_filter_map(move |event| {
                let directory_path = directory_path.clone();
                async move {
                    if event.mask.contains(EventMask::DELETE_SELF) {
                        return Err(FForchaWatcherError::WatchSourceVanished);
                    }

                    match event.name {
                        Some(event_name) => {
                            let file_asset_path = directory_path.join(PathBuf::from(event_name));
                            let file_asset = FForchaFileAsset::new(file_asset_path);
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
        let ignore_hidden_files = self.ignore_hidden_files;
        let follow_links = self.follow_links;
        let chained_watcher_action_stream = self.get_chained_watcher_action_stream().await?;
        let filtered_watcher_action_stream =
            chained_watcher_action_stream.try_filter_map(move |watcher_action| {
                let file_extensions = file_extensions.clone();
                async move {
                    match &watcher_action {
                        FForchaWatcherAction::Queue(file_asset)
                        | FForchaWatcherAction::ReQueue(file_asset)
                        | FForchaWatcherAction::DeQueue(file_asset) => {
                            let file_asset_path = file_asset.path();
                            let extension = match file_asset.extension() {
                                Some(extension) => extension,
                                None => return Ok(None),
                            };
                            let is_hidden = match file_asset.is_hidden() {
                                Some(is_hidden) => is_hidden,
                                None => return Ok(None),
                            };
                            let canonical_path = match file_asset_path.canonicalize() {
                                Ok(canonical_path) => canonical_path,
                                Err(error) => {
                                    warn!(
                                        "Failed to canonicalize file path '{}' with error: {}",
                                        file_asset_path.display(),
                                        error
                                    );
                                    return Ok(None);
                                }
                            };

                            if !file_extensions.contains(&extension) {
                                debug!(
                                    "Ignoring file with non-watched extension: {}",
                                    file_asset_path.display()
                                );
                                return Ok(None);
                            }

                            if ignore_hidden_files && is_hidden {
                                debug!("Ignoring hidden file: {}", file_asset_path.display());
                                return Ok(None);
                            }

                            if !follow_links && file_asset_path.is_symlink() {
                                debug!("Ignoring symlink: {}", file_asset_path.display());
                                return Ok(None);
                            }

                            Ok(Some(
                                watcher_action
                                    .map(|_| FForchaFileAsset::new(canonical_path))
                                    .into_boxed(),
                            ))
                        }
                    }
                }
            });

        Ok(filtered_watcher_action_stream.boxed())
    }
}
