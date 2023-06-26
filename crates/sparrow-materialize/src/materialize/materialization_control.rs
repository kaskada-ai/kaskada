use std::sync::Arc;

use error_stack::{IntoReport, ResultExt};
use futures::{StreamExt, TryStreamExt};
use sparrow_api::kaskada::v1alpha::ProgressInformation;
use sparrow_runtime::s3::S3Helper;
use tokio::task::JoinHandle;

use crate::{Error, Materialization};

/// The control for a materialization process.
///
/// This handles communication with the materialization process,
/// allowing a user to manipulate the process via the service layer.
pub struct MaterializationControl {
    /// Channel for sending the stop signal
    stop_signal_sender: tokio::sync::watch::Sender<bool>,
    /// Channel for receiving progress information
    progress_receiver: tokio::sync::watch::Receiver<MaterializationStatus>,
    /// Handle to the materialization process
    handle: Option<JoinHandle<error_stack::Result<(), Error>>>,
    /// Status of the materialization process
    status: MaterializationStatus,
}

impl MaterializationControl {
    /// Starts a materialization process
    ///
    /// Kicks off a query and begins producing results to the
    /// destination supplied in the `materialization`.
    pub fn start(
        materialization: Materialization,
        s3_helper: S3Helper,
        bounded_lateness_ns: Option<i64>,
    ) -> Self {
        let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
        let (progress_tx, progress_rx) = tokio::sync::watch::channel(MaterializationStatus {
            state: State::Uninitialized,
            progress: ProgressInformation::default(),
            error: None,
        });

        let handle = tokio::spawn(async move {
            let id = materialization.id.clone();
            let progress_stream =
                Materialization::start(materialization, s3_helper, bounded_lateness_ns, stop_rx)
                    .await?;
            let mut progress_stream = progress_stream.boxed();
            let mut last_progress = ProgressInformation::default();
            while let Some(message) = progress_stream
                .try_next()
                .await
                .change_context(Error::ReadingProgress)?
            {
                if let Some(progress) = message.progress {
                    last_progress = progress.clone();
                    match progress_tx.send(MaterializationStatus {
                        state: State::Running,
                        progress,
                        error: None,
                    }) {
                        Ok(_) => {}
                        Err(e) => {
                            tracing::error!("failed to send progress for materialization: {}, stopping with error: {}", id.clone(), e);
                            break;
                        }
                    };
                }
            }

            match progress_tx.send(MaterializationStatus {
                state: State::Stopped,
                progress: last_progress,
                error: None,
            }) {
                Ok(_) => {
                    tracing::debug!("set materialization (id: {}) status to stopped", id.clone())
                }
                Err(e) => {
                    // At this point, the query should be stopped (or stopping), but
                    // we've failed to update the status.
                    tracing::error!(
                        "failed to set progress to stopped for materialization: {} with error: {}",
                        id.clone(),
                        e
                    );
                }
            }

            tracing::info!("materialization (id: {}) is exiting", id);
            Ok(())
        });

        let status = MaterializationStatus {
            state: State::Uninitialized,
            progress: ProgressInformation::default(),
            error: None,
        };
        Self {
            stop_signal_sender: stop_tx,
            progress_receiver: progress_rx,
            handle: Some(handle),
            status,
        }
    }

    /// Stops a running materialization process.
    pub async fn stop(&mut self) -> error_stack::Result<(), Error> {
        self.stop_signal_sender
            .send(true)
            .into_report()
            .change_context(Error::Internal)?;

        let handle = self.handle.take().expect("handle");
        match &self.status {
            MaterializationStatus {
                state: State::Uninitialized,
                ..
            } => {
                // TODO: It's possible that the materialization process has started
                // and progress has not been reported yet, so we'll report the incorrect
                // status before stopping.
                // We could fix this by reporting progress on shutdown, and having the
                // handle return the latest progress.
                match handle
                    .await
                    .into_report()
                    .change_context(Error::Internal)?
                    .change_context(Error::Materialization)
                {
                    Ok(_) => {
                        self.status = MaterializationStatus {
                            state: State::Stopped,
                            progress: ProgressInformation::default(),
                            error: None,
                        };
                    }
                    Err(e) => {
                        self.status = MaterializationStatus {
                            state: State::Failed,
                            progress: ProgressInformation::default(),
                            error: Some(Arc::new(e)),
                        };
                    }
                }
            }
            MaterializationStatus {
                state: State::Running,
                progress,
                ..
            } => {
                match handle
                    .await
                    .into_report()
                    .change_context(Error::Internal)?
                    .change_context(Error::Materialization)
                {
                    Ok(_) => {
                        self.status = MaterializationStatus {
                            state: State::Stopped,
                            progress: progress.clone(),
                            error: None,
                        };
                    }
                    Err(e) => {
                        self.status = MaterializationStatus {
                            state: State::Failed,
                            progress: progress.clone(),
                            error: Some(Arc::new(e)),
                        };
                    }
                }
            }
            MaterializationStatus {
                state: State::Stopped,
                ..
            } => {
                // Note: Could return an "AlreadyStopped" status code
                tracing::info!("cannot stop; materialization already stopped")
            }
            MaterializationStatus {
                state: State::Failed,
                ..
            } => {
                // Note: Could return an "AlreadyFailed" status code
                tracing::info!("cannot stop; materialization already failed")
            }
        };

        Ok(())
    }

    /// Gets the status of the materialization.
    ///
    /// Note that progress is reported periodically, and may not contain the exact
    /// up-to-date numbers.
    pub fn get_status(&self) -> MaterializationStatus {
        self.progress_receiver.borrow().clone()
    }
}

/// Publish status of a materialization.
#[derive(Clone, Debug, Default)]
pub struct MaterializationStatus {
    /// Progress information
    pub progress: ProgressInformation,
    /// Current state of the materialization
    pub state: State,
    /// Failure reason
    ///
    /// Only populated if the materialization has failed
    pub error: Option<Arc<error_stack::Report<Error>>>,
}

/// Public state of a materialization.
#[derive(Default, Debug, Clone)]
pub enum State {
    #[default]
    Uninitialized,
    Running,
    Stopped,
    Failed,
}
