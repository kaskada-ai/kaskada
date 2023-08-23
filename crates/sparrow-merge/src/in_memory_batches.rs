use std::sync::RwLock;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::Stream;

use crate::old::homogeneous_merge;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to add in-memory batch")]
    Add,
    #[display(fmt = "receiver lagged")]
    ReceiverLagged,
}

impl error_stack::Context for Error {}

/// Struct for managing in-memory batches.
#[derive(Debug)]
pub struct InMemoryBatches {
    pub schema: SchemaRef,
    current: RwLock<(usize, RecordBatch)>,
    updates: tokio::sync::broadcast::Sender<(usize, RecordBatch)>,
    /// A subscriber that is never used -- it exists only to keep the sender
    /// alive.
    _subscriber: tokio::sync::broadcast::Receiver<(usize, RecordBatch)>,
}

impl InMemoryBatches {
    pub fn new(schema: SchemaRef) -> Self {
        let (updates, _subscriber) = tokio::sync::broadcast::channel(10);
        let merged = RecordBatch::new_empty(schema.clone());
        Self {
            schema,
            current: RwLock::new((0, merged)),
            updates,
            _subscriber,
        }
    }

    /// Add a batch, merging it into the in-memory version.
    ///
    /// Publishes the new batch to the subscribers.
    pub fn add_batch(&self, batch: RecordBatch) -> error_stack::Result<(), Error> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let new_version = {
            let mut write = self.current.write().map_err(|_| Error::Add)?;
            let (version, old) = &*write;
            let version = *version;

            let merged = if old.num_rows() == 0 {
                batch.clone()
            } else {
                homogeneous_merge(&self.schema, vec![old.clone(), batch.clone()])
                    .into_report()
                    .change_context(Error::Add)?
            };

            *write = (version + 1, merged);
            version + 1
        };

        self.updates
            .send((new_version, batch))
            .into_report()
            .change_context(Error::Add)?;
        Ok(())
    }

    /// Create a stream subscribed to the batches.
    ///
    /// The first batch will be the in-memory merged batch, and batches will be
    /// added as they arrive.
    pub fn subscribe(
        &self,
    ) -> impl Stream<Item = error_stack::Result<RecordBatch, Error>> + 'static {
        let (mut version, merged) = self.current.read().unwrap().clone();
        let mut recv = self.updates.subscribe();

        async_stream::try_stream! {
            tracing::info!("Starting subscriber with version {version}");
            yield merged;

            loop {
                match recv.recv().await {
                    Ok((recv_version, batch)) => {
                        if version < recv_version {
                            tracing::info!("Recevied version {recv_version}");
                            yield batch;
                            version = recv_version;
                        } else {
                            tracing::warn!("Ignoring old version {recv_version}");
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        tracing::info!("Sender closed.");
                        break;
                    },
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        Err(Error::ReceiverLagged)?;
                    }
                }
            }
        }
    }

    /// Retrieve the current in-memory batch.
    pub fn current(&self) -> RecordBatch {
        self.current.read().unwrap().1.clone()
    }
}
