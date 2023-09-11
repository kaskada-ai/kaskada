use std::sync::RwLock;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use error_stack::{IntoReportCompat, ResultExt};
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
    retained: bool,
    current: RwLock<Current>,
    sender: async_broadcast::Sender<(usize, RecordBatch)>,
    /// A subscriber that is never used -- it exists only to keep the sender
    /// alive.
    _receiver: async_broadcast::InactiveReceiver<(usize, RecordBatch)>,
}

#[derive(Debug)]
struct Current {
    schema: SchemaRef,
    version: usize,
    batch: RecordBatch,
}

impl Current {
    pub fn new(schema: SchemaRef) -> Self {
        let batch = RecordBatch::new_empty(schema.clone());
        Self {
            schema,
            version: 0,
            batch,
        }
    }

    pub fn add_batch(&mut self, batch: &RecordBatch) -> error_stack::Result<(), Error> {
        if self.batch.num_rows() == 0 {
            self.batch = batch.clone();
        } else {
            // This assumes that cloning the old batch is cheap.
            // If it isn't, we could replace it with an empty batch (`std::mem::replace`),
            // put it in an option, or allow `homogeneous_merge` to take `&RecordBatch`.
            self.batch = homogeneous_merge(&self.schema, vec![self.batch.clone(), batch.clone()])
                .into_report()
                .change_context(Error::Add)?;
        }
        Ok(())
    }
}

impl InMemoryBatches {
    pub fn new(retained: bool, schema: SchemaRef) -> Self {
        let (mut sender, receiver) = async_broadcast::broadcast(10);

        // Don't wait for a receiver. If no-one receives, `send` will fail.
        sender.set_await_active(false);

        let current = RwLock::new(Current::new(schema.clone()));
        Self {
            retained,
            current,
            sender,
            _receiver: receiver.deactivate(),
        }
    }

    /// Add a batch, merging it into the in-memory version.
    ///
    /// Publishes the new batch to the subscribers.
    pub async fn add_batch(&self, batch: RecordBatch) -> error_stack::Result<(), Error> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let new_version = {
            let mut write = self.current.write().map_err(|_| Error::Add)?;
            if self.retained {
                write.add_batch(&batch)?;
            }
            write.version += 1;
            write.version
        };

        let send_result = self.sender.broadcast((new_version, batch)).await;
        if send_result.is_err() {
            assert!(!self.sender.is_closed());
            tracing::info!("No-one subscribed for new batch");
        }
        Ok(())
    }

    /// Create a stream subscribed to the batches.
    ///
    /// The first batch will be the in-memory merged batch, and batches will be
    /// added as they arrive.
    pub fn subscribe(
        &self,
    ) -> impl Stream<Item = error_stack::Result<RecordBatch, Error>> + 'static {
        let (mut version, merged) = {
            let read = self.current.read().unwrap();
            (read.version, read.batch.clone())
        };
        let mut recv = self.sender.new_receiver();

        async_stream::try_stream! {
            tracing::info!("Starting subscriber with version {version}");
            if merged.num_rows() > 0 {
                yield merged;
            }

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
                    Err(async_broadcast::RecvError::Closed) => {
                        tracing::info!("Sender closed.");
                        break;
                    },
                    Err(async_broadcast::RecvError::Overflowed(_)) => {
                        Err(Error::ReceiverLagged)?;
                    }
                }
            }
        }
    }

    /// Retrieve the current in-memory batch.
    pub fn current(&self) -> Option<RecordBatch> {
        let batch = self.current.read().unwrap().batch.clone();
        if batch.num_rows() == 0 {
            None
        } else {
            Some(batch)
        }
    }
}
