use std::sync::{Arc, RwLock};

use arrow_array::RecordBatch;
use arrow_schema::{DataType, SchemaRef};
use error_stack::{IntoReportCompat, ResultExt};
use futures::{Stream, StreamExt, TryStreamExt};

use sparrow_batch::Batch;
use sparrow_interfaces::source::{Source, SourceError};
use sparrow_interfaces::ExecutionOptions;
use sparrow_merge::old::homogeneous_merge;

/// A shared, synchronized container for in-memory batches.
pub struct InMemorySource {
    /// The prepared schema.
    ///
    /// Note this is not the `projected_schema`, which is the schema
    /// after applying column projections.
    prepared_schema: SchemaRef,
    /// The in-memory batches.
    data: Arc<InMemoryBatches>,
}

impl InMemorySource {
    pub fn new(queryable: bool, schema: SchemaRef) -> error_stack::Result<Self, SourceError> {
        let data = Arc::new(InMemoryBatches::new(queryable, schema.clone()));
        let source = Self {
            prepared_schema: schema,
            data,
        };
        Ok(source)
    }

    /// Add a batch, publishing it to the subscribers.
    pub async fn add_batch(&self, batch: RecordBatch) -> error_stack::Result<(), SourceError> {
        self.data.add_batch(batch).await
    }
}

impl Source for InMemorySource {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn prepared_schema(&self) -> SchemaRef {
        self.prepared_schema.clone()
    }

    fn read(
        &self,
        projected_datatype: &DataType,
        execution_options: Arc<ExecutionOptions>,
    ) -> futures::stream::BoxStream<'static, error_stack::Result<Batch, SourceError>> {
        assert_eq!(
            &DataType::Struct(self.prepared_schema().fields().clone()),
            projected_datatype,
            "Projection not yet supported"
        );

        let input_stream = if execution_options.materialize {
            self.data
                .subscribe()
                .map_err(|e| e.change_context(SourceError::internal_msg("invalid input")))
                .and_then(move |batch| async move {
                    Batch::try_new_from_batch(batch)
                        .change_context(SourceError::internal_msg("invalid input"))
                })
                .boxed()
        } else if let Some(batch) = self.data.current() {
            futures::stream::once(async move {
                Batch::try_new_from_batch(batch)
                    .change_context(SourceError::internal_msg("invalid input"))
            })
            .boxed()
        } else {
            futures::stream::empty().boxed()
        };

        input_stream
    }
}

/// Struct for managing in-memory batches.
///
/// Note: several items left pub for use in old code path, can remove
/// when that path is removed.
#[derive(Debug)]
pub struct InMemoryBatches {
    /// Whether rows added will be available for interactive queries.
    /// If False, rows will be discarded after being sent to any active
    /// materializations.
    queryable: bool,
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
    fn new(schema: SchemaRef) -> Self {
        let batch = RecordBatch::new_empty(schema.clone());
        Self {
            schema,
            version: 0,
            batch,
        }
    }

    fn add_batch(&mut self, batch: &RecordBatch) -> error_stack::Result<(), SourceError> {
        if self.batch.num_rows() == 0 {
            self.batch = batch.clone();
        } else {
            // This assumes that cloning the old batch is cheap.
            // If it isn't, we could replace it with an empty batch (`std::mem::replace`),
            // put it in an option, or allow `homogeneous_merge` to take `&RecordBatch`.
            self.batch = homogeneous_merge(&self.schema, vec![self.batch.clone(), batch.clone()])
                .into_report()
                .change_context(SourceError::Add)?;
        }
        Ok(())
    }
}

impl InMemoryBatches {
    pub fn new(queryable: bool, schema: SchemaRef) -> Self {
        let (mut sender, receiver) = async_broadcast::broadcast(10);

        // Don't wait for a receiver. If no-one receives, `send` will fail.
        sender.set_await_active(false);

        let current = RwLock::new(Current::new(schema.clone()));
        Self {
            queryable,
            current,
            sender,
            _receiver: receiver.deactivate(),
        }
    }

    /// Add a batch, merging it into the in-memory version.
    ///
    /// Publishes the new batch to the subscribers.
    ///
    /// Note: This assumes the batch has been prepared, and will likely panic if not.
    pub async fn add_batch(&self, batch: RecordBatch) -> error_stack::Result<(), SourceError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let new_version = {
            let mut write = self.current.write().map_err(|_| SourceError::Add)?;
            if self.queryable {
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
    ) -> impl Stream<Item = error_stack::Result<RecordBatch, SourceError>> + 'static {
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
                            tracing::info!("Received version {recv_version}");
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
                        Err(SourceError::ReceiverLagged)?;
                    }
                }
            }
        }
        .boxed()
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

#[cfg(test)]
mod tests {
    use arrow_array::{
        types::{ArrowPrimitiveType, TimestampNanosecondType},
        ArrayRef, Int32Array, TimestampNanosecondArray, UInt64Array,
    };
    use arrow_schema::{Field, Schema};

    use super::*;

    #[static_init::dynamic]
    static SCHEMA: Schema = {
        Schema::new(vec![
            Field::new("_time", TimestampNanosecondType::DATA_TYPE, false),
            Field::new("_subsort", DataType::UInt64, false),
            Field::new("_key_hash", DataType::UInt64, false),
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::UInt64, true),
        ])
    };

    fn batch1() -> RecordBatch {
        let time: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![0, 1, 2]));
        let subsort: ArrayRef = Arc::new(UInt64Array::from(vec![0, 1, 2]));
        let key: ArrayRef = Arc::new(UInt64Array::from(vec![0, 0, 0]));
        let a: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2]));
        let b: ArrayRef = Arc::new(UInt64Array::from(vec![None, Some(1), Some(8)]));
        let schema = Arc::new(SCHEMA.clone());
        RecordBatch::try_new(schema.clone(), vec![time, subsort, key, a, b]).unwrap()
    }

    fn batch2() -> RecordBatch {
        let time: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![3, 4, 5]));
        let subsort: ArrayRef = Arc::new(UInt64Array::from(vec![0, 1, 2]));
        let key: ArrayRef = Arc::new(UInt64Array::from(vec![0, 0, 0]));
        let a: ArrayRef = Arc::new(Int32Array::from(vec![3, 4, 5]));
        let b: ArrayRef = Arc::new(UInt64Array::from(vec![Some(10), None, None]));
        let schema = Arc::new(SCHEMA.clone());
        RecordBatch::try_new(schema.clone(), vec![time, subsort, key, a, b]).unwrap()
    }

    #[tokio::test]
    async fn test_subscribe_to_batches() {
        let batch1 = batch1();
        let batch2 = batch2();
        let schema = Arc::new(SCHEMA.clone());

        let in_mem = InMemoryBatches::new(true, schema);

        let mut s1 = in_mem.subscribe();
        let mut s2 = in_mem.subscribe();

        // Add the first batch
        in_mem.add_batch(batch1.clone()).await.unwrap();

        let b1_s1 = s1.next().await.unwrap().unwrap();
        assert_eq!(batch1, b1_s1);
        let b1_s2 = s2.next().await.unwrap().unwrap();
        assert_eq!(batch1, b1_s2);

        // Add the second batch
        in_mem.add_batch(batch2.clone()).await.unwrap();
        let b2_s1 = s1.next().await.unwrap().unwrap();
        assert_eq!(batch2, b2_s1);
        let b2_s2 = s2.next().await.unwrap().unwrap();
        assert_eq!(batch2, b2_s2);
    }

    #[tokio::test]
    async fn test_subscribe_to_multiple_batches() {
        // Sends multiple batches before reading
        let batch1 = batch1();
        let batch2 = batch2();
        let schema = Arc::new(SCHEMA.clone());

        let in_mem = InMemoryBatches::new(true, schema.clone());

        let mut s1 = in_mem.subscribe();
        let mut s2 = in_mem.subscribe();

        // Add both batches before reading
        in_mem.add_batch(batch1.clone()).await.unwrap();
        in_mem.add_batch(batch2.clone()).await.unwrap();

        let b1_s1 = s1.next().await.unwrap().unwrap();
        assert_eq!(batch1, b1_s1);
        let b1_s2 = s2.next().await.unwrap().unwrap();
        assert_eq!(batch1, b1_s2);

        let b2_s1 = s1.next().await.unwrap().unwrap();
        assert_eq!(batch2, b2_s1);
        let b2_s2 = s2.next().await.unwrap().unwrap();
        assert_eq!(batch2, b2_s2);
    }

    #[tokio::test]
    async fn test_late_subscription_receives_merged_batch() {
        // Verify later subscription gets the full merged batch
        let batch1 = batch1();
        let batch2 = batch2();
        let schema = Arc::new(SCHEMA.clone());

        let in_mem = InMemoryBatches::new(true, schema.clone());

        // Subscribe first stream
        let mut s1 = in_mem.subscribe();

        // Send both batches. In-memory should have merged them.
        in_mem.add_batch(batch1.clone()).await.unwrap();
        in_mem.add_batch(batch2.clone()).await.unwrap();

        // Subscribe second stream
        let mut s2 = in_mem.subscribe();

        let b1_s1 = s1.next().await.unwrap().unwrap();
        assert_eq!(batch1, b1_s1);

        let b2_s1 = s1.next().await.unwrap().unwrap();
        assert_eq!(batch2, b2_s1);

        let merged_batch =
            arrow_select::concat::concat_batches(&batch1.schema(), &[batch1, batch2]).unwrap();

        let b1_s2 = s2.next().await.unwrap().unwrap();
        assert_eq!(merged_batch, b1_s2);
    }

    #[tokio::test]
    async fn test_reads_current_batch() {
        let batch1 = batch1();
        let schema = Arc::new(SCHEMA.clone());

        let in_mem = InMemoryBatches::new(true, schema);
        in_mem.add_batch(batch1.clone()).await.unwrap();

        let received = in_mem.current().unwrap();
        assert_eq!(batch1, received);

        let batch2 = batch2();
        in_mem.add_batch(batch2.clone()).await.unwrap();

        let received = in_mem.current().unwrap();
        let expected =
            arrow_select::concat::concat_batches(&batch1.schema(), &[batch1, batch2]).unwrap();
        assert_eq!(received, expected);
    }

    #[tokio::test]
    async fn test_non_queryable_reads_empty_current_batch() {
        let batch1 = batch1();
        let schema = Arc::new(SCHEMA.clone());

        let in_mem = InMemoryBatches::new(false, schema);
        in_mem.add_batch(batch1.clone()).await.unwrap();

        assert!(in_mem.current().is_none());
    }

    #[tokio::test]
    async fn test_non_queryable_subscription() {
        let batch1 = batch1();
        let batch2 = batch2();
        let schema = Arc::new(SCHEMA.clone());

        let in_mem = InMemoryBatches::new(false, schema.clone());

        // Subscribe first stream
        let mut s1 = in_mem.subscribe();

        // Send both batches
        in_mem.add_batch(batch1.clone()).await.unwrap();

        // Subscribe second stream
        let mut s2 = in_mem.subscribe();

        // Send the second batch
        in_mem.add_batch(batch2.clone()).await.unwrap();

        let b1_s1 = s1.next().await.unwrap().unwrap();
        assert_eq!(batch1, b1_s1);

        let b2_s1 = s1.next().await.unwrap().unwrap();
        assert_eq!(batch2, b2_s1);

        // Second subscription should only see second batch
        let b1_s2 = s2.next().await.unwrap().unwrap();
        assert_eq!(batch2, b1_s2);
    }
}
