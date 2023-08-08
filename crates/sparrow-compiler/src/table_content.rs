use tonic::codegen::BoxStream;

use crate::Error;

pub struct TableContent {
    schema: SchemaRef,
    version: usize,
    merged: RecordBatch,
    updates: tokio::sync::broadcast::Sender<(usize, RecordBatch)>,
}

impl TableContent {
    pub fn new(schema: SchemaRef) -> Self {
        let (updates, _) = tokio::sync::broadcast::channel(1);
        let merged = RecordBatch::new_empty(schema.clone());
        mSelf {
            schema,
            version: 0,
            merged,
            updates,
        }
    }

    pub fn add_batch(&mut self, batch: RecordBatch) -> error_stack::Result<(), Error> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let merged = if self.merged.num_rows() == 0 {
            batch
        } else {
            homogeneous_merge(&self.schema, vec![self.merged.clone(), batch])
                .into_report()
                .change_context(Error::Internal("add_batch"))?
        };

        self.version += 1;
        self.updates
            .send((self.version, merged.clone()))
            .into_report()
            .change_context(Error::Internal("add_batch"))?;
        self.merged = merged;
        Ok(())
    }

    pub fn stream(&self) -> BoxStream<RecordBatch> {
        async_stream::stream! {
            let mut version = self.version;
            let mut batches = self.updates.subscribe();

            tracing::info!("Starting subscriber with version {version}");
            yield self.merged;
            while let Ok((recv_version, batch)) = batches.recv().await {
                tracing::
                if version < recv_version {
                    tracing::info!("Received version {recv_version} (prev: {version}");
                    yield batch;
                    version = recv_version;
                } else {
                    tracing::warn!("Ignoring version {recv_version} (already up to {version})")
                }
            }
        }
    }
}
