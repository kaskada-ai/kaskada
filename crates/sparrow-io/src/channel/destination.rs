use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use error_stack::{IntoReport, ResultExt};
use sparrow_batch::Batch;
use sparrow_interfaces::{
    destination::{Destination, DestinationError, WriteError, Writer},
    types::Partition,
};

#[derive(Debug)]
pub struct ChannelDestination {
    /// The expected output schema.
    schema: SchemaRef,
    // TODO: The output current expects [RecordBatch] instead of [Batch]
    // but we should standardize on [Batch].
    txs: Vec<tokio::sync::mpsc::Sender<RecordBatch>>,
}

impl ChannelDestination {
    /// Construct a new channel destination with the given senders.
    ///
    /// The number of `txs` should equal the number of partitions the output
    /// produces from.
    pub fn new(schema: SchemaRef, txs: Vec<tokio::sync::mpsc::Sender<RecordBatch>>) -> Self {
        Self { schema, txs }
    }
}

impl Destination for ChannelDestination {
    fn new_writer(
        &self,
        partition: Partition,
    ) -> error_stack::Result<Box<dyn Writer>, DestinationError> {
        let partition: usize = partition.into();
        let tx = self
            .txs
            .get(partition)
            .ok_or_else(|| {
                DestinationError::internal_msg("expected channel for partition {partition}")
            })?
            .clone(); // Senders can be cloned cheaply
        Ok(Box::new(ChannelWriter {
            schema: self.schema.clone(),
            tx,
        }))
    }
}

#[derive(Debug)]
struct ChannelWriter {
    schema: SchemaRef,
    tx: tokio::sync::mpsc::Sender<RecordBatch>,
}

impl Writer for ChannelWriter {
    fn write_batch(&mut self, batch: Batch) -> error_stack::Result<(), WriteError> {
        // HACK: This converts `Batch` to `RecordBatch` because the current execution logic
        // expects `RecordBatch` outputs. This should be changed to standardize on `Batch`
        // which makes it easier to carry a primitive value out.
        if let Some(batch) = batch.into_record_batch(self.schema.clone()) {
            self.tx
                .blocking_send(batch)
                .into_report()
                .change_context(WriteError::internal())?;
        }
        Ok(())
    }

    fn close(&self) -> error_stack::Result<(), WriteError> {
        self.tx.downgrade();
        Ok(())
    }
}
