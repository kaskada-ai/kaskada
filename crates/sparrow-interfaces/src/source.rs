use arrow_schema::{DataType, SchemaRef};
use futures::stream::BoxStream;
use sparrow_batch::Batch;

use crate::SourceError;

/// Trait implemented by sources.
pub trait Source: Send + Sync {
    fn prepared_schema(&self) -> SchemaRef;
    /// Defines how a source provides data to the execution layer.
    ///
    /// Creates a stream which can be read in a loop to obtain batches
    /// until some stopping condition is met (stream is exhausted,
    /// a certain number of batches are read, a stop signal is received,
    /// etc.
    ///
    /// Parameters:
    /// * `projected_datatype`: The datatype of the data to produce. Note that
    ///    this may differ from the prepared type.
    /// * `read_config`: Configuration for the read.
    fn read(
        &self,
        projected_datatype: &DataType,
        read_config: ReadConfig,
    ) -> BoxStream<'_, error_stack::Result<Batch, SourceError>>;
}

/// Defines the configuration for a read from a source.
#[derive(Debug)]
pub struct ReadConfig {
    /// If true, the read will act as an unbounded source and continue reading
    /// as new data is added. It is on the consumer to close the channel.
    ///
    /// If false, the read will act as a bounded source, and stop once the set
    /// of data available at the time of the read has been processed.
    pub keep_open: bool,
    /// Optional timestamp in nanos at which to start reading.
    ///
    /// Defaults to the earliest available timestamp.
    pub start_time: Option<i64>,
    /// Optional timestamp in nanos at which to end reading.
    ///
    /// Defaults to reading until the source is closed.
    pub end_time: Option<i64>,
}
