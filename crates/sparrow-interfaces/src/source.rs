use std::sync::Arc;

use arrow_schema::{DataType, SchemaRef};
use futures::stream::BoxStream;
use sparrow_batch::Batch;

use crate::{ExecutionOptions, SourceError};

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
    /// * `execution_options`: Options for the entire execution.
    fn read(
        &self,
        projected_datatype: &DataType,
        execution_options: Arc<ExecutionOptions>,
    ) -> BoxStream<'static, error_stack::Result<Batch, SourceError>>;

    /// Allow downcasting the source.
    fn as_any(&self) -> &dyn std::any::Any;
}

pub trait SourceExt {
    fn downcast_source_opt<T: Source + 'static>(&self) -> Option<&T>;
    fn downcast_source<T: Source + 'static>(&self) -> &T {
        self.downcast_source_opt().expect("unexpected type")
    }
}

impl SourceExt for Arc<dyn Source> {
    fn downcast_source_opt<T: Source + 'static>(&self) -> Option<&T> {
        self.as_any().downcast_ref()
    }
}

impl SourceExt for &Arc<dyn Source> {
    fn downcast_source_opt<T: Source + 'static>(&self) -> Option<&T> {
        self.as_any().downcast_ref()
    }
}
