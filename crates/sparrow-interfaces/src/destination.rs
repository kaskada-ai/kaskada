use sparrow_batch::Batch;
use std::fmt::Debug;

use crate::types::Partition;

/// Interface defining how output is written to a particular destination.
pub trait Destination: Send + Sync + Debug {
    /// Creates a new writer to this destination.
    ///
    /// Arguments:
    /// * partition: The partition this writer will write to.
    fn new_writer(&self, partition: Partition) -> error_stack::Result<Box<dyn Writer>, WriteError>;
}

/// Writer for a specific destination.
pub trait Writer: Send + Sync + Debug {
    /// Write a batch to the given writer.
    ///
    /// NOTE: Some destinations (such as Parquet) may actually rotate files
    /// during / after calls to `write_batch`.
    ///
    /// Arguments:
    /// * batch - The batch to write.
    fn write_batch(&mut self, batch: Batch) -> error_stack::Result<(), WriteError>;

    /// Close this writer.
    fn close(&self) -> error_stack::Result<(), WriteError>;
}

#[non_exhaustive]
#[derive(derive_more::Display, Debug)]
pub enum WriteError {
    #[display(fmt = "internal error on write: {}", _0)]
    Internal(&'static str),
}

impl error_stack::Context for WriteError {}

impl WriteError {
    pub fn internal() -> Self {
        WriteError::Internal("no additional context")
    }

    pub fn internal_msg(msg: &'static str) -> Self {
        WriteError::Internal(msg)
    }
}
