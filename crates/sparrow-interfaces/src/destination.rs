use sparrow_batch::Batch;
use std::fmt::Debug;

use crate::types::Partition;

/// Trait implemented by destinations.
pub trait Destination: Send + Sync + Debug {
    /// Creates a new writer to this destination.
    fn new_writer(
        &self,
        partition: Partition,
    ) -> error_stack::Result<Box<dyn Writer>, DestinationError>;
}

pub trait Writer: Send + Sync + Debug {
    /// Write a batch to the given writer.
    ///
    /// NOTE: Some destinations (such as Parquet) may actually rotate files during / after
    /// calls to `write_batch`.
    fn write_batch(&mut self, batch: Batch) -> error_stack::Result<(), WriteError>;

    /// Close this writer.
    fn close(&self) -> error_stack::Result<(), WriteError>;
}

#[non_exhaustive]
#[derive(derive_more::Display, Debug)]
pub enum DestinationError {
    #[display(fmt = "internal error: {}", _0)]
    Internal(&'static str),
}

impl error_stack::Context for DestinationError {}

impl DestinationError {
    pub fn internal() -> Self {
        DestinationError::Internal("no additional context")
    }

    pub fn internal_msg(msg: &'static str) -> Self {
        DestinationError::Internal(msg)
    }
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
