use sparrow_arrow::Batch;

use crate::{Error, Partition, Partitioned, Queue, TaskRef};

/// Trait for the output of a pipeline.
pub trait Sink: Sync + Send + std::fmt::Debug {
    /// Return the name of the sink.
    ///
    /// Defaults to the name of the type that Sink is implemented for.
    fn name(&self) -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Send a batch to the given sink for the specific partition.
    ///
    /// Errors:
    /// - If the given partition is already closed.
    /// - If there is a problem sending the batch to the partition.
    fn send(
        &self,
        partition: Partition,
        batch: Batch,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), Error>;

    /// Close the specific partition.
    ///
    /// Errors:
    /// - If the partition is already closed.
    fn close(
        &self,
        partition: Partition,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), Error>;
}

#[derive(Debug)]
pub struct PipelineSink {
    tasks: Partitioned<TaskRef>,
    input: usize,
}

impl Sink for PipelineSink {
    fn send(
        &self,
        partition: Partition,
        batch: Batch,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), Error> {
        self.tasks[partition].push(self.input, batch, queue)
    }

    fn close(
        &self,
        partition: Partition,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), Error> {
        self.tasks[partition].close(self.input, queue)
    }
}
