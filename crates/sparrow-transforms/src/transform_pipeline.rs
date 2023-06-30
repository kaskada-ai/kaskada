use std::collections::VecDeque;
use std::sync::Arc;

use error_stack::ResultExt;
use itertools::Itertools;
use parking_lot::Mutex;
use sparrow_arrow::Batch;
use sparrow_scheduler::{
    Partition, Partitioned, Pipeline, PipelineSink, Queue, Scheduler, Sink, TaskRef,
};

use crate::transforms::Transform;
use crate::Error;

/// Runs a linear sequence of transforms as a pipeline.
pub struct TransformPipeline {
    /// The inputs this transform processes.
    ///
    /// For each partition:
    ///   Hold a shared, mutex-protected input queue.
    inputs: Partitioned<Arc<Mutex<InputBuffer>>>,
    transforms: Vec<Box<dyn Transform>>,
    /// Compute tasks to wake up as needed.
    tasks: Partitioned<TaskRef>,
    /// Sink for the down-stream computation.
    sink: PipelineSink,
}

impl std::fmt::Debug for TransformPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransformPipeline")
            .field(
                "transforms",
                &self.transforms.iter().map(|t| t.name()).format(","),
            )
            .finish()
    }
}

#[derive(Debug, Default)]
struct InputBuffer {
    buffer: VecDeque<Batch>,
    is_closed: bool,
}

impl TransformPipeline {
    pub(crate) fn schedule(
        scheduler: &mut Scheduler,
        partitions: usize,
        transforms: Vec<Box<dyn Transform>>,
        sink: PipelineSink,
    ) {
        scheduler.add_pipeline(partitions, |tasks| {
            let input_partitions = tasks.len();
            let mut inputs = Partitioned::with_capacity(input_partitions);
            for _ in 0..input_partitions {
                inputs.push(Arc::new(Mutex::new(InputBuffer::default())));
            }
            Self {
                inputs,
                transforms,
                tasks,
                sink,
            }
        })
    }

    fn pop_batch(&self, partition: Partition) -> Option<Batch> {
        let mut input_partition = self.inputs[partition].lock();
        input_partition.buffer.pop_front()
    }

    fn pending(&self, partition: Partition) -> usize {
        self.inputs[partition].lock().buffer.len()
    }

    fn is_closed(&self, partition: Partition) -> bool {
        self.inputs[partition].lock().is_closed
    }
}

impl Pipeline for TransformPipeline {
    type Context = Error;

    fn push(
        &self,
        partition: Partition,
        input: usize,
        batch: Batch,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), Error> {
        error_stack::ensure!(input == 0, Error::InvalidInput { input });

        let mut input_partition = self.inputs[partition].lock();
        error_stack::ensure!(
            !input_partition.is_closed,
            Error::PartitionClosed(partition)
        );
        input_partition.buffer.push_back(batch);
        queue.push(self.tasks[partition].clone());

        Ok(())
    }

    fn close(
        &self,
        partition: Partition,
        input: usize,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), Error> {
        error_stack::ensure!(input == 1, Error::InvalidInput { input });

        let mut input_partition = self.inputs[partition].lock();
        error_stack::ensure!(
            !input_partition.is_closed,
            Error::PartitionClosed(partition)
        );
        input_partition.is_closed = true;
        if input_partition.buffer.is_empty() {
            self.sink
                .close(partition, queue)
                .change_context(Error::Sink)?;
        } else {
            // We shouldn't need to wake the input partition.
            // It should have been woken when we pushed the batch in the buffer,
            // or when we pushed a batch after execution started.
        }

        Ok(())
    }

    fn do_work(
        &self,
        partition: Partition,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), crate::Error> {
        let Some(input) = self.pop_batch(partition) else {
            error_stack::bail!(Error::IllegalState("no input batch in do_work"))
        };

        tracing::trace!(
            "Performing work for partition {partition} on {} rows",
            input.num_rows()
        );

        if !input.is_empty() {
            let mut batch = input;
            for transform in self.transforms.iter() {
                batch = transform.apply(batch)?;
                if batch.is_empty() {
                    break;
                }
            }
            if !batch.is_empty() {
                self.sink
                    .send(partition, batch, queue)
                    .change_context(Error::Sink)?;
            }
        }

        let pending = self.pending(partition);

        if pending > 0 {
            tracing::trace!("{pending} batches remaining in {partition}");
            queue.push(self.tasks[partition].clone());
        } else if self.is_closed(partition) {
            self.sink
                .close(partition, queue)
                .change_context(Error::Sink)?;
        }

        Ok(())
    }
}
