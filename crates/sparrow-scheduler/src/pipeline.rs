use sparrow_arrow::Batch;

use crate::queue::Queue;
use crate::{Partition, TaskRef, Worker};

/// A push-based interface used by the scheduler to drive query execution
///
/// A pipeline processes data from one or more input partitions, producing output
/// to one or more output partitions. As a [`Pipeline`] may drawn on input from
/// more than one upstream [`Pipeline`], input partitions are identified by both
/// a child index, and a partition index, whereas output partitions are only
/// identified by a partition index.
///
/// This is not intended as an eventual replacement for the physical plan
/// representation, but rather a generic interface that parts of the physical
/// plan are converted to for execution.
///
/// # Eager vs Lazy Execution
///
/// Whether computation is eagerly done on push, or lazily done on pull, is
/// intentionally left as an implementation detail of the [`Pipeline`]
///
/// This allows flexibility to support the following different patterns, and potentially more:
///
/// An eager, push-based pipeline, that processes a batch synchronously in [`Pipeline::push`]
/// and immediately wakes the corresponding output partition.
///
/// A parallel, push-based pipeline, that enqueues the processing of a batch to
/// the thread pool in [`Pipeline::push`], and wakes the corresponding output
/// partition when the job completes. Order and non-order preserving variants
/// are possible
///
/// A merge pipeline which combines data from one or more input partitions into one or
/// more output partitions. [`Pipeline::push`] adds data to an input buffer, and wakes
/// any output partitions that may now be able to make progress. This may be none if
/// the operator is waiting on data from a different input partition.
///
/// An aggregation pipeline which combines data from one or more input partitions into
/// a single output partition. [`Pipeline::push`] would eagerly update the computed
/// aggregates, and the final [`Pipeline::close`] trigger flushing these to the output.
/// It would also be possible to flush once the partial aggregates reach a certain size.
///
/// A partition-aware aggregation pipeline, which functions similarly to the above, but
/// computes aggregations per input partition, before combining these prior to flush.
///
/// An async input pipeline, which has no inputs, and wakes the output partition
/// whenever new data is available.
///
/// A JIT compiled sequence of synchronous operators, that perform multiple operations
/// from the physical plan as a single [`Pipeline`]. Parallelized implementations
/// are also possible.
///
pub trait Pipeline: Send + Sync + std::fmt::Debug {
    type Context: error_stack::Context;

    fn name() -> &'static str {
        std::any::type_name::<Self>()
    }

    /// Push a [`Batch`] to the given input partition.
    ///
    /// This is called from outside the pipeline -- either a Tokio thread
    /// reading from a source or a producing pipeline. As a result, this should
    /// generally add the batch to a mutex-protected queue and ensure a task is
    /// scheduled for executing this partition of this pipeline..
    ///
    /// Schedules any tasks that need to be executed on the worker.
    fn push(
        &self,
        input_partition: Partition,
        input: usize,
        batch: Batch,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), Self::Context>;

    /// Mark an input partition as exhausted.
    ///
    /// Schedules any tasks that need to be executed on the worker.
    fn close(
        &self,
        input_partition: Partition,
        input: usize,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), Self::Context>;

    /// Run the pipeline on the data that has been pushed in.
    ///
    /// Generally this should return after processing / producing a single
    /// batch. If additional work can be done, returning `Ok(true)` indicates
    /// that this pipeline should be immediately rescheduled.
    fn do_work(
        &self,
        partition: Partition,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), Self::Context>;
}

/// Wrap a specific pipeline into a handle that produces a crate-specific error.
pub(crate) struct PipelineHandle<T: Pipeline> {
    pipeline: T,
}

impl<T: Pipeline> std::fmt::Debug for PipelineHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PipelineHandle")
            .field("pipeline", &self.pipeline)
            .finish()
    }
}
