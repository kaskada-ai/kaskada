use std::borrow::Cow;

use sparrow_batch::Batch;

use crate::{Partition, Partitioned, Scheduler, TaskRef};

#[derive(derive_more::Display, Debug)]
pub enum PipelineError {
    #[display(fmt = "invalid input index {input} for pipeline with {input_len} inputs")]
    InvalidInput { input: usize, input_len: usize },
    #[display(fmt = "input {input} for partition {input_partition} is already closed")]
    InputClosed {
        input: usize,
        input_partition: Partition,
    },
    #[display(fmt = "illegal state: {_0}")]
    IllegalState(Cow<'static, str>),
    #[display(fmt = "error executing pipeline")]
    Execution,
}

impl PipelineError {
    pub fn illegal_state(state: impl Into<Cow<'static, str>>) -> Self {
        Self::IllegalState(state.into())
    }
}

impl error_stack::Context for PipelineError {}

/// A push-based interface used by the scheduler to drive query execution
///
/// A pipeline processes data from one or more input partitions, producing output
/// to one or more output partitions. As a [`Pipeline`] may draw on input from
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
    /// Initialize the pipeline by providing the tasks it will use for scheduling.
    ///
    /// The number of tasks provided indicate the number of partitions this pipeline
    /// should execute with.
    ///
    /// This is unfortunately separate to allow cyclic initialization. Specifically,
    /// `Arc::new_cyclic` expects a function that doesn't take errors, while creating
    /// many pipelines *does* produce errors. To address this, we first create the
    /// part of the pipeline that doesn't need to reference it's own tasks, and then
    /// we initialize it as part of `Arc::new_cyclic`.
    fn initialize(&mut self, tasks: Partitioned<TaskRef>);

    /// Add a [`Batch`] to the given input partition and input index.
    ///
    /// This is called from outside the pipeline -- either a Tokio thread
    /// reading from a source or a producing pipeline. As a result, this should
    /// generally add the batch to a mutex-protected buffer and ensure a task is
    /// scheduled for executing this partition of this pipeline..
    ///
    /// Schedules any tasks that need to be executed on the `scheduler`.
    fn add_input(
        &self,
        input_partition: Partition,
        input: usize,
        batch: Batch,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError>;

    /// Mark an input partition and input index as complete.
    ///
    /// Schedules any tasks that need to be executed on the `scheduler`.
    fn close_input(
        &self,
        input_partition: Partition,
        input: usize,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError>;

    /// Run the pipeline on the data that has been pushed in.
    ///
    /// May schedule additional work to be done on the `scheduler`.
    ///
    /// Generally this should return after processing / producing a single
    /// batch. If additional work must be done, this partition may be
    /// re-scheduled with the `scheduler`.
    fn do_work(
        &self,
        partition: Partition,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError>;
}
