use std::sync::Arc;

use error_stack::ResultExt;
use sparrow_arrow::Batch;

use crate::queue::Queue;
use crate::schedule_count::ScheduleCount;
use crate::{Error, Partition, Pipeline, Worker};

/// The unit of work executed by the scheduler.
///
/// A task processes a single unit of input (typically a batch), applies a
/// single [Pipeline] and produces a single unit of output (typically a batch).
#[derive(Debug)]
pub struct Task<T: Pipeline> {
    /// Index of this pipeline. Used for debugging.
    index: usize,
    /// The pipeline to execute.
    ///
    /// This is a weak reference to avoid cycles.
    pipeline: std::sync::Weak<T>,
    /// The partition of the pipeline to execute.
    partition: Partition,
    /// An atomic counter tracking how many times the task has been submitted.
    ///
    /// This is reset after the task is executed.
    schedule_count: ScheduleCount,
}

impl<T: Pipeline> Task<T> {
    /// Create a new task executing the given pipeline and partition.
    pub(crate) fn new(index: usize, pipeline: std::sync::Weak<T>, partition: Partition) -> Self {
        Self {
            index,
            pipeline,
            partition,
            schedule_count: ScheduleCount::default(),
        }
    }

    fn pipeline(&self) -> error_stack::Result<Arc<T>, Error> {
        Ok(self
            .pipeline
            .upgrade()
            .ok_or_else(|| Error::PipelineDropped {
                index: self.index,
                name: T::name(),
                partition: self.partition,
            })?)
    }

    fn error(&self, method: &'static str) -> Error {
        Error::Pipeline {
            method,
            index: self.index,
            name: T::name(),
            partition: self.partition,
        }
    }
}

/// Trait with methods on generic `Task`.
///
/// This is an object safe wrapper allowing execution of the pipeline within the task.
pub trait TaskObject: std::fmt::Debug + Sync + Send {
    /// Record a request for scheduling.
    ///
    /// Returns true if this task wasn't previously scheduled.
    ///
    /// Generally should only be called by the worker.
    fn schedule(&self) -> bool;

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
        input: usize,
        batch: Batch,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), Error>;

    /// Mark an input partition as exhausted.
    ///
    /// Schedules any tasks that need to be executed on the worker.
    fn close(&self, input: usize, queue: &mut dyn Queue<TaskRef>)
        -> error_stack::Result<(), Error>;

    /// Run the pipeline on the data that has been pushed in.
    ///
    /// Generally this should return after processing / producing a single
    /// batch. If additional work can be done, returning `Ok(true)` indicates
    /// that this pipeline should be immediately rescheduled.
    ///
    /// Returns true if the task was re-scheduled while during execution,
    /// indicating it should be re-submitted to the global queue for execution.
    fn do_work(&self, queue: &mut dyn Queue<TaskRef>) -> error_stack::Result<bool, Error>;
}

pub type TaskRef = Arc<dyn TaskObject>;

impl<T: Pipeline> TaskObject for Task<T> {
    fn schedule(&self) -> bool {
        self.schedule_count.schedule()
    }

    fn push(
        &self,
        input: usize,
        batch: Batch,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), Error> {
        self.pipeline()?
            .push(self.partition, input, batch, queue)
            .change_context_lazy(|| self.error("push"))
    }

    fn close(
        &self,
        input: usize,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), Error> {
        self.pipeline()?
            .close(self.partition, input, queue)
            .change_context_lazy(|| self.error("close"))
    }

    fn do_work(&self, queue: &mut dyn Queue<TaskRef>) -> error_stack::Result<bool, Error> {
        let guard = self.schedule_count.guard();
        self.pipeline()?
            .do_work(self.partition, queue)
            .change_context_lazy(|| self.error("do_work"))?;
        Ok(guard.finish())
    }
}
