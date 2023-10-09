use std::sync::Arc;

use error_stack::ResultExt;

use crate::pending::PendingPartition;
use crate::schedule_count::ScheduleCount;
use crate::{Error, Pipeline, Scheduler};

/// The unit of work executed by the scheduler.
///
/// A task processes a single unit of input (typically a batch), applies a
/// single [Pipeline] and produces a single unit of output (typically a batch).
#[derive(Debug)]
pub struct Task {
    /// Entry recording the status (pending or not) of this task.
    pending: PendingPartition,
    /// Name of the pipeline implementation.
    name: &'static str,
    /// The pipeline to execute.
    ///
    /// This is a weak reference to avoid cycles.
    pipeline: std::sync::Weak<dyn Pipeline>,
    /// An atomic counter tracking how many times the task has been submitted.
    ///
    /// This is reset after the task is executed.
    schedule_count: ScheduleCount,
}

impl Task {
    /// Create a new task executing the given pipeline and partition.
    pub(crate) fn new(
        pending: PendingPartition,
        name: &'static str,
        pipeline: std::sync::Weak<dyn Pipeline>,
    ) -> Self {
        Self {
            pending,
            name,
            pipeline,
            schedule_count: ScheduleCount::default(),
        }
    }

    /// Mark this task as scheduled.
    ///
    /// Returns `false` if this task was previously scheduled and has not
    /// yet been executed.
    ///
    /// Generally should only be called by the worker.
    ///
    /// If this is called while it is being executed (eg., during `do_work`) then
    /// the `guard` will return `true` to indicate the task should be re-executed.
    pub(crate) fn schedule(&self) -> bool {
        self.schedule_count.schedule()
    }

    fn pipeline(&self) -> error_stack::Result<Arc<dyn Pipeline>, Error> {
        Ok(self.pipeline.upgrade().ok_or(Error::PipelineDropped {
            index: self.pending.pipeline_index,
            name: self.name,
            partition: self.pending.partition,
        })?)
    }

    fn error(&self, method: &'static str) -> Error {
        Error::Pipeline {
            method,
            index: self.pending.pipeline_index,
            name: self.name,
            partition: self.pending.partition,
        }
    }

    #[inline]
    pub(crate) fn do_work(
        &self,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<bool, Error> {
        let guard = self.schedule_count.guard();
        self.pipeline()?
            .do_work(self.pending.partition, scheduler)
            .change_context_lazy(|| self.error("do_work"))?;
        Ok(guard.finish())
    }

    /// Mark this task as completed.
    ///
    /// After this, it should not be scheduled nor should work be done.
    pub fn complete(&self) {
        self.pending.complete()
    }

    pub fn is_complete(&self) -> bool {
        self.pending.is_complete()
    }
}

pub type TaskRef = Arc<Task>;
