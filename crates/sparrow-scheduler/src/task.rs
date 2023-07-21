use std::sync::Arc;

use error_stack::ResultExt;

use crate::schedule_count::ScheduleCount;
use crate::{Error, Partition, Pipeline, Scheduler};

/// The unit of work executed by the scheduler.
///
/// A task processes a single unit of input (typically a batch), applies a
/// single [Pipeline] and produces a single unit of output (typically a batch).
#[derive(Debug)]
pub struct Task {
    /// Index of this pipeline. Used for debugging.
    index: usize,
    /// Name of the pipeline implementation.
    name: &'static str,
    /// The pipeline to execute.
    ///
    /// This is a weak reference to avoid cycles.
    pipeline: std::sync::Weak<dyn Pipeline>,
    /// The partition of the pipeline to execute.
    partition: Partition,
    /// An atomic counter tracking how many times the task has been submitted.
    ///
    /// This is reset after the task is executed.
    schedule_count: ScheduleCount,
}

impl Task {
    /// Create a new task executing the given pipeline and partition.
    pub(crate) fn new(
        index: usize,
        name: &'static str,
        pipeline: std::sync::Weak<dyn Pipeline>,
        partition: Partition,
    ) -> Self {
        Self {
            index,
            name,
            pipeline,
            partition,
            schedule_count: ScheduleCount::default(),
        }
    }

    /// Record a request for scheduling.
    ///
    /// Returns true if this task wasn't previously scheduled.
    ///
    /// Generally should only be called by the worker.
    pub(crate) fn schedule(&self) -> bool {
        self.schedule_count.schedule()
    }

    fn pipeline(&self) -> error_stack::Result<Arc<dyn Pipeline>, Error> {
        Ok(self.pipeline.upgrade().ok_or(Error::PipelineDropped {
            index: self.index,
            name: self.name,
            partition: self.partition,
        })?)
    }

    fn error(&self, method: &'static str) -> Error {
        Error::Pipeline {
            method,
            index: self.index,
            name: self.name,
            partition: self.partition,
        }
    }

    #[inline]
    pub(crate) fn do_work(
        &self,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<bool, Error> {
        let guard = self.schedule_count.guard();
        self.pipeline()?
            .do_work(self.partition, scheduler)
            .change_context_lazy(|| self.error("do_work"))?;
        Ok(guard.finish())
    }
}

pub type TaskRef = Arc<Task>;
