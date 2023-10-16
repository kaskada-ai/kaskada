use std::sync::atomic::AtomicBool;
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
    /// Name of the pipeline implementation.
    name: &'static str,
    partition: Partition,
    pipeline_index: usize,
    /// The pipeline to execute.
    ///
    /// This is a weak reference to avoid cycles.
    pipeline: std::sync::Weak<dyn Pipeline>,
    /// An atomic counter tracking how many times the task has been submitted.
    ///
    /// This is reset after the task is executed.
    schedule_count: ScheduleCount,
    /// Whether this task has been completed.
    complete: AtomicBool,
}

impl std::fmt::Display for Task {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({}) partition {}",
            self.name, self.pipeline_index, self.partition
        )
    }
}

impl Task {
    /// Create a new task executing the given pipeline and partition.
    pub(crate) fn new(
        name: &'static str,
        partition: Partition,
        pipeline_index: usize,
        pipeline: std::sync::Weak<dyn Pipeline>,
    ) -> Self {
        Self {
            name,
            partition,
            pipeline_index,
            pipeline,
            schedule_count: ScheduleCount::default(),
            complete: AtomicBool::new(false),
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
        debug_assert!(!self.is_complete(), "Scheduling completed task");
        self.schedule_count.schedule()
    }

    fn pipeline(&self) -> error_stack::Result<Arc<dyn Pipeline>, Error> {
        Ok(self.pipeline.upgrade().ok_or(Error::PipelineDropped {
            index: self.pipeline_index,
            name: self.name,
            partition: self.partition,
        })?)
    }

    fn error(&self, method: &'static str) -> Error {
        Error::Pipeline {
            method,
            index: self.pipeline_index,
            name: self.name,
            partition: self.partition,
        }
    }

    /// Execute one iteration of the given task.
    ///
    /// Returns `true` if the task was scheduled during execution, indicating
    /// it should be added to the queue again.
    pub(crate) fn do_work(
        &self,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<bool, Error> {
        let guard = self.schedule_count.guard();
        let span = tracing::debug_span!(
            "Running task",
            name = self.name,
            pipeline = self.pipeline_index,
            partition = ?self.partition
        );
        let _enter = span.enter();

        tracing::info!("Start");
        self.pipeline()?
            .do_work(self.partition, scheduler)
            .change_context_lazy(|| self.error("do_work"))?;
        tracing::info!("Done");
        Ok(guard.finish())
    }

    /// Mark this task as completed.
    ///
    /// After this, it should not be scheduled nor should work be done.
    pub fn complete(&self) -> bool {
        self.complete
            .fetch_or(true, std::sync::atomic::Ordering::AcqRel)
    }

    pub fn is_complete(&self) -> bool {
        self.complete.load(std::sync::atomic::Ordering::Acquire)
    }
}

pub type TaskRef = Arc<Task>;
