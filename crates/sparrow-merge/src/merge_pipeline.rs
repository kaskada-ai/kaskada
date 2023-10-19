use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};

use error_stack::ResultExt;
use itertools::Itertools;
use parking_lot::Mutex;
use sparrow_batch::Batch;
use sparrow_physical::{StepId, StepKind};
use sparrow_scheduler::{
    InputHandles, Partition, PartitionIndex, Partitioned, Pipeline, PipelineError, Scheduler,
    TaskRef,
};

/// Runs a merge as a pipeline.
pub struct MergePipeline {
    /// The inputs to the merge.
    producer_ids: Vec<StepId>,
    /// The state for each partition.
    partitions: Partitioned<MergePartition>,
    /// Input handles for the consuming (receiving) computations.
    consumers: InputHandles,
}

impl std::fmt::Debug for MergePipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergePipeline").finish()
    }
}

impl MergePipeline {
    /// Create a new merge pipeline.
    ///
    /// Args:
    ///   producer_ids: The `StepId`s of the steps that produce input to this
    ///     pipeline.
    ///   steps: Iterator over the steps (in order) comprising the pipeline.
    ///     They should all be transforms.
    ///   consumers: The `InputHandles` to output the result of the transform to.
    pub fn try_new<'a>(
        producer_ids: Vec<StepId>,
        step: sparrow_physical::Step,
        consumers: InputHandles,
    ) -> error_stack::Result<Self, Error> {
        assert_eq!(
            producer_ids.len(),
            2,
            "expected 2 inputs for merge, saw {}",
            producer_ids.len()
        );
        tracing::debug!(
            "Creating merge pipeline with inputs: {}, {}",
            producer_ids[0],
            producer_ids[1]
        );
        Ok(Self {
            producer_ids,
            partitions: Partitioned::default(),
            consumers,
        })
    }
}

impl Pipeline for MergePipeline {
    fn initialize(&mut self, tasks: Partitioned<TaskRef>) {
        self.partitions = tasks
            .into_iter()
            .map(|task| MergePartition {
                is_closed: AtomicBool::new(false),
                inputs: Mutex::new(VecDeque::new()),
                task,
            })
            .collect();
    }

    fn add_input(
        &self,
        input_partition: PartitionIndex,
        input: usize,
        batch: Batch,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        error_stack::ensure!(
            input == 0 || input == 1,
            PipelineError::InvalidInput {
                input,
                input_len: 2
            }
        );
        let partition = &self.partitions[input_partition];
        error_stack::ensure!(
            !partition.is_closed(),
            PipelineError::InputClosed {
                input,
                input_partition
            }
        );
        // TODO: may need to add input usize here, so
        // when the merge worker takes the batch off the queue it knows
        // from which side it was
        // Does that work? Go look at where it takes it off to see if it would
        partition.add_input(batch);
        scheduler.schedule(partition.task.clone());
        Ok(())
    }

    fn close_input(
        &self,
        input_partition: PartitionIndex,
        input: usize,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        error_stack::ensure!(
            input == 0,
            PipelineError::InvalidInput {
                input,
                input_len: 1
            }
        );
        let partition = &self.partitions[input_partition];
        tracing::trace!("Closing input for transform {}", partition.task);
        error_stack::ensure!(
            !partition.is_closed(),
            PipelineError::InputClosed {
                input,
                input_partition
            }
        );

        // Don't close the sink here. We may be currently executing a `do_work`
        // loop, in which case we need to allow it to output to the sink before
        // we close it.
        partition.close();
        scheduler.schedule(partition.task.clone());

        Ok(())
    }

    fn do_work(
        &self,
        input_partition: PartitionIndex,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        let partition = &self.partitions[input_partition];
        let _enter = tracing::trace_span!("MergePipeline::do_work", %partition.task).entered();

        let Some(batch) = partition.pop_input() else {
            error_stack::ensure!(
                partition.is_closed(),
                PipelineError::illegal_state("scheduled without work")
            );

            tracing::info!("Input is closed and empty. Closing consumers and finishing pipeline.");
            self.consumers.close_input(input_partition, scheduler)?;
            partition.task.complete();
            return Ok(());
        };

        // If the batch is non empty, process it.
        // TODO: Propagate empty batches to further the watermark.
        if !batch.is_empty() {
            let mut batch = batch;
            for transform in self.transforms.iter() {
                batch = transform
                    .apply(batch)
                    .change_context(PipelineError::Execution)?;

                // Exit the sequence of transforms early if the batch is empty.
                // Merges don't add rows.
                if batch.is_empty() {
                    break;
                }
            }

            // If the result is non-empty, output it.
            if !batch.is_empty() {
                self.consumers
                    .add_input(input_partition, batch, scheduler)
                    .change_context(PipelineError::Execution)?;
            }
        }

        // Note: We don't re-schedule the transform if there is input or it's
        // closed. This should be handled by the fact that we scheduled the
        // transform when we added the batch (or closed it), which should
        // trigger the "scheduled during execution" -> "re-schedule" logic (see
        // ScheduleCount).

        Ok(())
    }
}

struct MergePartition {
    /// Whether this partition is closed.
    is_closed: AtomicBool,
    /// Inputs for this partition.
    ///
    /// TODO: This could use a thread-safe queue to avoid locking.
    inputs: Mutex<VecDeque<Batch>>,
    /// Task for this partition.
    task: TaskRef,
}

impl MergePartition {
    /// Close the input. Returns true if the input buffer is empty.
    fn close(&self) -> bool {
        self.is_closed.store(true, Ordering::Release);
        self.inputs.lock().is_empty()
    }

    fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }

    fn add_input(&self, batch: Batch) {
        self.inputs.lock().push_back(batch);
    }

    fn pop_input(&self) -> Option<Batch> {
        self.inputs.lock().pop_front()
    }
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "transforms should accept exactly 1 input, but length for '{kind}' was {len}")]
    TooManyInputs { kind: &'static str, len: usize },
    #[display(fmt = "invalid transform: expected input {expected} but was {actual}")]
    UnexpectedInput { expected: StepId, actual: StepId },
    #[display(fmt = "step '{kind}' is not supported as a transform")]
    UnsupportedStepKind { kind: &'static str },
    #[display(fmt = "failed to create transform for step '{kind}'")]
    CreatingMerge { kind: &'static str },
}

impl error_stack::Context for Error {}
