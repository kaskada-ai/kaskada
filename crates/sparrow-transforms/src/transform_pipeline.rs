use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};

use error_stack::ResultExt;
use itertools::Itertools;
use parking_lot::Mutex;
use sparrow_batch::Batch;
use sparrow_physical::{StepId, StepKind};
use sparrow_scheduler::{
    InputHandles, Partition, Partitioned, Pipeline, PipelineError, Scheduler, TaskRef,
};

use crate::transform::Transform;

/// Runs a linear sequence of transforms as a pipeline.
pub struct TransformPipeline {
    /// The state for each partition.
    partitions: Partitioned<TransformPartition>,
    transforms: Vec<Box<dyn Transform>>,
    /// Input handles for the consuming (receiving) computations.
    consumers: InputHandles,
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

struct TransformPartition {
    /// Whether this partition is closed.
    is_closed: AtomicBool,
    /// Inputs for this partition.
    ///
    /// TODO: This could use a thread-safe queue to avoid locking.
    inputs: Mutex<VecDeque<Batch>>,
    /// Task for this partition.
    task: TaskRef,
}

impl TransformPartition {
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
    CreatingTransform { kind: &'static str },
}

impl error_stack::Context for Error {}

impl TransformPipeline {
    /// Create a new transform pipeline.
    ///
    /// Args:
    ///   producer_id: The `StepId` of the step that produces input to this
    ///     pipeline. It should be the only input step to the first step in
    ///     `steps`.
    ///   steps: Iterator over the steps (in order) comprising the pipeline.
    ///     They should all be transforms.
    ///   consumers: The `InputHandles` to output the result of the transform to.
    pub fn try_new<'a>(
        producer_id: StepId,
        steps: impl Iterator<Item = &'a sparrow_physical::Step> + ExactSizeIterator,
        consumers: InputHandles,
    ) -> error_stack::Result<Self, Error> {
        let mut input_step_id = producer_id;
        let mut transforms = Vec::with_capacity(steps.len());
        for step in steps {
            error_stack::ensure!(
                step.inputs.len() == 1,
                Error::TooManyInputs {
                    kind: (&step.kind).into(),
                    len: step.inputs.len()
                }
            );
            error_stack::ensure!(
                step.inputs[0] == input_step_id,
                Error::UnexpectedInput {
                    expected: input_step_id,
                    actual: step.inputs[0]
                }
            );

            let transform: Box<dyn Transform> = match &step.kind {
                StepKind::Project => Box::new(
                    crate::project::Project::try_new(&step.exprs, &step.result_type)
                        .change_context_lazy(|| Error::CreatingTransform {
                            kind: (&step.kind).into(),
                        })?,
                ),
                StepKind::Filter => Box::new(
                    crate::select::Select::try_new(&step.exprs, &step.result_type)
                        .change_context_lazy(|| Error::CreatingTransform {
                            kind: (&step.kind).into(),
                        })?,
                ),
                unsupported => {
                    error_stack::bail!(Error::UnsupportedStepKind {
                        kind: unsupported.into()
                    })
                }
            };
            transforms.push(transform);
            input_step_id = step.id;
        }
        Ok(Self {
            partitions: Partitioned::default(),
            transforms,
            consumers,
        })
    }
}

impl Pipeline for TransformPipeline {
    fn initialize(&mut self, tasks: Partitioned<TaskRef>) {
        self.partitions = tasks
            .into_iter()
            .map(|task| TransformPartition {
                is_closed: AtomicBool::new(false),
                inputs: Mutex::new(VecDeque::new()),
                task,
            })
            .collect();
    }

    fn add_input(
        &self,
        input_partition: Partition,
        input: usize,
        batch: Batch,
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
        error_stack::ensure!(
            !partition.is_closed(),
            PipelineError::InputClosed {
                input,
                input_partition
            }
        );
        partition.add_input(batch);
        scheduler.schedule(partition.task.clone());
        Ok(())
    }

    fn close_input(
        &self,
        input_partition: Partition,
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

        // Don't close the sink (consumers) here. We may be currently executing a `do_work`
        // loop, in which case we need to allow it to output to the sink before
        // we close it.
        //
        // We call `close` on the partition to indicate that once the work loop sees
        // that the partition is empty of inputs, it can close its consumers and
        // complete itself.
        partition.close();
        scheduler.schedule(partition.task.clone());

        Ok(())
    }

    fn do_work(
        &self,
        input_partition: Partition,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        let partition = &self.partitions[input_partition];
        let _enter = tracing::trace_span!("TransformPipeline::do_work", %partition.task).entered();

        let Some(batch) = partition.pop_input() else {
            error_stack::ensure!(
                partition.is_closed(),
                PipelineError::illegal_state("scheduled without work")
            );

            tracing::info!(
                "Input is closed and empty. Closing consumers and finishing partition {}.",
                input_partition
            );
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
                // Transforms don't add rows.
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
