use std::collections::VecDeque;
use std::sync::Arc;

use error_stack::ResultExt;
use itertools::Itertools;
use parking_lot::Mutex;
use sparrow_arrow::Batch;
use sparrow_physical::{StepId, StepKind};
use sparrow_scheduler::{
    Partition, Partitioned, Pipeline, PipelineError, PipelineInput, Queue, TaskRef,
};

use crate::transform::Transform;

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
    sink: PipelineInput,
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
    pub fn try_new<'a>(
        input_step: &sparrow_physical::Step,
        steps: impl Iterator<Item = &'a sparrow_physical::Step> + ExactSizeIterator,
        sink: PipelineInput,
    ) -> error_stack::Result<Self, Error> {
        let mut input_step = input_step;
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
                step.inputs[0] == input_step.id,
                Error::UnexpectedInput {
                    expected: input_step.id,
                    actual: step.inputs[0]
                }
            );

            let transform: Box<dyn Transform> = match &step.kind {
                StepKind::Project { exprs } => Box::new(
                    crate::project::Project::try_new(
                        &input_step.schema,
                        exprs,
                        step.schema.clone(),
                    )
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
            input_step = step;
        }
        Ok(Self {
            inputs: Partitioned::default(),
            transforms,
            tasks: Partitioned::default(),
            sink,
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
    fn initialize(&mut self, tasks: Partitioned<TaskRef>) {
        let input_partitions = tasks.len();
        self.inputs.resize_with(input_partitions, || {
            Arc::new(Mutex::new(InputBuffer::default()))
        });
        self.tasks = tasks;
    }

    fn add_input(
        &self,
        partition: Partition,
        input: usize,
        batch: Batch,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), PipelineError> {
        error_stack::ensure!(
            input == 0,
            PipelineError::InvalidInput {
                input,
                input_len: 1
            }
        );

        let mut input_partition = self.inputs[partition].lock();
        error_stack::ensure!(
            !input_partition.is_closed,
            PipelineError::InputClosed { input, partition }
        );
        input_partition.buffer.push_back(batch);
        queue.schedule(self.tasks[partition].clone());

        Ok(())
    }

    fn close_input(
        &self,
        partition: Partition,
        input: usize,
        queue: &mut dyn Queue<TaskRef>,
    ) -> error_stack::Result<(), PipelineError> {
        error_stack::ensure!(
            input == 0,
            PipelineError::InvalidInput {
                input,
                input_len: 1
            }
        );

        let mut input_partition = self.inputs[partition].lock();
        error_stack::ensure!(
            !input_partition.is_closed,
            PipelineError::InputClosed { input, partition }
        );
        input_partition.is_closed = true;
        if input_partition.buffer.is_empty() {
            self.sink
                .close_input(partition, queue)
                .change_context(PipelineError::Execution)?;
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
    ) -> error_stack::Result<(), PipelineError> {
        let Some(input) = self.pop_batch(partition) else {
            error_stack::bail!(PipelineError::illegal_state("no input batch in do_work"))
        };

        tracing::trace!(
            "Performing work for partition {partition} on {} rows",
            input.num_rows()
        );

        if !input.is_empty() {
            let mut batch = input;
            for transform in self.transforms.iter() {
                batch = transform
                    .apply(batch)
                    .change_context(PipelineError::Execution)?;
                if batch.is_empty() {
                    break;
                }
            }
            if !batch.is_empty() {
                self.sink
                    .add_input(partition, batch, queue)
                    .change_context(PipelineError::Execution)?;
            }
        }

        let pending = self.pending(partition);

        if pending > 0 {
            tracing::trace!("{pending} batches remaining in {partition}");
            queue.schedule(self.tasks[partition].clone());
        } else if self.is_closed(partition) {
            self.sink
                .close_input(partition, queue)
                .change_context(PipelineError::Execution)?;
        }

        Ok(())
    }
}
