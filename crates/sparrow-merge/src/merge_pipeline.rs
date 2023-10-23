use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow_schema::DataType;
use error_stack::ResultExt;
use itertools::Itertools;
use parking_lot::Mutex;
use sparrow_batch::Batch;
use sparrow_physical::{Step, StepId, StepKind};
use sparrow_scheduler::{
    InputHandles, Partition, Partitioned, Pipeline, PipelineError, Scheduler, TaskRef,
};

use crate::spread::Spread;

/// Runs a merge operation.
pub struct MergePipeline {
    /// The state for each partition.
    partitions: Partitioned<MergePartition>,
    /// Input handles for the consuming (receiving) computations.
    consumers: InputHandles,
    /// The id for the left input
    input_l: StepId,
    /// The id for the right input
    input_r: StepId,
    /// The left data type
    datatype_l: DataType,
    /// The right data type
    datatype_r: DataType,
}

impl std::fmt::Debug for MergePipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergePipeline").finish()
    }
}

struct MergePartition {
    /// Whether this partition is closed.
    is_closed: AtomicBool,
    /// Task for this partition.
    task: TaskRef,
    /// Left side inputs for this partition.
    ///
    /// TODO: This could use a thread-safe queue to avoid locking.
    input_l: Mutex<VecDeque<Batch>>,
    /// Right side inputs for this partition.
    input_r: Mutex<VecDeque<Batch>>,
    /// Implementation of `spread` to use for this input
    spread_l: Spread,
    /// Implementation of `spread` to use for this input
    spread_r: Spread,
}

impl MergePartition {
    /// Close the input. Returns true if the input buffer is empty.
    fn close(&self) -> bool {
        self.is_closed.store(true, Ordering::Release);
        self.input_l.lock().is_empty()
    }

    fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }

    fn add_input(&self, batch: Batch, input: usize) {
        if input == 0 {
            self.input_l.lock().push_back(batch);
        } else if input == 1 {
            self.input_r.lock().push_back(batch);
        } else {
            panic!("TODO")
        }
    }

    fn pop_input(&self) -> Option<Batch> {
        self.input_l.lock().pop_front()
    }
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "merge should accept exactly 2 input, but length for '{kind}' was {len}")]
    TooManyInputs { kind: &'static str, len: usize },
    #[display(fmt = "invalid merge: expected input {expected} but was {actual}")]
    UnexpectedInput { expected: StepId, actual: StepId },
    #[display(fmt = "failed to create merge for step '{kind}'")]
    CreatingMerge { kind: &'static str },
}

impl error_stack::Context for Error {}

impl MergePipeline {
    /// Create a new merge pipeline.
    ///
    /// Args:
    ///   consumers: The `InputHandles` to output the result of the transform to.
    pub fn try_new<'a>(
        input_l: StepId,
        input_r: StepId,
        datatype_l: &DataType,
        datatype_r: &DataType,
        consumers: InputHandles,
    ) -> error_stack::Result<Self, Error> {
        Ok(Self {
            partitions: Partitioned::default(),
            consumers,
            input_l,
            input_r,
            datatype_l: datatype_l.clone(),
            datatype_r: datatype_r.clone(),
        })
    }
}

impl Pipeline for MergePipeline {
    fn initialize(&mut self, tasks: Partitioned<TaskRef>) {
        // TODO: error?
        self.partitions = tasks
            .into_iter()
            .map(|task| MergePartition {
                is_closed: AtomicBool::new(false),
                task,
                input_l: Mutex::new(VecDeque::new()),
                input_r: Mutex::new(VecDeque::new()),
                spread_l: Spread::try_new(false, &self.datatype_l).expect("spread"),
                spread_r: Spread::try_new(false, &self.datatype_r).expect("spread"),
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
        partition.add_input(batch, input);
        // TODO: we _might_ could decide not to schedule work here if we haven't progressed
        // one or the other input enough such that merge can't progress, but that adds
        // cpu time on the producer.
        scheduler.schedule(partition.task.clone());
        Ok(())
    }

    fn close_input(
        &self,
        input_partition: Partition,
        input: usize,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        // TODO: FRAZ - we have two inputs for merge, so we need to wait
        // for both to finish before closing the partition.
        error_stack::ensure!(
            input == 0 || input == 1,
            PipelineError::InvalidInput {
                input,
                input_len: 2
            }
        );
        let partition = &self.partitions[input_partition];
        tracing::trace!("Closing input for merge {}", partition.task);
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
        input_partition: Partition,
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
            // TODO: FRAZ Do merge

            // If the result is non-empty, output it.
            if !batch.is_empty() {
                self.consumers
                    .add_input(input_partition, batch, scheduler)
                    .change_context(PipelineError::Execution)?;
            }
        }

        Ok(())
    }
}
