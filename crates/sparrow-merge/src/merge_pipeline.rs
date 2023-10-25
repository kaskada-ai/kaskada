use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use arrow_schema::DataType;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use itertools::Itertools;
use parking_lot::Mutex;
use sparrow_batch::Batch;
use sparrow_physical::StepId;
use sparrow_scheduler::{
    InputHandles, Partition, Partitioned, Pipeline, PipelineError, Scheduler, TaskRef,
};
use tokio::sync::mpsc::error::TryRecvError;

use crate::gather::Gatherer;
use crate::merge::{BinaryMergeInput, HeterogeneousMerge};
use crate::spread::Spread;

/// Runs a merge operation.
pub struct MergePipeline {
    /// The state for each partition.
    partitions: Partitioned<MergePartition>,
    /// Input handles for the consuming (receiving) computations.
    consumers: InputHandles,
    /// The left input.
    left: Input,
    /// The right input.
    right: Input,
    /// The result type of this merge.
    result_type: DataType,
}

impl std::fmt::Debug for MergePipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MergePipeline").finish()
    }
}

struct Input {
    /// The input id for this input
    input: StepId,
    /// The data type for this input
    datatype: DataType,
    /// Whether this side is closed
    is_closed: Mutex<bool>,
}

struct MergePartition {
    /// Whether this partition is closed.
    is_closed: AtomicBool,
    /// Task for this partition.
    task: TaskRef,
    /// The senders for each input.
    ///
    /// Batches are first placed into the channels, which allows us to use
    /// [Gatherer] to choose which input needs to be merged next.
    ///
    /// This vec should contains one element for each input.
    txs: Vec<tokio::sync::mpsc::UnboundedSender<Batch>>,
    // Keeps track of the entities seen by this partition
    // TODO: key_hash_index: KeyHashIndex,
    /// Handles the merge logic.
    ///
    /// All objects that require locking should be within this handler.
    handler: Mutex<MergePartitionHandler>,
}

impl MergePartition {
    /// Close the input.
    fn close(&self) {
        self.is_closed.store(true, Ordering::Release);
    }

    fn is_closed(&self) -> bool {
        self.is_closed.load(Ordering::Acquire)
    }
}

/// Handles the merge logic.
///
/// Separated from the [MergePartition] to avoid multiple locks.
struct MergePartitionHandler {
    /// The batch receivers for each input.
    ///
    /// This vec should contains one element for each input.
    rxs: Vec<tokio::sync::mpsc::UnboundedReceiver<Batch>>,
    /// Tracks the state and handles merge logic.
    merger: HeterogeneousMerge,
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
        result_type: &DataType,
        consumers: InputHandles,
    ) -> error_stack::Result<Self, Error> {
        let left = Input {
            input: input_l,
            datatype: datatype_l.clone(),
            is_closed: Mutex::new(false),
        };
        let right = Input {
            input: input_r,
            datatype: datatype_r.clone(),
            is_closed: Mutex::new(false),
        };
        Ok(Self {
            partitions: Partitioned::default(),
            consumers,
            left,
            right,
            result_type: result_type.clone(),
        })
    }
}

impl Pipeline for MergePipeline {
    fn initialize(&mut self, tasks: Partitioned<TaskRef>) {
        // TODO: FRAZ - need to have the channels here.
        self.partitions = tasks
            .into_iter()
            .map(|task| {
                MergePartition {
                    task,
                    is_closed: AtomicBool::new(false),
                    txs: Vec::new(),
                    // TODO: Error handling
                    // TODO: Interpolation
                    // Current impl uses unlatched spread (`Null` interpolation), meaning discrete behavior.
                    handler: Mutex::new(MergePartitionHandler {
                        rxs: Vec::new(),
                        merger: HeterogeneousMerge::new(
                            &self.result_type,
                            &self.left.datatype,
                            &self.right.datatype,
                        ),
                    }),
                }
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

        partition.txs[input]
            .send(batch)
            .into_report()
            .change_context(PipelineError::Execution)?;
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
            input == 0 || input == 1,
            PipelineError::InvalidInput {
                input,
                input_len: 2
            }
        );

        {
            // In a separate block to ensure the lock is released
            let mut is_closed = if input == 0 {
                self.left.is_closed.lock()
            } else {
                self.right.is_closed.lock()
            };
            *is_closed = true;
        }

        // Close the specific input -- required for the merge to know that the remaining
        // side can progress.
        //
        // While this contends with `do_work` for the partition lock, it's okay
        // since this should only be called once per input on completion.
        let partition = &self.partitions[input_partition];
        {
            partition.handler.lock().merger.close(input);
        }

        tracing::trace!("Closing input {} for merge {}", input, partition.task);
        error_stack::ensure!(
            !partition.is_closed(),
            PipelineError::InputClosed {
                input,
                input_partition
            }
        );

        if *self.left.is_closed.lock() && *self.right.is_closed.lock() {
            // Don't close the sink (consumers) here. We may be currently executing a `do_work`
            // loop, in which case we need to allow it to output to the sink before
            // we close it.
            //
            // We call `close` on the partition to indicate that once the work loop sees
            // that the partition is empty of inputs, it can close its consumers and
            // complete itself.
            partition.close();
            scheduler.schedule(partition.task.clone());
        }

        Ok(())
    }

    fn do_work(
        &self,
        input_partition: Partition,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        let partition = &self.partitions[input_partition];
        let _enter = tracing::trace_span!("MergePipeline::do_work", %partition.task).entered();

        let mut handler = partition.handler.lock();
        if let Some(active_input) = handler.merger.blocking_input() {
            let receiver = &mut handler.rxs[active_input];
            match receiver.try_recv() {
                Ok(batch) => {
                    // Add the batch to the active input
                    let ready_to_produce = handler.merger.add_batch(active_input, batch);
                    if ready_to_produce {
                        let merged_batch = handler
                            .merger
                            .merge()
                            .change_context(PipelineError::Execution)?;

                        // If the result is non-empty, output it.
                        self.consumers
                            .add_input(input_partition, merged_batch, scheduler)
                            .change_context(PipelineError::Execution)?;
                    }
                }
                Err(TryRecvError::Empty) => {
                    // TODO: This is uhh an invalid state? We called do_work, but have
                    // no work to do
                    todo!("?")
                }
                Err(TryRecvError::Disconnected) => {
                    tracing::info!(
                        "Input is closed and empty. Closing consumers and finishing partition {}.",
                        input_partition
                    );
                    self.consumers.close_input(input_partition, scheduler)?;
                    partition.task.complete();
                    return Ok(());
                }
            }
        } else {
            // Though all inputs are closed, the merger may have batches remaining to flush.
            tracing::info!("Inputs are closed. Flushing merger.");
            assert!(handler.merger.all_closed());

            let last_batch = handler
                .merger
                .merge()
                .change_context(PipelineError::Execution)?;
            if !last_batch.is_empty() {
                self.consumers
                    .add_input(input_partition, last_batch, scheduler)
                    .change_context(PipelineError::Execution)?;
            }

            tracing::info!(
                "Closing consumers and finishing partition {}.",
                input_partition
            );
            self.consumers.close_input(input_partition, scheduler)?;
            partition.task.complete();
            return Ok(());
        }

        Ok(())
    }
}
