use arrow_schema::DataType;
use error_stack::{IntoReport, ResultExt};
use parking_lot::Mutex;
use sparrow_batch::Batch;
use sparrow_physical::StepId;
use sparrow_scheduler::{
    InputHandles, Partition, Partitioned, Pipeline, PipelineError, Scheduler, TaskRef,
};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc::error::TryRecvError;

use crate::merge::HeterogeneousMerge;

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
    #[allow(unused)]
    id: StepId,
    /// The data type for this input
    datatype: DataType,
    /// Whether this side is closed
    is_closed: AtomicBool,
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
    /// Handles the merge logic and keeps track of the state.
    ///
    /// All objects that require locking should be within this state.
    state: Mutex<MergePartitionState>,
    // Keeps track of the entities seen by this partition
    // TODO: key_hash_index: KeyHashIndex,
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
struct MergePartitionState {
    /// The receivers for each input.
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
    /// - `input_l`: The left input.
    /// - `input_r`: The right input.
    /// - `datatype_l`: The data type for the left input.
    /// - `datatype_r`: The data type for the right input.
    /// - `result_type`: The result type of this merge.
    /// - `consumers`: The consumers for this merge.
    pub fn try_new(
        input_l: StepId,
        input_r: StepId,
        datatype_l: &DataType,
        datatype_r: &DataType,
        result_type: &DataType,
        consumers: InputHandles,
    ) -> error_stack::Result<Self, Error> {
        let left = Input {
            id: input_l,
            datatype: datatype_l.clone(),
            is_closed: AtomicBool::new(false),
        };
        let right = Input {
            id: input_r,
            datatype: datatype_r.clone(),
            is_closed: AtomicBool::new(false),
        };
        Ok(Self {
            partitions: Partitioned::default(),
            consumers,
            left,
            right,
            result_type: result_type.clone(),
        })
    }

    fn complete_partition(
        &self,
        input_partition: Partition,
        merge_partition: &MergePartition,
        scheduler: &mut dyn Scheduler,
    ) -> error_stack::Result<(), PipelineError> {
        tracing::info!(
            "Input is closed and empty. Closing consumers and finishing partition {}.",
            input_partition
        );
        self.consumers.close_input(input_partition, scheduler)?;
        merge_partition.task.complete();
        Ok(())
    }
}

impl Pipeline for MergePipeline {
    fn initialize(&mut self, tasks: Partitioned<TaskRef>) {
        self.partitions = tasks
            .into_iter()
            .map(|task| {
                let (tx_l, rx_l) = tokio::sync::mpsc::unbounded_channel();
                let (tx_r, rx_r) = tokio::sync::mpsc::unbounded_channel();
                MergePartition {
                    task,
                    is_closed: AtomicBool::new(false),
                    txs: vec![tx_l, tx_r],
                    // TODO: Interpolation
                    // Current impl uses unlatched spread (`Null` interpolation).
                    state: Mutex::new(MergePartitionState {
                        rxs: vec![rx_l, rx_r],
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

        if input == 0 {
            self.left.is_closed.store(true, Ordering::Release);
        } else {
            self.right.is_closed.store(true, Ordering::Release);
        }

        // Close the specific input -- required for the merge to know that the remaining
        // side can progress.
        //
        // While this contends with `do_work` for the partition lock, it's okay
        // since this should only be called once per input on completion.
        let partition = &self.partitions[input_partition];
        {
            partition.state.lock().merger.close(input);
        }

        tracing::trace!("Closing input {} for merge {}", input, partition.task);
        error_stack::ensure!(
            !partition.is_closed(),
            PipelineError::InputClosed {
                input,
                input_partition
            }
        );

        if self.left.is_closed.load(Ordering::Acquire)
            && self.right.is_closed.load(Ordering::Acquire)
        {
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

        let mut handler = partition.state.lock();
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
                    error_stack::ensure!(
                        partition.is_closed(),
                        PipelineError::illegal_state("scheduled without work")
                    );

                    // The channel is empty and the partition is closed, but the input may
                    // not have disconnected from the channel yet. This is okay -- if the partition
                    // is verified closed, we can continue to close our consumers here.
                    return self.complete_partition(input_partition, partition, scheduler);
                }
                Err(TryRecvError::Disconnected) => {
                    return self.complete_partition(input_partition, partition, scheduler)
                }
            }
        } else {
            // Though nothing is blocking the gatherer (in this case, all inputs must be closed),
            // the merger may have batches remaining to flush.
            assert!(handler.merger.all_closed());
            tracing::info!("Inputs are closed. Flushing merger.");

            // Check whether we need to flush the leftovers
            if handler.merger.can_produce() {
                let last_batch = handler
                    .merger
                    .merge()
                    .change_context(PipelineError::Execution)?;
                if !last_batch.is_empty() {
                    self.consumers
                        .add_input(input_partition, last_batch, scheduler)
                        .change_context(PipelineError::Execution)?;
                }
                assert!(!handler.merger.can_produce(), "expected only one batch");
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
