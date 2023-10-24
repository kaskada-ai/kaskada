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
    /// The senders for each input and partition.
    ///
    /// Batches are first placed into the channels, which allows us to use
    /// [Gatherer] to choose which input needs to be merged next.
    ///
    /// This vec should contains one element for each input.
    txs: Vec<Partitioned<tokio::sync::mpsc::UnboundedSender<Batch>>>,
    /// The matching receivers for each sender.
    rxs: Vec<Partitioned<tokio::sync::mpsc::UnboundedReceiver<Batch>>>,
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

    // Keeps track of the entities seen by this partition
    // TODO: key_hash_index: KeyHashIndex,
    merger: Mutex<HeterogeneousMerge>,
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
            txs: Vec::new(),
            rxs: Vec::new(),
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
        self.partitions = tasks
            .into_iter()
            .map(|task| {
                MergePartition {
                    is_closed: AtomicBool::new(false),
                    task,
                    // TODO: Error handling
                    // TODO: Interpolation
                    // Current impl uses unlatched spread (`Null` interpolation), meaning discrete behavior.
                    merger: Mutex::new(HeterogeneousMerge::new(
                        &self.result_type,
                        &self.left.datatype,
                        &self.right.datatype,
                    )),
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

        self.txs[input][input_partition]
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
            partition.merger.lock().close(input);
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
        let mut partition = &self.partitions[input_partition];
        let _enter = tracing::trace_span!("MergePipeline::do_work", %partition.task).entered();

        let merger = partition.merger.lock();
        if let Some(active_input) = merger.blocking_input() {
            let mut receiver = self.rxs[active_input][input_partition];
            match receiver.try_recv() {
                Ok(batch) => {
                    // Add the batch to the active input
                    let ready_to_produce = merger.add_batch(active_input, batch);
                    if ready_to_produce {
                        let merged_batch =
                            merger.merge().change_context(PipelineError::Execution)?;

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
            // TODO: Are we actually sure all inputs are empty, just because the merger
            // blocking input is none?
            // NO! We need to call `next_batch` on the gatherer one last time to flush.

            todo!("Call next_batch on the gatherer");
            tracing::info!(
                "Input is closed and empty. Closing consumers and finishing partition {}.",
                input_partition
            );
            self.consumers.close_input(input_partition, scheduler)?;
            partition.task.complete();
            return Ok(());
        }

        // let Some((batch, input)) = receiver.try_recv() else {
        //     error_stack::ensure!(
        //         partition.is_closed(),
        //         PipelineError::illegal_state("scheduled without work")
        //     );

        //     // We _should_ only have to call `next_batch` once, as both inputs should have
        //     // been marked as closed, and the gatherer will concatenate all leftover batches.
        //     assert!(partition.all_closed());
        //     // let gathered_batches =gatherer.next_batch();
        //     // debug_assert!(gatherer.next_batch().is_none(), "expected only one batch");

        //     let gathered_batches = todo!();
        //     if let Some(gathered_batches) = gathered_batches {
        //         todo!()
        //     assert_eq!(gathered_batches.batches.len(), 2);
        //     let concatted_batches: Vec<Batch> = gathered_batches.concat();
        //     let left: &Batch = &concatted_batches[0];
        //     let right: &Batch = &concatted_batches[1];

        //     // TODO: Assumes batch data is non-empty.
        //     let left_merge_input = BinaryMergeInput::new(
        //         left.time().expect("time"),
        //         left.subsort().expect("subsort"),
        //         left.key_hash().expect("key_hash"),
        //     );
        //     let right_merge_input = BinaryMergeInput::new(
        //         right.time().expect("time"),
        //         right.subsort().expect("subsort"),
        //         right.key_hash().expect("key_hash"),
        //     );
        //     let merged_result = crate::binary_merge(left_merge_input, right_merge_input)
        //         .into_report()
        //         .change_context(PipelineError::Execution)?;

        //     let left_spread_bits = arrow::compute::is_not_null(&merged_result.take_a)
        //         .into_report()
        //         .change_context(PipelineError::Execution)?;
        //     let right_spread_bits = arrow::compute::is_not_null(&merged_result.take_b)
        //         .into_report()
        //         .change_context(PipelineError::Execution)?;

        //     let merged_time = Arc::new(merged_result.time);
        //     let merged_subsort = Arc::new(merged_result.subsort);
        //     let merged_key_hash = Arc::new(merged_result.key_hash);

        //     let spread_l = &mut partition.spread_l;
        //     let spread_r = &mut partition.spread_r;
        //     todo!()
        // }

        //     tracing::info!(
        //         "Input is closed and empty. Closing consumers and finishing partition {}.",
        //         input_partition
        //     );
        //     self.consumers.close_input(input_partition, scheduler)?;
        //     partition.task.complete();
        //     return Ok(());
        // };

        // If the batch is non empty, process it.
        // TODO: Propagate empty batches to further the watermark.

        Ok(())
    }
}
