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

use crate::gather::Gatherer;
use crate::merge::BinaryMergeInput;
use crate::spread::Spread;

/// Runs a merge operation.
pub struct MergePipeline {
    /// The state for each partition.
    partitions: Partitioned<Mutex<MergePartition>>,
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
    /// Keeps track of closed inputs.
    is_closed_l: Mutex<bool>,
    is_closed_r: Mutex<bool>,
    /// The result type of this merge
    result_type: DataType,
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
    /// Inputs (for either side) for this partition.
    ///
    /// the `usize` should be either 0 (left), or 1 (right).
    inputs: Mutex<VecDeque<(Batch, usize)>>,
    /// Implementation of `spread` to use for this input
    #[allow(unused)]
    spread_l: Spread,
    /// Implementation of `spread` to use for this input
    #[allow(unused)]
    spread_r: Spread,

    /// Gathers batches from both sides and produced [GatheredBatches]
    /// up to a valid watermark.
    gatherer: Mutex<Gatherer>,
    // Keeps track of the entities seen by this partition
    // TODO: key_hash_index: KeyHashIndex,
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

    fn add_input(&self, batch: Batch, input: usize) {
        self.inputs.lock().push_back((batch, input));
    }

    fn pop_input(&self) -> Option<(Batch, usize)> {
        self.inputs.lock().pop_front()
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
        Ok(Self {
            partitions: Partitioned::default(),
            consumers,
            input_l,
            input_r,
            datatype_l: datatype_l.clone(),
            datatype_r: datatype_r.clone(),
            is_closed_l: Mutex::new(false),
            is_closed_r: Mutex::new(false),
            result_type: result_type.clone(),
        })
    }
}

impl Pipeline for MergePipeline {
    fn initialize(&mut self, tasks: Partitioned<TaskRef>) {
        self.partitions = tasks
            .into_iter()
            .map(|task| {
                Mutex::new(MergePartition {
                    is_closed: AtomicBool::new(false),
                    task,
                    inputs: Mutex::new(VecDeque::new()),
                    // TODO: Error handling
                    // TODO: Interpolation
                    // Current impl uses unlatched spread (`Null` interpolation), meaning discrete behavior.
                    spread_l: Spread::try_new(false, &self.datatype_l).expect("spread"),
                    spread_r: Spread::try_new(false, &self.datatype_r).expect("spread"),
                    gatherer: Mutex::new(Gatherer::new(2)),
                })
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
        let partition = &self.partitions[input_partition].lock();
        error_stack::ensure!(
            !partition.is_closed(),
            PipelineError::InputClosed {
                input,
                input_partition
            }
        );
        partition.add_input(batch, input);
        // TODO: we _could_(?) decide not to schedule work here if we haven't progressed
        // one or the other input enough such that merge can't progress, but that adds
        // cpu time on the producer task.
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
            let is_closed = if input == 0 {
                &self.is_closed_l
            } else {
                &self.is_closed_r
            };
            let mut is_closed_guard = is_closed.lock();
            *is_closed_guard = true;
        }

        let partition = &self.partitions[input_partition];
        partition.gatherer.lock().close(input);

        tracing::trace!("Closing input {} for merge {}", input, partition.task);
        error_stack::ensure!(
            !partition.is_closed(),
            PipelineError::InputClosed {
                input,
                input_partition
            }
        );

        if *self.is_closed_l.lock() && *self.is_closed_r.lock() {
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

        // TODO: Decide how to pop sides and see which can progress.
        // We don't really want to pop more than 1 batch though. Maybe we have to indicate which to pop?
        // Open question:
        // Merge doesn't have a 1 input -> 1 output pattern.
        // How do we know how much work to do per `do_work` loop?
        // We may have call do_work an infinite number of times on one side, but not be able
        // to produce anything.
        // Maybe I'm overthinking this -- if we have a unit of work, we've gotten a batch on
        // some side. it's possible we get one side overloaded, but supposedly we should be
        // sending empty batches with up_to_time if we are filtering the other side still.
        // And if we truly just have more batches on one side, then that's how it goes.
        // We'll concat them all and produce them once the other side closes (or progresses).
        //
        // 1. Task scheduled for left
        // 2. do_work() happens
        // 3. We pop input, there's nothing we can do because we haven't progressed right.
        // 4. Where do we put input?
        //      In the gatherer?
        //
        // TODO: Use Gather
        // 1. Add batch to gatherer
        // 2. If it's ready to produce, get next_batch: GatheredBatches.
        // 3. This is the vec<batch> that we need to merge together.
        // 4. Should have len 2.
        // 5. Merge them together (they may have different schemas).
        // 5a. They should have _time, _subsort, _key_hash, _data
        // 5b. _data should be a prim array or struct array.
        // 5c. I need to get the output schema from the two,
        // .   and make a new schema with a fields if structs
        // .   that's doable. Result should be new _data as structarray.
        //     (q: can column be the same? what does that mean?)
        // 6. Output the result, single batch.
        let Some((batch, input)) = partition.pop_input() else {
            error_stack::ensure!(
                partition.is_closed(),
                PipelineError::illegal_state("scheduled without work")
            );

            // Though both inputs have closed, we may still have ungathered batches.
            // Gather the rest here and merge them.
            let mut gatherer = partition.gatherer.lock();

            // We _should_ only have to call `next_batch` once, as both inputs should have
            // been marked as closed, and the gatherer will concatenate all leftover batches.
            assert!(gatherer.all_closed());
            let gathered_batches = gatherer.next_batch();
            debug_assert!(gatherer.next_batch().is_none(), "expected only one batch");

            if let Some(gathered_batches) = gathered_batches {
                assert_eq!(gathered_batches.batches.len(), 2);
                let concatted_batches: Vec<Batch> = gathered_batches.concat();
                let left: &Batch = &concatted_batches[0];
                let right: &Batch = &concatted_batches[1];

                // TODO: Assumes batch data is non-empty.
                let left_merge_input = BinaryMergeInput::new(
                    left.time().expect("time"),
                    left.subsort().expect("subsort"),
                    left.key_hash().expect("key_hash"),
                );
                let right_merge_input = BinaryMergeInput::new(
                    right.time().expect("time"),
                    right.subsort().expect("subsort"),
                    right.key_hash().expect("key_hash"),
                );
                let merged_result = crate::binary_merge(left_merge_input, right_merge_input)
                    .into_report()
                    .change_context(PipelineError::Execution)?;

                let left_spread_bits = arrow::compute::is_not_null(&merged_result.take_a)
                    .into_report()
                    .change_context(PipelineError::Execution)?;
                let right_spread_bits = arrow::compute::is_not_null(&merged_result.take_b)
                    .into_report()
                    .change_context(PipelineError::Execution)?;

                let merged_time = Arc::new(merged_result.time);
                let merged_subsort = Arc::new(merged_result.subsort);
                let merged_key_hash = Arc::new(merged_result.key_hash);

                let spread_l = &mut partition.spread_l;
                let spread_r = &mut partition.spread_r;
                todo!()
            }

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
            // TODO: fix merged batch
            let merged_batch: Batch = batch.clone();

            let mut gatherer = partition.gatherer.lock();
            // Adds the batch to the appropriate index
            let can_produce = gatherer.add_batch(input, batch);
            if can_produce {
                let gathered_batches = gatherer.next_batch();
                // TODO: Refactor this into a method because you have to do it above too.
            }

            // If the result is non-empty, output it.
            if !merged_batch.is_empty() {
                self.consumers
                    .add_input(input_partition, merged_batch, scheduler)
                    .change_context(PipelineError::Execution)?;
            }
        }

        Ok(())
    }
}
