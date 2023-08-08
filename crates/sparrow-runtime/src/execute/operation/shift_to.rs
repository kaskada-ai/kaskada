use std::cmp;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{Array, ArrayRef, TimestampNanosecondArray, UInt32Array, UInt64Array};
use arrow::compute::SortColumn;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::NaiveDateTime;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::StreamExt;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::operation_plan;
use sparrow_api::kaskada::v1alpha::operation_plan::shift_to_operation::Time;
use sparrow_arrow::downcast::{downcast_boolean_array, downcast_primitive_array};
use sparrow_core::KeyTriples;
use sparrow_instructions::{ComputeStore, GroupingIndices, StoreKey};
use tokio_stream::wrappers::ReceiverStream;
use tracing::info;

use super::BoxedOperation;
use crate::execute::error::{invalid_operation, Error};
use crate::execute::operation::expression_executor::InputColumn;
use crate::execute::operation::single_consumer_helper::SingleConsumerHelper;
use crate::execute::operation::spread_zip::spread_zip;
use crate::execute::operation::{InputBatch, Operation};
use crate::Batch;

/// Implementation of Shift To for a literal.
#[derive(Debug)]
struct ShiftToLiteralOperation {
    /// The timestamp nanos to shift to.
    timestamp: i64,
    /// The next subsort to use when emitting records.
    next_subsort: u64,
    /// An array containing many repetitions of the timestamp.
    ///
    /// This allows creating the time column by reference / slicing.
    time_array: ArrayRef,
    incoming_stream: ReceiverStream<Batch>,
    helper: SingleConsumerHelper,
}

/// Implementation of Shift To for a time column.
///
/// If the computed time is `null` or in the past (relative the row being
/// shifted) it will be dropped.
///
/// PERFORMANCE: This is likely to have performance problems when we buffer up
/// large amounts of pending rows. Specifically, if we have `N` rows buffered,
/// and wish to add `k` it will create a new column of size `N + k` and copy all
/// rows into that. It will need to do the copy for each batch.
///
/// Better options include slicing the time range into "buckets" so that instead
/// copying the entire pending batch we only copy the rows in the buckets that
/// need to be grown. We could combine this with some form of "lazy" batches
/// where we keep the builders, and then build the batch and sort them *before
/// output*.
///
/// Another approach could involve putting the rows (which would require a
/// row-level encoding) in RocksDB keyed by time, so we could later iterate over
/// a specific time range. We may be able to use the Data Fusion row-based
/// format for this: https://github.com/apache/arrow-datafusion/blob/master/datafusion/row/src/lib.rs.
#[derive(Debug)]
struct ShiftToColumnOperation {
    shift_time_column: usize,
    /// The pending data for the shift.
    pending: Option<InputBatch>,
    incoming_stream: ReceiverStream<Batch>,
    helper: SingleConsumerHelper,
}

/// Create the stream of input batches for a select operation.
pub(super) fn create(
    operation: operation_plan::ShiftToOperation,
    incoming_channels: Vec<tokio::sync::mpsc::Receiver<Batch>>,
    input_columns: &[InputColumn],
) -> error_stack::Result<BoxedOperation, super::Error> {
    let input_channel = incoming_channels
        .into_iter()
        .exactly_one()
        .into_report()
        .change_context(Error::internal_msg("expected one channel"))?;

    let incoming_stream = ReceiverStream::new(input_channel);
    let helper = SingleConsumerHelper::try_new(operation.input, input_columns)
        .into_report()
        .change_context(Error::internal_msg("error creating single consumer helper"))?;

    match operation
        .time
        .ok_or_else(|| invalid_operation!("missing time"))?
    {
        Time::Computed(computed) => {
            error_stack::ensure!(
                computed.producing_operation == operation.input,
                crate::execute::error::invalid_operation!(
                    "producing operation and input operation do not match"
                )
            );
            let time_input_column = computed.input_column as usize;
            ShiftToColumnOperation::try_new(time_input_column, incoming_stream, helper)
                .into_report()
                .change_context(Error::internal_msg("failed to create operation"))
        }
        Time::Literal(timestamp) => {
            let timestamp =
                NaiveDateTime::from_timestamp_opt(timestamp.seconds, timestamp.nanos as u32)
                    .ok_or_else(|| invalid_operation!("invalid literal timestamp"))?;
            let timestamp = timestamp.timestamp_nanos();
            ShiftToLiteralOperation::try_new(timestamp, incoming_stream, helper)
                .into_report()
                .change_context(Error::internal_msg("failed to create operation"))
        }
    }
}

impl ShiftToLiteralOperation {
    fn try_new(
        timestamp: i64,
        incoming_stream: ReceiverStream<Batch>,
        helper: SingleConsumerHelper,
    ) -> anyhow::Result<BoxedOperation> {
        let time_array = Arc::new(TimestampNanosecondArray::from_value(timestamp, 100_000));

        Ok(Box::new(Self {
            timestamp,
            next_subsort: 0,
            time_array,
            incoming_stream,
            helper,
        }))
    }

    /// Returns the next record batch from the shift-to-literal.
    ///
    /// Only incoming rows with times before the destination (literal) are
    /// produced. Thus, if a batch is entirely before the destination, we can
    /// pass the entire batch through with new time and subsort columns. If
    /// the batch is entirely after the destination, we can report `None` since
    /// there is nothing to output, nor will there ever be anything else to
    /// output. If the batch includes rows from before and after the target
    /// time, we split it into the part from before and the part from after,
    /// and then apply the same reasoning.
    fn create_input(&mut self, incoming: Batch) -> anyhow::Result<Option<InputBatch>> {
        if incoming.num_rows() == 0 {
            return Ok(Some(
                self.helper
                    .new_input_batch(incoming, |input| Ok(input.clone()))?,
            ));
        }

        // TODO: Use the timestamped batches instead of having to compute it here?
        let incoming_time: &TimestampNanosecondArray =
            downcast_primitive_array(incoming.column(0).as_ref())?;
        let min_incoming_time = incoming_time.value(0);
        let max_incoming_time = incoming_time.value(incoming.num_rows() - 1);

        if min_incoming_time > self.timestamp {
            // Once the minimum time is past the timestamp the shift will never
            // output anything more. However, we need to output an empty batch
            // with the correct bounds, to allow downstream consumers to merge, tick,
            // or otherwise progress up to the correct time.
            let empty_batch = RecordBatch::new_empty(incoming.schema());
            let incoming = incoming.with_data(empty_batch);
            return Ok(Some(
                self.helper.new_input_batch(incoming, |i| Ok(i.clone()))?,
            ));
        } else if max_incoming_time > self.timestamp {
            let incoming_times = incoming_time.values();
            // If the max incoming time is after the timestamp it means we won't
            // be able to shift the entire incoming batch.
            let length = match incoming_times.binary_search(&self.timestamp) {
                Ok(mut length) => {
                    while incoming_times.get(length + 1) == Some(&self.timestamp) {
                        length += 1
                    }
                    length
                }
                Err(insertion) => {
                    // If we'd insert `time` at position `N`, it means that [0,
                    // N) are less than time and [N, len) are greater. Thus, the
                    // length of the outputtable section is N.
                    insertion
                }
            };
            incoming.with_data(incoming.data.slice(0, length));
        } else {
            // If we get here, we're able to shift the entire incoming batch
            // to the given timestamp without moving backwards, so don't update
            // the record batch
        };
        let length = incoming.data.num_rows();

        if length > self.time_array.len() {
            // Resize the time array.
            self.time_array =
                Arc::new(TimestampNanosecondArray::from_value(self.timestamp, length));
        }

        let time = self.time_array.slice(0, length);

        // Create a subsort column.
        let subsort: UInt64Array =
            (self.next_subsort..self.next_subsort + (length as u64)).collect();
        let subsort = Arc::new(subsort);
        self.next_subsort += length as u64;

        let key_hash = incoming.data.column(2).clone();

        self.helper
            .new_input_batch_with_keys(&incoming, time, subsort, key_hash, |column| {
                Ok(column.clone())
            })
    }

    /// TODO: This is an artifact of the old operation API. It may be possible
    /// to cleanup the implementation by moving it to occur directly
    /// within the `execute` logic.
    async fn try_next(&mut self) -> anyhow::Result<Option<InputBatch>> {
        if let Some(incoming) = self.incoming_stream.next().await {
            // If the literal case returns `None`, there is nothing
            // to output now nor will there ever be.
            self.create_input(incoming)
        } else {
            // Incoming batches shifted to a literal were output as
            // they arrived. If we reach here, there must be nothing
            // to output.
            //
            // If we passed the literal time, then we already returned
            // `None` from the above call to `self.create_input(...)`.
            // But, the literal may be after the time in all batches,
            // in which case we'll hit this first (out of input).
            Ok(None)
        }
    }
}

#[async_trait]
impl Operation for ShiftToLiteralOperation {
    fn restore_from(
        &mut self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()> {
        self.helper.restore_from(operation_index, compute_store)?;
        let subsort: Option<u64> =
            compute_store.get(&StoreKey::new_shift_to_subsort(operation_index))?;
        self.next_subsort = if let Some(s) = subsort { s } else { 0 };
        Ok(())
    }

    fn store_to(&self, operation_index: u8, compute_store: &ComputeStore) -> anyhow::Result<()> {
        self.helper.store_to(operation_index, compute_store)?;
        compute_store.put_proto(
            &StoreKey::new_shift_to_subsort(operation_index),
            &self.next_subsort,
        )?;
        Ok(())
    }

    async fn execute(
        &mut self,
        sender: tokio::sync::mpsc::Sender<InputBatch>,
    ) -> error_stack::Result<(), Error> {
        while let Some(input) = self
            .try_next()
            .await
            .into_report()
            .change_context(Error::internal())?
        {
            sender
                .send(input)
                .await
                .into_report()
                .change_context(Error::internal())?;
        }
        Ok(())
    }
}

impl ShiftToColumnOperation {
    fn try_new(
        time_input_column: usize,
        incoming_stream: ReceiverStream<Batch>,
        helper: SingleConsumerHelper,
    ) -> anyhow::Result<BoxedOperation> {
        Ok(Box::new(Self {
            shift_time_column: time_input_column,
            pending: None,
            incoming_stream,
            helper,
        }))
    }

    fn create_input(&mut self, incoming: Batch) -> anyhow::Result<Option<InputBatch>> {
        // The incoming batch's upper bound is the max time we can output to.
        let lower_bound = incoming.lower_bound;
        let upper_bound = incoming.upper_bound;

        let lower_bound_time = lower_bound.time;
        let upper_bound_time = upper_bound.time;

        if incoming.num_rows() == 0 {
            if self.pending.is_none() {
                return Ok(Some(
                    self.helper
                        .new_input_batch(incoming, |input| Ok(input.clone()))?,
                ));
            } else {
                return self.split_output(lower_bound_time, upper_bound_time);
            }
        }
        // Create the "input" batch to be buffered. This requires
        // changing the time to the `time_input_column` (filtering out invalid
        // times).
        let incoming_times = incoming.column(0);
        let incoming_times_primitive: &TimestampNanosecondArray =
            downcast_primitive_array(incoming_times.as_ref())?;
        let shifted_times = incoming.column(self.shift_time_column);
        let shifted_times_primitive: &TimestampNanosecondArray =
            downcast_primitive_array(shifted_times.as_ref())?;

        debug_assert_eq!(incoming_times.null_count(), 0);
        // We don't need to specially handle the case of `null` values:
        //
        // 1. `lt_eq` will consider `null` less than `non-null`.
        // 2. `incoming_times` does not contain any `null` value.
        // 3. if `shifted_times` is null, it will be seen as a backward
        //    shift (since it is less than `incoming` and filtered out).
        let is_forward_shift = arrow::compute::kernels::comparison::lt_eq(
            incoming_times_primitive,
            shifted_times_primitive,
        )?;

        let num_filtered = is_forward_shift.len() - is_forward_shift.values().count_set_bits();
        if num_filtered != 0 {
            let num_null = shifted_times.null_count();
            let num_backward = num_filtered - num_null;

            info!(
                num_null,
                num_backward, "shift_to dropping null or backwards times"
            )
        }

        // Determine the sort indices to use for `taking` batches.
        let sort_indices = arrow::compute::lexsort_to_indices(
            &[
                // The shifted batches should be sorted by the new time.
                // So we use that as the first sort column.
                SortColumn {
                    values: shifted_times.clone(),
                    options: None,
                },
                // We use the incoming (times, subsort, key_hash) as additional sort columns
                // to preserve the original order. This wouldn't be necessary if we had a
                // stable `lexsort_to_indices` (eg., breaking ties with the index).
                SortColumn {
                    values: incoming.column(0).clone(),
                    options: None,
                },
                SortColumn {
                    values: incoming.column(1).clone(),
                    options: None,
                },
                SortColumn {
                    values: incoming.column(2).clone(),
                    options: None,
                },
            ],
            None,
        )?;

        // Filter out the null and backward shifts. This creates a set of
        // sort indices which (when used with take) perform both the filtering
        // and ordering necessary.
        let take_sorted_indices = arrow::compute::take(&is_forward_shift, &sort_indices, None)?;
        let take_sorted_indices = downcast_boolean_array(&take_sorted_indices)?;

        let sort_indices = arrow::compute::filter(&sort_indices, take_sorted_indices)?;
        let sort_indices: &UInt32Array = downcast_primitive_array(sort_indices.as_ref())?;

        let transform = |column: &ArrayRef| {
            arrow::compute::take(column.as_ref(), sort_indices, None).context("take for shift_to")
        };
        let time = transform(shifted_times)?;

        // Create a subsort array of the given size. We do this sequentially so that
        // we know the order of elements arriving is preserved if shifted to the same
        let subsort = transform(incoming.column(1))?;
        let key_hash = transform(incoming.column(2))?;

        let input = self
            .helper
            .new_input_batch_with_keys(&incoming, time, subsort, key_hash, transform)?;

        if self.pending.is_none() {
            // 1. Pending set is empty
            if let Some(input) = input {
                // 1a. New input is non-empty
                self.add_input(input)?;
                self.split_output(lower_bound_time, upper_bound_time)
            } else {
                // 1b. New input is also empty
                Ok(Some(InputBatch::new_empty(
                    incoming.schema(),
                    incoming.lower_bound,
                    incoming.upper_bound,
                )))
            }
        } else {
            // 2. Pending set is non-empty
            if let Some(input) = input {
                // 2a. New input is non-empty
                // Merge new input to pending set, then split to get output.
                self.add_input(input)?;
                self.split_output(lower_bound_time, upper_bound_time)
            } else {
                // 2b. New input is empty
                // No need to add an empty input. Split at the incoming
                // batch's upper bound.
                self.split_output(lower_bound_time, upper_bound_time)
            }
        }
    }

    /// Adds an incoming record batch to the pending batch.
    fn add_input(&mut self, input: InputBatch) -> anyhow::Result<()> {
        self.pending = Some(if let Some(right) = self.pending.take() {
            let left = input;
            let merge_result =
                sparrow_merge::old::binary_merge(left.as_merge_input()?, right.as_merge_input()?)?;

            // TODO: Binary merge optimization opportunity.
            // HACK: We'd like the binary merge to return a boolean array `BooleanArray`
            // indicating whether to use values from the left/right. Since it doesn't, we
            // create one using the `is_not_null` kernel.
            //
            // NOTE: since we generated unique subsort indices, we know that any shifted
            // row is either on the left or right, but not both. Thus, we only need to
            // examine the left.
            let take_left = arrow::compute::is_not_null(&merge_result.take_a)?;
            let group_indices = spread_zip(
                &take_left,
                left.grouping.group_indices(),
                right.grouping.group_indices(),
            )?;
            let group_indices: &UInt32Array = downcast_primitive_array(group_indices.as_ref())?;
            // TODO: We shouldn't need to clone the array. Instead, we should be able
            // to pass an owned reference to the grouping indices. But... the clone
            // shouldn't be too expensive, and that would require API changes.
            let group_indices: UInt32Array = UInt32Array::from(group_indices.to_data());
            let num_groups = left.grouping.num_groups().max(right.grouping.num_groups());
            let grouping = GroupingIndices::new(num_groups, group_indices);

            let input_columns = left
                .input_columns
                .iter()
                .zip_eq(right.input_columns)
                .map(|(left, right)| {
                    spread_zip(&take_left, left.as_ref(), right.as_ref())
                        .context("zip for shift_to")
                })
                .try_collect()?;

            anyhow::ensure!(
                !merge_result.time.is_empty(),
                "Expected non-empty merge result"
            );

            let time = Arc::new(merge_result.time);
            let subsort = Arc::new(merge_result.subsort);
            let key_hash = Arc::new(merge_result.key_hash);
            let key_triples = KeyTriples::try_new(time.clone(), subsort.clone(), key_hash.clone())?;
            let lower_bound = key_triples.value(0);
            let upper_bound = key_triples.value(key_triples.len() - 1);
            InputBatch {
                time,
                subsort,
                key_hash,
                grouping,
                input_columns,
                lower_bound,
                upper_bound,
            }
        } else {
            input
        });

        Ok(())
    }

    fn split_output(
        &mut self,
        incoming_lower_bound_time: i64,
        incoming_upper_bound_time: i64,
    ) -> anyhow::Result<Option<InputBatch>> {
        if let Some(pending) = self.pending.take() {
            // Create the input batch from by slicing the pending rows up to the max
            // timestamp.
            let pending_times: &TimestampNanosecondArray =
                downcast_primitive_array(pending.time.as_ref())?;
            let pending_times = pending_times.values();
            let split_length = match pending_times.binary_search(&incoming_upper_bound_time) {
                Ok(mut found) => {
                    // We found the time. We need to run back from that to find the *first*
                    // occurrence of that time.
                    while found > 0 && pending_times[found - 1] == incoming_upper_bound_time {
                        found -= 1;
                    }
                    found
                }
                Err(not_found) => not_found,
            };
            let (prefix, suffix) = pending.split(split_length)?;
            // TODO: What if suffix is empty?
            self.pending = Some(suffix);

            // The subsort column can naively monotonically increase and preserve the
            // uniqueness invariant.
            let subsort = Arc::new(UInt64Array::from_iter_values(0..(prefix.time.len() as u64)));

            // Fix the bounds based on the generated subsort column above.
            let mut lower_bound = prefix.lower_bound;
            lower_bound.time = cmp::min(incoming_lower_bound_time, lower_bound.time);
            lower_bound.subsort = 0;
            let mut upper_bound = prefix.upper_bound;
            upper_bound.time = cmp::min(incoming_upper_bound_time, upper_bound.time);

            let upper_subsort = if prefix.time.len() == 0 {
                0
            } else {
                // 0-indexed, so sub 1
                prefix.time.len() as u64 - 1
            };
            upper_bound.subsort = upper_subsort;

            let prefix = InputBatch {
                time: prefix.time,
                subsort,
                key_hash: prefix.key_hash,
                grouping: prefix.grouping,
                input_columns: prefix.input_columns,
                lower_bound,
                upper_bound,
            };
            Ok(Some(prefix))
        } else {
            Ok(None)
        }
    }

    /// TODO: This is an artifact of the old operation API. It may be possible
    /// to cleanup the implementation by moving it to occur directly
    /// within the `execute` logic.
    async fn try_next(&mut self) -> anyhow::Result<Option<InputBatch>> {
        loop {
            if let Some(incoming) = self.incoming_stream.next().await {
                let result = self.create_input(incoming)?;
                if result.is_some() {
                    // For the computed case, we can't stop until the
                    // incoming stream is empty. So, if there is nothing
                    // to output in response to this incoming batch, we
                    // just continue looping and wait for the next
                    // incoming batch.
                    return Ok(result);
                }
            } else {
                // If there is anything in the pending batch, this will output it.
                // We'll only reach this case if the incoming stream has reached the end,
                // so we can output the pending batch. The next call to `try_next` should
                // see that `computed.pending.take()` returns `None`, indicating there is
                // nothing else.

                let result = self.pending.take();
                // Update the subsort column to be monotonically increasing from 0,
                // to match the pattern of the merged results. We don't update the
                // subsort column in the pending batch, as it helps keep the correct
                // ordering before they're output.
                if let Some(result) = result {
                    let subsort =
                        Arc::new(UInt64Array::from_iter_values(0..(result.time.len() as u64)));
                    // Fix the bounds based on the generated subsort column above.
                    let mut lower_bound = result.lower_bound;
                    lower_bound.subsort = 0;
                    let mut upper_bound = result.upper_bound;
                    upper_bound.subsort = result.time.len() as u64;

                    return Ok(Some(InputBatch {
                        time: result.time,
                        subsort,
                        key_hash: result.key_hash,
                        grouping: result.grouping,
                        input_columns: result.input_columns,
                        lower_bound,
                        upper_bound,
                    }));
                } else {
                    return Ok(None);
                }
            }
        }
    }
}

#[async_trait]
impl Operation for ShiftToColumnOperation {
    fn restore_from(
        &mut self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()> {
        self.helper.restore_from(operation_index, compute_store)?;
        Ok(())
    }

    fn store_to(&self, operation_index: u8, compute_store: &ComputeStore) -> anyhow::Result<()> {
        self.helper.store_to(operation_index, compute_store)?;
        Ok(())
    }

    async fn execute(
        &mut self,
        sender: tokio::sync::mpsc::Sender<InputBatch>,
    ) -> error_stack::Result<(), Error> {
        while let Some(input) = self
            .try_next()
            .await
            .into_report()
            .change_context(Error::internal())?
        {
            sender
                .send(input)
                .await
                .into_report()
                .change_context(Error::internal())?;
        }
        Ok(())
    }
}
