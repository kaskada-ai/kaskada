use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, BooleanArray, PrimitiveArray, TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::{Field, Schema, SchemaRef, TimeUnit, TimestampNanosecondType, UInt64Type};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::StreamExt;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use sparrow_api::kaskada::v1alpha::operation_plan;
use sparrow_arrow::downcast::{downcast_boolean_array, downcast_primitive_array};
use sparrow_core::KeyTriples;
use sparrow_instructions::{ComputeStore, StoreKey};
use tokio_stream::wrappers::ReceiverStream;

use super::BoxedOperation;
use crate::execute::error::{invalid_operation, Error};
use crate::execute::operation::expression_executor::InputColumn;
use crate::execute::operation::single_consumer_helper::SingleConsumerHelper;
use crate::execute::operation::{InputBatch, Operation};
use crate::key_hash_index::KeyHashIndex;
use crate::Batch;

/// Operation for `ShiftUntil`.
///
/// This handles the case of shifting input rows forward to arbitrary points in
/// time when the predicate evaluates to true.
///
/// This operation stores rows until such time the predicate evaluates to true
/// for a key associated with a stored row.
#[derive(Debug)]
pub(super) struct ShiftUntilOperation {
    condition_input_column: usize,
    incoming_stream: ReceiverStream<Batch>,
    helper: SingleConsumerHelper,
    /// retained schema contains key+hash followed by all the incoming columns
    retained_schema: Arc<Schema>,
    /// outgoing schema contains the triplet (time, subsort, key_hash) followed
    /// by all the incoming columns
    outgoing_schema: Arc<Schema>,
    pending: Vec<RetainedBatch>,
    subsort_start: u64,
}

impl ShiftUntilOperation {
    pub(super) fn create(
        operation: operation_plan::ShiftUntilOperation,
        input_channels: Vec<tokio::sync::mpsc::Receiver<Batch>>,
        input_columns: &[InputColumn],
    ) -> error_stack::Result<BoxedOperation, super::Error> {
        let mut pending_schema_fields = vec![Field::new(
            "entity_hash",
            arrow::datatypes::DataType::UInt64,
            false,
        )];

        let mut outgoing_schema_fields = vec![
            Field::new(
                "_time",
                arrow::datatypes::DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("_subsort", arrow::datatypes::DataType::UInt64, false),
            Field::new("_key_hash", arrow::datatypes::DataType::UInt64, false),
        ];

        for (pos, input) in input_columns.iter().enumerate() {
            let value = Field::new(pos.to_string().as_str(), input.data_type.clone(), true);
            pending_schema_fields.push(value.clone());
            outgoing_schema_fields.push(value);
        }

        let retained_schema = Arc::new(Schema::new(pending_schema_fields));
        let outgoing_schema = Arc::new(Schema::new(outgoing_schema_fields));
        let input_channel = input_channels
            .into_iter()
            .exactly_one()
            .into_report()
            .change_context(Error::internal_msg("expected one channel"))?;

        let condition_input_column = operation
            .condition
            .ok_or_else(|| invalid_operation!("missing condition"))?
            .input_column as usize;

        Ok(Box::new(Self {
            condition_input_column,
            incoming_stream: ReceiverStream::new(input_channel),
            helper: SingleConsumerHelper::try_new(operation.input, input_columns)
                .into_report()
                .change_context(Error::internal_msg("error creating single consumer helper"))?,
            retained_schema,
            outgoing_schema,
            pending: vec![],
            subsort_start: 0,
        }))
    }

    fn create_input(&mut self, incoming: Batch) -> anyhow::Result<Option<InputBatch>> {
        if incoming.num_rows() == 0 {
            return Ok(Some(
                self.helper
                    .new_input_batch(incoming, |input| Ok(input.clone()))?,
            ));
        }

        let mut current_batch = self.create_current_batch(&incoming)?;
        let condition_column = incoming.column(self.condition_input_column);
        let condition = downcast_boolean_array(condition_column)?;

        let mut subsort_start = self.subsort_start;
        // Iterate through the `predicate`, attempt to output rows if `true`.
        let mut output_batches = Vec::new();
        for (index, predicate) in condition.iter().enumerate() {
            if let Some(true) = predicate {
                let target_time: &PrimitiveArray<TimestampNanosecondType> =
                    downcast_primitive_array(incoming.column(0))?;
                let target_time = target_time.value(index);

                let target_key: &PrimitiveArray<UInt64Type> =
                    downcast_primitive_array(incoming.column(2))?;
                let target_key = target_key.value(index);

                // Output the associated key from any past batches.
                for pending_batch in self.pending.iter_mut() {
                    if let Some(batch) = pending_batch.batch_to_emit(target_key)? {
                        let output_batch = retained_to_output_batch(
                            &mut subsort_start,
                            &self.outgoing_schema,
                            target_time,
                            batch,
                        )?;
                        output_batches.push(output_batch);
                    }
                }

                // Output all rows from the current batch associated with the key.
                if let Some(batch) = current_batch.batch_to_emit(target_key, index)? {
                    let output_batch = retained_to_output_batch(
                        &mut subsort_start,
                        &self.outgoing_schema,
                        target_time,
                        batch,
                    )?;
                    output_batches.push(output_batch);
                }
            }
        }

        self.subsort_start = subsort_start;

        // Convert the `CurrentBatch` to a `RetainedBatch` and store it.
        if let Some(retained) = current_batch.retain()? {
            self.pending.push(retained);
        }

        let final_output_record_batch = arrow::compute::kernels::concat::concat_batches(
            &self.outgoing_schema,
            &output_batches,
        )?;

        if final_output_record_batch.num_rows() == 0 {
            let empty_record_batch = RecordBatch::new_empty(self.outgoing_schema.clone());
            self.create_input_batch_from_empty_record_batch(empty_record_batch, incoming)
        } else {
            let final_output_batch = Batch::try_new_from_batch(final_output_record_batch)?;
            self.create_input_batch_from_record_batch(final_output_batch)
        }
    }

    // `self.helper` contains functions that rely on the original input column index
    // given at the operation's creation time. We altered the indeces thus this
    // helper function.
    fn create_input_batch_from_record_batch(
        &mut self,
        final_output_batch: Batch,
    ) -> Result<Option<InputBatch>, anyhow::Error> {
        let time = final_output_batch.column(0).clone();
        let subsort = final_output_batch.column(1).clone();
        let key_hash = final_output_batch.column(2).clone();
        let triplets = KeyTriples::try_new(time.clone(), subsort.clone(), key_hash.clone())?;
        let lower_bound = triplets.value(0);
        let upper_bound = triplets.value(triplets.len() - 1);
        let grouping = KeyHashIndex::default()
            .get_or_update_indices(downcast_primitive_array(key_hash.as_ref())?)?;
        let input_columns: Vec<Arc<dyn Array>> = (3..self.outgoing_schema.fields().len())
            .map(|index| final_output_batch.column(index).clone())
            .collect();

        let result = InputBatch {
            time,
            subsort,
            key_hash,
            grouping,
            input_columns,
            lower_bound,
            upper_bound,
        };
        Ok(Some(result))
    }

    // When there are no rows we pass in the original bounds (lower and upper)
    fn create_input_batch_from_empty_record_batch(
        &mut self,
        empty_record_batch: RecordBatch,
        incoming: Batch,
    ) -> Result<Option<InputBatch>, anyhow::Error> {
        let time = empty_record_batch.column(0).clone();
        let subsort = empty_record_batch.column(1).clone();
        let key_hash = empty_record_batch.column(2).clone();
        let input_columns: Vec<Arc<dyn Array>> = (3..self.outgoing_schema.fields().len())
            .map(|index| empty_record_batch.column(index).clone())
            .collect();
        let grouping = KeyHashIndex::default()
            .get_or_update_indices(downcast_primitive_array(key_hash.as_ref())?)?;
        Ok(Some(InputBatch {
            time,
            subsort,
            key_hash,
            grouping,
            input_columns,
            upper_bound: incoming.upper_bound,
            lower_bound: incoming.lower_bound,
        }))
    }

    /// Create a RecordBatch containing the key hash and the value columns.
    /// This is used for intermediate storage within the ShiftUntil sink.
    fn create_current_batch(&mut self, input_batch: &Batch) -> anyhow::Result<CurrentBatch> {
        let schema = self.retained_schema.as_ref();
        let capacity = schema.fields().len();
        let mut columns = Vec::with_capacity(capacity);

        columns.push(input_batch.column(2).clone());
        for input_column_index in &self.helper.incoming_columns {
            columns.push(input_batch.column(*input_column_index).clone());
        }

        let batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
        let batch_rows = batch.num_rows();
        let current_batch = CurrentBatch {
            batch,
            already_emitted: BooleanArray::from(vec![false; batch_rows]),
        };
        Ok(current_batch)
    }
}

#[async_trait]
impl Operation for ShiftUntilOperation {
    fn restore_from(
        &mut self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()> {
        self.helper.restore_from(operation_index, compute_store)?;
        let subsort_key = StoreKey::new_shift_to_subsort(operation_index);
        let subsort = compute_store.get(&subsort_key)?;
        if let Some(s) = subsort {
            self.subsort_start = s;
        } else {
            self.subsort_start = 0;
        }

        let pending_key = StoreKey::new_shift_until_retained_batches(operation_index);
        let pending: Option<Vec<RetainedBatch>> = compute_store.get(&pending_key)?;
        if let Some(p) = pending {
            self.pending = p;
        } else {
            self.pending = vec![];
        }

        Ok(())
    }

    fn store_to(&self, operation_index: u8, compute_store: &ComputeStore) -> anyhow::Result<()> {
        self.helper.store_to(operation_index, compute_store)?;
        compute_store.put(
            &StoreKey::new_shift_to_subsort(operation_index),
            &self.subsort_start,
        )?;

        compute_store.put(
            &StoreKey::new_shift_until_retained_batches(operation_index),
            &self.pending,
        )?;

        Ok(())
    }

    async fn execute(
        &mut self,
        sender: tokio::sync::mpsc::Sender<InputBatch>,
    ) -> error_stack::Result<(), Error> {
        while let Some(incoming) = self.incoming_stream.next().await {
            if let Some(input) = self
                .create_input(incoming)
                .into_report()
                .change_context(Error::internal())?
            {
                sender
                    .send(input)
                    .await
                    .into_report()
                    .change_context(Error::internal())?
            }
        }

        Ok(())
    }
}

/// The representation for the current batch.
///
/// Once a `CurrentBatch` is processed, it will be converted into a
/// `RetainedBatch` for storage, if rows exist.
struct CurrentBatch {
    /// The data for the current batch.
    batch: RecordBatch,
    /// A bitset detailing which rows in this batch have been emitted.
    ///
    /// Since the `batch` is iterated over, this bitset is dynamically updated
    /// to ensure rows are not emitted more than once.
    already_emitted: BooleanArray,
}

impl CurrentBatch {
    /// Return the batch to emit for the given key hash.
    ///
    /// Includes all rows from the current batch that haven't already been
    /// emitted associated with the given key hash up to (and including) the
    /// current row.
    fn batch_to_emit(
        &mut self,
        key_hash: u64,
        current_row: usize,
    ) -> anyhow::Result<Option<RecordBatch>> {
        // NOTE: The following can potentially be optimized by operating over 64-bit
        // chunks, applying the logic within the loop, and avoiding the
        // intermediate arrays.

        // Gets the array matching the key hash
        let key_arr: &UInt64Array = downcast_primitive_array(self.batch.columns()[0].as_ref())?;
        let key_filter = arrow::compute::eq_scalar(key_arr, key_hash)?;

        // `key_filter & !already_emitted` gives the array of rows that match and have
        // not been emitted.
        let not_emitted_already = arrow::compute::not(&self.already_emitted)?;
        let rows_to_emit_not_filtered_by_time =
            arrow::compute::and(&key_filter, &not_emitted_already)?;

        // Creates a BooleanArray with length equal to the batch size, with valid bits
        // up to the `current_row + 1`. As `current_row` comes in as the index, we add 1
        // to ensure it is included in the valid rows to emit.
        let current_row = current_row + 1;
        debug_assert!(
            current_row <= self.batch.num_rows(),
            "Current row should not be greater than the number of rows in the batch"
        );
        let valid_rows_mask: BooleanArray = {
            let valid_rows = std::iter::repeat(true).take(current_row);
            let invalid_rows = std::iter::repeat(false).take(self.batch.num_rows() - current_row);
            valid_rows.chain(invalid_rows).map(Some).collect()
        };

        // AND the `valid_rows_mask` with the rows to filter out rows occurring after
        // the current time
        let valid_row_filter =
            arrow::compute::and(&valid_rows_mask, &rows_to_emit_not_filtered_by_time)?;
        let rows_to_emit =
            arrow::compute::kernels::filter::filter_record_batch(&self.batch, &valid_row_filter)?;

        if rows_to_emit.num_rows() > 0 {
            // Emitting some rows, so update the `already_emitted` bitset
            self.already_emitted = arrow::compute::or(&self.already_emitted, &valid_row_filter)?;
            Ok(Some(rows_to_emit))
        } else {
            Ok(None)
        }
    }

    /// Convert the current batch to a retained batch.
    fn retain(self) -> anyhow::Result<Option<RetainedBatch>> {
        // First, filter the batch to only those rows that haven't been emitted.
        let not_emitted = arrow::compute::not(&self.already_emitted)?;
        let remaining =
            arrow::compute::kernels::filter::filter_record_batch(&self.batch, &not_emitted)?;
        let remaining_rows = remaining.num_rows();

        // If we have emitted all rows for this batch, do not retain anything.
        if remaining_rows == 0 {
            return Ok(None);
        }

        // Compute the key hashes that occur in the remaining rows.
        let keys: &UInt64Array = downcast_primitive_array(remaining.columns()[0].as_ref())?;
        let keys: HashSet<u64> = keys.iter().flatten().collect();

        Ok(Some(RetainedBatch {
            batch: remaining,
            keys,
            remaining_rows,
        }))
    }
}

/// The representation for a stored batch.
///
/// Rows in this batch are stored until such time the predicate for a given
/// entity evaluates to true.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct RetainedBatch {
    /// The retained data.
    ///
    /// This may include data that has already been emitted (with keys that are
    /// no longer in the key set). This allows us to avoid excessive
    /// recreation of the batch.
    #[serde(with = "sparrow_arrow::serde::record_batch")]
    batch: RecordBatch,
    /// Keys part of the retained batch that have not been emitted.
    keys: HashSet<u64>,
    /// The number of rows in the batch that have not been emitted.
    ///
    /// This, along with the `total_rows` may be used to compute how many rows
    /// are wasted in storage, and to rewrite the batch as appropriate.
    remaining_rows: usize,
}

impl RetainedBatch {
    /// Return the batch to emit for the given key hash.
    ///
    /// Includes all rows from this batch that haven't already been emitted,
    /// determined by the given key_hash.
    fn batch_to_emit(&mut self, key_hash: u64) -> anyhow::Result<Option<RecordBatch>> {
        if self.keys.remove(&key_hash) {
            // NOTE: The following can potentially be optimized by operating over 64-bit
            // chunks, avoiding the intermediate arrays.

            // If the key was present, we haven't sent rows for this key prior.
            // Find all rows matching the key, and return them.
            let key_arr: &UInt64Array = downcast_primitive_array(self.batch.columns()[0].as_ref())?;
            let row_filter = arrow::compute::eq_scalar(key_arr, key_hash)?;
            let rows_to_emit =
                arrow::compute::kernels::filter::filter_record_batch(&self.batch, &row_filter)?;

            // Subtract the number of rows we're emitting; allows management/optimization of
            // retained batches.
            self.remaining_rows -= rows_to_emit.num_rows();

            Ok(Some(rows_to_emit))
        } else {
            Ok(None)
        }
    }
}

/// Given a record batch with the `retained_schema`, add the output time and
/// subsort to make a batch with `output_schema`
fn retained_to_output_batch(
    subsort_start: &mut u64,
    output_schema: &SchemaRef,
    time_ns: i64,
    retained_batch: RecordBatch,
) -> anyhow::Result<RecordBatch> {
    let rows = retained_batch.num_rows();

    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(output_schema.fields().len());
    let time_values = std::iter::repeat(time_ns).take(rows);
    let time_column = TimestampNanosecondArray::from_iter_values(time_values);
    columns.push(Arc::new(time_column));

    let subsort_values = *subsort_start..(*subsort_start + rows as u64);
    let subsort_column = UInt64Array::from_iter_values(subsort_values);
    columns.push(Arc::new(subsort_column));
    *subsort_start += rows as u64;

    for col in retained_batch.columns() {
        columns.push(col.clone());
    }
    Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
}
