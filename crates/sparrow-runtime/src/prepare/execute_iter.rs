use std::pin::Pin;
use std::sync::{Arc, PoisonError};
use std::task::Poll;

use arrow::array::{PrimitiveArray, TimestampNanosecondArray};
use arrow::compute::SortColumn;
use arrow::datatypes::{ArrowPrimitiveType, SchemaRef, TimestampNanosecondType, UInt64Type};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, IntoReportCompat, Report, ResultExt};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::{slice_plan, TableConfig};
use sparrow_core::{downcast_primitive_array, TableSchema};

use crate::execute::key_hash_inverse::ThreadSafeKeyHashInverse;
use crate::prepare::slice_preparer::SlicePreparer;
use crate::prepare::Error;
use crate::RawMetadata;

use super::column_behavior::ColumnBehavior;

/// The bounded lateness parameter, which configures the delay in watermark time.
///
/// In practical terms, this allows for items in the stream to be within 1 second
/// compared to the max timestamp read.
const BOUNDED_LATENESS: i64 = 1000;

/// An iterator over prepare batches. TODO
///
/// In addition to iterating, this is responsible for the following:
///
/// 1. Inserting the time column, subsort column, and key hash from source to
///    the batch
/// 2. Casts required columns
/// 3. Sorts the record batches by the time column, subsort column, and key hash
pub struct ExecuteIter<'a> {
    reader: BoxStream<'a, Result<RecordBatch, ArrowError>>,
    /// The final schema to produce, including the 3 key columns
    prepared_schema: SchemaRef,
    /// Instructions for creating the resulting batches from a read
    columns: Vec<ColumnBehavior>,
    /// The slice preparer to operate on a per batch basis
    slice_preparer: SlicePreparer,
    /// The key hash inverse table
    key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
    /// The table config
    table_config: &'a TableConfig,
    /// Bounded disorder assumes an allowed amount of lateness in events,
    /// which then allows this node to advance its watermark up to event
    /// time (t - delta).
    ///
    /// This simple bound is a good start, but we can improve on this hueristic
    /// by statistically modeling event behavior and adapting the watermark accordingly.
    bounded_lateness: i64,
    /// The watermark represents a timestamp beyond which the system assumes
    /// that all data with earlier timestamps has been produced.
    watermark: i64,
    /// The leftovers are events that have been read from the stream but have not
    /// been produced, as the watermark has not advanced far enough.
    ///
    /// Note this implies that if a period of inactivity in the stream occurs, we
    /// cannot produce the last n events until the watermark advances when new
    /// events are produced to the stream.
    leftovers: Option<RecordBatch>,
}

impl<'a> Stream for ExecuteIter<'a> {
    /// This stream may return None in the case where messages were read from
    /// the underlying stream, but the watermark has not advanced far enough to
    /// produce any messages.
    ///
    /// The correct behavior for a consumer would be to ignore the empty message
    /// and continue polling the stream.
    type Item = Result<Option<RecordBatch>, ExecuteIterWrapper>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let reader = &mut self.reader;
        Pin::new(reader).poll_next(cx).map(|opt| match opt {
            Some(Ok(batch)) => {
                let result = self
                    .get_mut()
                    .prepare_next_batch(batch)
                    .map_err(|err| err.change_context(Error::ReadingBatch).into());
                Some(result)
            }
            Some(Err(err)) => Some(Err(Report::new(err)
                .change_context(Error::ReadingBatch)
                .into())),
            None => {
                // Indicates that the stream has likely closed unexpectedly
                tracing::error!(
                    "Unexpected empty message from stream - likely the stream has closed"
                );
                Some(Err(Report::new(Error::ReadInput)
                    .change_context(Error::Internal)
                    .into()))
            }
        })
    }
}

/// the Stream API works best with Result types that include an actual
/// std::error::Error.  Simple things work okay with arbitrary Results,
/// but things like TryForEach do not.  Since error_stack Errors do
/// not implement std::error::Error, we wrap them in this.
#[derive(Debug)]
pub struct ExecuteIterWrapper(pub Report<Error>);

impl std::fmt::Display for ExecuteIterWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for ExecuteIterWrapper {}

impl From<Report<Error>> for ExecuteIterWrapper {
    fn from(error: Report<Error>) -> Self {
        ExecuteIterWrapper(error)
    }
}

impl<T> From<PoisonError<T>> for ExecuteIterWrapper {
    fn from(error: PoisonError<T>) -> Self {
        ExecuteIterWrapper(Report::new(Error::Internal).attach_printable(format!("{:?}", error)))
    }
}

impl From<std::io::Error> for ExecuteIterWrapper {
    fn from(error: std::io::Error) -> Self {
        ExecuteIterWrapper(Report::new(Error::Internal).attach_printable(format!("{:?}", error)))
    }
}

impl From<anyhow::Error> for ExecuteIterWrapper {
    fn from(error: anyhow::Error) -> Self {
        ExecuteIterWrapper(Report::new(Error::Internal).attach_printable(format!("{:?}", error)))
    }
}

impl<'a> std::fmt::Debug for ExecuteIter<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecuteIter")
            .field("prepared_schema", &self.prepared_schema)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

impl<'a> ExecuteIter<'a> {
    pub fn try_new(
        reader: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send + 'static,
        config: &'a TableConfig,
        raw_metadata: RawMetadata,
        prepare_hash: u64,
        slice: &Option<slice_plan::Slice>,
        key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
    ) -> anyhow::Result<Self> {
        // This is a "hacky" way of adding the 3 key columns. We may just want
        // to manually do that (as part of deprecating `TableSchema`)?
        let prepared_schema = TableSchema::try_from_data_schema(raw_metadata.table_schema.clone())?;
        let prepared_schema = prepared_schema.schema_ref().clone();

        // Add column behaviors for each of the 3 key columns.
        let mut columns = Vec::with_capacity(prepared_schema.fields().len());
        columns.push(ColumnBehavior::try_new_cast(
            &raw_metadata.raw_schema,
            &config.time_column_name,
            &TimestampNanosecondType::DATA_TYPE,
            false,
        )?);
        if let Some(subsort_column_name) = &config.subsort_column_name {
            columns.push(ColumnBehavior::try_new_subsort(
                &raw_metadata.raw_schema,
                subsort_column_name,
            )?);
        } else {
            columns.push(ColumnBehavior::try_default_subsort(prepare_hash)?);
        }

        columns.push(ColumnBehavior::try_new_execute_entity_key(
            &raw_metadata.raw_schema,
            &config.group_column_name,
            false,
        )?);

        // Add column behaviors for each column.  This means we include the key columns
        // redundantly, but cleaning that up is a big refactor.
        // See https://github.com/riptano/kaskada/issues/90
        for field in raw_metadata.table_schema.fields() {
            columns.push(ColumnBehavior::try_cast_or_reference_or_null(
                &raw_metadata.raw_schema,
                field,
            )?);
        }

        // we've already checked that the group column exists so we can just unwrap it here
        let (source_index, field) = raw_metadata
            .raw_schema
            .column_with_name(&config.group_column_name)
            .unwrap();
        let slice_preparer =
            SlicePreparer::try_new(source_index, field.data_type().clone(), slice)?;

        Ok(Self {
            reader: reader.boxed(),
            prepared_schema,
            columns,
            slice_preparer,
            key_hash_inverse,
            table_config: config,
            bounded_lateness: BOUNDED_LATENESS,
            watermark: 0,
            leftovers: None,
        })
    }

    /// Convert a read batch to the merged batch format.
    fn prepare_next_batch(
        &mut self,
        unfiltered_batch: RecordBatch,
    ) -> error_stack::Result<Option<RecordBatch>, Error> {
        // Keep a buffer of values with a bounded disorder window. This simple
        // hueristic allows for us to produce unordered values directly from a stream, dropping
        // "late data" that is outside of the disorder window. The disorder window is
        // configured as the max_event_time minus the max allowed lateness.
        //
        // The pulsar reader _could_ do some of the work of dropping "late" rows,
        // but then the execute_iter would still be responsible for handling the "leftover"
        // batch that needs to be persisted, and I'd rather keep the input buffer
        // responsibilities in a single place.

        // 1. Drop all "late data" from the batch
        // The watermark is the max event time minus the max allowed lateness,
        // carried over from the previous batch.

        // Cast the original time column to a timestampnanosecond array so we can compare
        // the times against the watermark
        let time_column = unfiltered_batch
            .column_by_name(&self.table_config.time_column_name)
            .expect("time column");
        let time_column = arrow::compute::cast(time_column, &TimestampNanosecondType::DATA_TYPE)
            .into_report()
            .change_context(Error::PreparingColumn)?;
        let time_column: &TimestampNanosecondArray = downcast_primitive_array(time_column.as_ref())
            .into_report()
            .change_context(Error::PreparingColumn)?;

        let watermark = if self.watermark != 0 {
            self.watermark
        } else {
            // If there's no watermark yet, initialize it to the first event time - the bounded lateness
            time_column.value(0) - self.bounded_lateness
        };
        let take_indices = time_column
            .iter()
            .enumerate()
            .filter_map(|(index, time)| {
                let time = time.expect("valid time");
                if time >= watermark {
                    self.watermark = std::cmp::max(watermark, time - self.bounded_lateness);
                    Some(index as u64)
                } else {
                    None
                }
            })
            .collect::<Vec<u64>>();
        let take_indices: PrimitiveArray<UInt64Type> =
            PrimitiveArray::from_iter_values(take_indices);

        // Drop all rows with late data from each column
        let filtered_columns = unfiltered_batch
            .columns()
            .iter()
            .map(|column| arrow::compute::take(column, &take_indices, None))
            .try_collect()
            .into_report()
            .change_context(Error::PreparingColumn)?;
        let record_batch =
            RecordBatch::try_new(unfiltered_batch.schema().clone(), filtered_columns)
                .into_report()
                .change_context(Error::PreparingColumn)?;

        // 2. Slicing may reduce the number of entities to operate and sort on.
        let record_batch = self.slice_preparer.slice_batch(record_batch)?;

        // 3. Prepare each of the columns by getting the column behavior result
        let prepared_columns: Vec<_> = self
            .columns
            .iter_mut()
            .map(|column| column.get_result(None, Some(&self.key_hash_inverse), &record_batch))
            .try_collect()?;
        let record_batch = RecordBatch::try_new(self.prepared_schema.clone(), prepared_columns)
            .into_report()
            .change_context(Error::PreparingColumn)?;

        // 4. After preparing the batch, concatenate the leftovers from the previous batch
        // Note this is done after slicing, since the leftovers were already sliced.
        let record_batch = if let Some(leftovers) = self.leftovers.take() {
            arrow::compute::concat_batches(&self.prepared_schema, &[leftovers, record_batch])
                .into_report()
                .change_context(Error::PreparingColumn)?
        } else {
            // The only time there should be no leftovers is the first batch, but we still need to handle this case.
            record_batch
        };

        // 5. Pull out the time, subsort and key hash columns to sort the record batch
        let time_column = record_batch.column(0);
        let subsort_column = record_batch.column(1);
        let key_hash_column = record_batch.column(2);
        let sorted_indices = arrow::compute::lexsort_to_indices(
            &[
                SortColumn {
                    values: time_column.clone(),
                    options: None,
                },
                SortColumn {
                    values: subsort_column.clone(),
                    options: None,
                },
                SortColumn {
                    values: key_hash_column.clone(),
                    options: None,
                },
            ],
            None,
        )
        .into_report()
        .change_context(Error::SortingBatch)?;

        // 6. Produce the fully ordered record batch by taking the indices out from the columns
        let sorted_columns: Vec<_> = record_batch
            .columns()
            .iter()
            .map(|column| arrow::compute::take(column, &sorted_indices, None))
            .try_collect()
            .into_report()
            .change_context(Error::SortingBatch)?;

        let record_batch =
            RecordBatch::try_new(self.prepared_schema.clone(), sorted_columns.clone())
                .into_report()
                .change_context(Error::Internal)?;

        // 7. Store the leftovers for the next batch
        // We cannot produce the entire batch; the watermark cannot advance past the max time in the batch,
        // and  we cannot produce rows past the watermark.
        let time_column: &TimestampNanosecondArray =
            downcast_primitive_array(record_batch.column(0).as_ref())
                .into_report()
                .change_context(Error::PreparingColumn)?;
        let record_batch = if time_column.value(0) >= self.watermark {
            // Add entire batch to leftovers
            self.leftovers = Some(record_batch);
            None
        } else {
            // Split the batch at the watermark
            let times = time_column.values();
            error_stack::ensure!(self.watermark < times[times.len() - 1], Error::Internal);
            let split_at = self.watermark;
            let split_point = match times.binary_search(&split_at) {
                Ok(mut found_index) => {
                    // Just do a linear search for the first value less than split time.
                    while found_index > 0 && times[found_index - 1] == split_at {
                        found_index -= 1
                    }
                    found_index
                }
                Err(not_found_index) => not_found_index,
            };

            // The right split are rows that are greater than or equal to the watermark
            if split_point < record_batch.num_rows() {
                self.leftovers =
                    Some(record_batch.slice(split_point, record_batch.num_rows() - split_point));
            };

            // The left split are the rows that are less than the watermark
            if split_point > 0 {
                Some(record_batch.slice(0, split_point))
            } else {
                None
            }
        };

        Ok(record_batch)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use static_init::dynamic;

    use crate::prepare::PrepareMetadata;

    use super::ColumnBehavior;

    #[dynamic]
    static COMPLETE_SCHEMA: SchemaRef = {
        Arc::new(Schema::new(vec![
            Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("_subsort", DataType::UInt64, false),
            Field::new("_key_hash", DataType::UInt64, false),
            Field::new("a", DataType::Int64, true),
        ]))
    };

    fn make_test_batch(num_rows: usize) -> RecordBatch {
        let time = TimestampNanosecondArray::from_iter_values(0..num_rows as i64);
        let subsort = UInt64Array::from_iter_values(0..num_rows as u64);
        let key = UInt64Array::from_iter_values(0..num_rows as u64);
        let a = Int64Array::from_iter_values(0..num_rows as i64);

        RecordBatch::try_new(
            COMPLETE_SCHEMA.clone(),
            vec![
                Arc::new(time),
                Arc::new(subsort),
                Arc::new(key),
                Arc::new(a),
            ],
        )
        .unwrap()
    }
}
