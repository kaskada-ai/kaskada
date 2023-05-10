use std::sync::Arc;

use arrow::array::{ArrayRef, PrimitiveArray, TimestampNanosecondArray};
use arrow::compute::SortColumn;
use arrow::datatypes::{ArrowPrimitiveType, SchemaRef, TimestampNanosecondType, UInt64Type};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
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
///
/// This is hard-coded for now, but could easily be made configurable as a parameter
/// to the table.
#[allow(unused)]
const BOUNDED_LATENESS_NS: i64 = 1_000_000_000;

/// An iterator over batches during execution.
///
/// In addition to iterating, this is responsible for the following:
///
/// 1. Inserting the time column, subsort column, and key hash from source to
///    the batch
/// 2. Casts required columns
/// 3. Sorts the record batches by the time column, subsort column, and key hash
/// 4. Handles late data
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
    /// This simple hueristic is a good start, but we can improve on this
    /// by statistically modeling event behavior and adapting the watermark accordingly.
    bounded_lateness: i64,
    /// The watermark represents a timestamp beyond which the system assumes
    /// that all data with earlier timestamps has been produced. No data past
    /// the watermark can be processed.
    watermark: i64,
    /// The leftovers are events that have been read from the stream but have not
    /// been produced, as the watermark has not advanced far enough.
    ///
    /// Note this implies that if a period of inactivity in the stream occurs, we
    /// cannot produce the last n events until the watermark advances.
    leftovers: Option<RecordBatch>,
}

// impl<'a> Stream for ExecuteIter<'a> {
//     /// This stream may return None in the case where messages were read from
//     /// the underlying stream, but the watermark has not advanced far enough to
//     /// produce any messages.
//     ///
//     /// The correct behavior for a consumer is to ignore the empty message
//     /// and continue polling the stream.
//     type Item = Result<Option<RecordBatch>, ExecuteIterWrapper>;

//     fn poll_next(
//         mut self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> Poll<Option<Self::Item>> {
//         let reader = &mut self.reader;
//         Pin::new(reader).poll_next(cx).map(|opt| match opt {
//             Some(Ok(batch)) => {
//                 let result = self
//                     .get_mut()
//                     .prepare_next_batch(batch)
//                     .map_err(|err| err.change_context(Error::ReadingBatch).into());
//                 Some(result)
//             }
//             Some(Err(err)) => Some(Err(Report::new(err)
//                 .change_context(Error::ReadingBatch)
//                 .into())),
//             None => {
//                 // Indicates that the stream has likely closed unexpectedly
//                 tracing::error!(
//                     "Unexpected empty message from stream - likely the stream has closed"
//                 );
//                 Some(Err(Report::new(Error::ReadInput)
//                     .change_context(Error::Internal)
//                     .into()))
//             }
//         })
//     }
// }

impl<'a> std::fmt::Debug for ExecuteIter<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecuteIter")
            .field("prepared_schema", &self.prepared_schema)
            .field("columns", &self.columns)
            // TODO: FRAZ add new fields
            .finish_non_exhaustive()
    }
}

impl<'a> ExecuteIter<'a> {
    #[allow(unused)]
    pub fn try_new(
        reader: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send + 'static,
        config: &'a TableConfig,
        raw_metadata: RawMetadata,
        prepare_hash: u64,
        slice: Option<&slice_plan::Slice>,
        key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
        bounded_lateness: i64,
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
            .expect("group column");
        let slice_preparer =
            SlicePreparer::try_new(source_index, field.data_type().clone(), slice)?;

        Ok(Self {
            reader: reader.boxed(),
            prepared_schema,
            columns,
            slice_preparer,
            key_hash_inverse,
            table_config: config,
            bounded_lateness,
            watermark: 0,
            leftovers: None,
        })
    }

    /// Creates a stream of prepared batches from the underlying reader.
    #[allow(unused)]
    pub fn stream(&mut self) -> BoxStream<'_, error_stack::Result<Option<RecordBatch>, Error>> {
        async_stream::try_stream! {
            while let Some(Ok(batch)) = self.reader.next().await {
                let result = self.prepare_next_batch(batch).await?;
                yield result;
            }
        }
        .boxed()
    }

    /// Convert a read batch to the merged batch format.
    ///
    /// This method drops "late data" from each batch using a simplistic
    /// watermark heuristic.
    #[allow(unused)]
    async fn prepare_next_batch(
        &mut self,
        unfiltered_batch: RecordBatch,
    ) -> error_stack::Result<Option<RecordBatch>, Error> {
        error_stack::ensure!(unfiltered_batch.num_rows() > 0, Error::Internal);
        // Keep a buffer of values with a bounded disorder window. This simple
        // hueristic allows for us to process unordered values directly from a stream, dropping
        // "late data" that is outside of the disorder window. The watermark is
        // configured as the max_event_time minus the bounded_lateness.
        //
        // The stream reader _could_ do some of the work of dropping "late" rows,
        // but then the execute_iter would still be responsible for handling the "leftover"
        // batch that needs to be persisted, and I'd rather keep the input buffer
        // responsibilities in a single place.

        // 1. Drop all "late data" from the batch
        let time_column = unfiltered_batch
            .column_by_name(&self.table_config.time_column_name)
            .expect("time column");
        let time_column = arrow::compute::cast(time_column, &TimestampNanosecondType::DATA_TYPE)
            .into_report()
            .change_context(Error::PreparingColumn)?;
        let time_column: &TimestampNanosecondArray = downcast_primitive_array(time_column.as_ref())
            .into_report()
            .change_context(Error::PreparingColumn)?;

        if self.watermark < 0 {
            debug_assert!(
                time_column.value(0) - self.bounded_lateness > 0,
                "invalid time; below 0",
            );
            // If there's no watermark yet, initialize it to the first event time - the bounded lateness
            self.watermark = time_column.value(0) - self.bounded_lateness
        };

        // Find which indices to take from the batch (the non-late data)
        let take_indices = time_column
            .iter()
            .enumerate()
            .filter_map(|(index, time)| {
                let time = time.expect("valid time");
                if time >= self.watermark {
                    self.watermark = std::cmp::max(self.watermark, time - self.bounded_lateness);
                    Some(index as u64)
                } else {
                    // Late data - drop it
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
        let record_batch = RecordBatch::try_new(unfiltered_batch.schema(), filtered_columns)
            .into_report()
            .change_context(Error::PreparingColumn)?;

        // 2. Slicing may reduce the number of entities to operate and sort on.
        let record_batch = self.slice_preparer.slice_batch(record_batch)?;

        // 3. Prepare each of the columns by getting the column behavior result
        // let prepared_columns: Vec<ArrayRef> = self
        //     .columns
        //     .iter_mut()
        //     .map(|column| column.get_result(None, Some(&self.key_hash_inverse), &record_batch))
        //     .try_collect()?;

        let mut prepared_columns: Vec<ArrayRef> = Vec::new();
        for c in self.columns.iter_mut() {
            let batch = c
                .get_result(None, Some(&self.key_hash_inverse), &record_batch)
                .await?;
            prepared_columns.push(batch);
        }

        let record_batch = RecordBatch::try_new(self.prepared_schema.clone(), prepared_columns)
            .into_report()
            .change_context(Error::PreparingColumn)?;

        // 4. After preparing the batch, concatenate the leftovers from the previous batch
        // Note this is done after slicing, since the leftovers were already sliced.
        let record_batch = if let Some(leftovers) = self.leftovers.take() {
            debug_assert!(self.leftovers.is_none());
            arrow::compute::concat_batches(&self.prepared_schema, &[leftovers, record_batch])
                .into_report()
                .change_context(Error::PreparingColumn)?
        } else {
            // The only time there should be no leftovers is the first batch, but we still need to handle this case.
            record_batch
        };

        // 5. Pull out the time, subsort, and key hash columns to sort the record batch
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
        // and we cannot produce rows past the watermark.
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

    use arrow::array::{Int64Array, TimestampNanosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use sparrow_api::kaskada::v1alpha::TableConfig;
    use sparrow_core::downcast_primitive_array;
    use static_init::dynamic;
    use uuid::Uuid;

    use crate::execute::key_hash_inverse::{KeyHashInverse, ThreadSafeKeyHashInverse};
    use crate::RawMetadata;

    use super::ExecuteIter;

    #[dynamic]
    static RAW_SCHEMA: SchemaRef = {
        Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("subsort", DataType::UInt64, false),
            Field::new("key", DataType::UInt64, false),
            Field::new("a", DataType::Int64, true),
        ]))
    };

    fn default_iter<'a>(config: &'a TableConfig, bounded_lateness: i64) -> ExecuteIter<'a> {
        let reader = Box::pin(futures::stream::iter(vec![]));
        let raw_schema = RAW_SCHEMA.clone();
        let raw_metadata = RawMetadata::from_raw_schema(raw_schema.clone());
        let key_hash_inverse = Arc::new(ThreadSafeKeyHashInverse::new(
            KeyHashInverse::from_data_type(DataType::UInt64),
        ));
        ExecuteIter::try_new(
            reader,
            config,
            raw_metadata,
            0,
            None,
            key_hash_inverse,
            bounded_lateness,
        )
        .unwrap()
    }

    fn make_time_batch(times: &[i64]) -> RecordBatch {
        let time = TimestampNanosecondArray::from_iter_values(times.iter().copied());
        let subsort = UInt64Array::from_iter_values(0..times.len() as u64);
        let key = UInt64Array::from_iter_values(std::iter::repeat(0).take(times.len()));
        let a = Int64Array::from_iter_values(0..times.len() as i64);

        RecordBatch::try_new(
            RAW_SCHEMA.clone(),
            vec![
                Arc::new(time),
                Arc::new(subsort),
                Arc::new(key),
                Arc::new(a),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_sequence_of_batches_prepared() {
        let config = TableConfig::new(
            "Table1",
            &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
            "time",
            Some("subsort"),
            "key",
            "",
        );
        let mut execute_iter = default_iter(&config, 5);
        let batch1 = make_time_batch(&[0, 3, 1, 10, 4, 7]);
        let batch2 = make_time_batch(&[6, 12, 10, 17, 11, 12]);
        let batch3 = make_time_batch(&[20]);

        let prepared1 = execute_iter
            .prepare_next_batch(batch1)
            .await
            .unwrap()
            .unwrap();
        let prepared2 = execute_iter
            .prepare_next_batch(batch2)
            .await
            .unwrap()
            .unwrap();
        let prepared3 = execute_iter
            .prepare_next_batch(batch3)
            .await
            .unwrap()
            .unwrap();

        let times1: &TimestampNanosecondArray =
            downcast_primitive_array(prepared1.column(0).as_ref()).unwrap();
        assert_eq!(&[0, 1, 3], times1.values());

        let times2: &TimestampNanosecondArray =
            downcast_primitive_array(prepared2.column(0).as_ref()).unwrap();
        assert_eq!(&[6, 7, 10, 10], times2.values());

        let times3: &TimestampNanosecondArray =
            downcast_primitive_array(prepared3.column(0).as_ref()).unwrap();
        assert_eq!(&[12, 12], times3.values())
    }

    #[tokio::test]
    async fn test_when_watermark_does_not_advance() {
        let config = TableConfig::new(
            "Table1",
            &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
            "time",
            Some("subsort"),
            "key",
            "",
        );
        let mut execute_iter = default_iter(&config, 5);
        let batch1 = make_time_batch(&[0, 1, 3, 10]);
        let batch2 = make_time_batch(&[6, 7, 8, 9, 10]);
        let batch3 = make_time_batch(&[20]);

        let prepared1 = execute_iter
            .prepare_next_batch(batch1)
            .await
            .unwrap()
            .unwrap();

        let times1: &TimestampNanosecondArray =
            downcast_primitive_array(prepared1.column(0).as_ref()).unwrap();
        assert_eq!(&[0, 1, 3], times1.values());

        let prepared2 = execute_iter.prepare_next_batch(batch2).await.unwrap();
        assert!(prepared2.is_none());

        let prepared3 = execute_iter
            .prepare_next_batch(batch3)
            .await
            .unwrap()
            .unwrap();
        let times3: &TimestampNanosecondArray =
            downcast_primitive_array(prepared3.column(0).as_ref()).unwrap();
        assert_eq!(&[6, 7, 8, 9, 10, 10], times3.values())
    }
}
