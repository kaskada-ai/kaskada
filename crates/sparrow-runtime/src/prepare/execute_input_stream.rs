use std::sync::Arc;

use anyhow::Context;
use arrow::array::{ArrayRef, PrimitiveArray, TimestampNanosecondArray, UInt64Array};
use arrow::compute::SortColumn;
use arrow::datatypes::{ArrowPrimitiveType, SchemaRef, TimestampNanosecondType, UInt64Type};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::BoxStream;
use futures::StreamExt;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::{slice_plan, TableConfig};
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_core::TableSchema;

use crate::key_hash_inverse::ThreadSafeKeyHashInverse;
use crate::prepare::slice_preparer::SlicePreparer;
use crate::prepare::Error;

use super::column_behavior::ColumnBehavior;

/// Struct to store logic for handling late data with a watermark and
/// bounded lateness.
struct InputBuffer {
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

impl InputBuffer {
    fn new() -> Self {
        InputBuffer {
            watermark: 0,
            leftovers: None,
        }
    }
}

/// A stream of input batches ready for processing.
///
/// This stream reads unordered, unprepared batches from its `reader`, and
/// is responsible for the following:
/// * Inserting the time column, subsort column, and key hash from source to
///    the batch
/// * Casting required columns
/// * Dropping all but projected columns
/// * Sorting the record batches by the time column, subsort column, and key hash
/// * Handling late data
#[allow(clippy::too_many_arguments)]
pub async fn prepare_input<'a>(
    mut reader: BoxStream<'a, Result<RecordBatch, ArrowError>>,
    config: Arc<TableConfig>,
    raw_schema: SchemaRef,
    projected_schema: SchemaRef,
    prepare_hash: u64,
    slice: Option<&slice_plan::Slice>,
    key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
    bounded_lateness: i64,
) -> anyhow::Result<BoxStream<'a, error_stack::Result<Option<RecordBatch>, Error>>> {
    // This is a "hacky" way of adding the 3 key columns. We may just want
    // to manually do that (as part of deprecating `TableSchema`)?
    let prepared_schema = TableSchema::try_from_data_schema(projected_schema.clone())?;
    let prepared_schema = prepared_schema.schema_ref().clone();

    // Add column behaviors for each of the 3 key columns.
    let mut columns = Vec::with_capacity(prepared_schema.fields().len());
    columns.push(ColumnBehavior::try_new_cast(
        &raw_schema,
        &config.time_column_name,
        &TimestampNanosecondType::DATA_TYPE,
        false,
    )?);
    if let Some(subsort_column_name) = &config.subsort_column_name {
        columns.push(ColumnBehavior::try_new_subsort(
            &raw_schema,
            subsort_column_name,
        )?);
    } else {
        columns.push(ColumnBehavior::try_default_subsort(prepare_hash)?);
    }

    columns.push(ColumnBehavior::try_new_entity_key(
        &raw_schema,
        &config.group_column_name,
        false,
    )?);

    // Add column behaviors for each column.  This means we include the key columns
    // redundantly, but cleaning that up is a big refactor.
    // See https://github.com/riptano/kaskada/issues/90
    for field in projected_schema.fields() {
        columns.push(ColumnBehavior::try_cast_or_reference_or_null(
            &raw_schema,
            field,
        )?);
    }

    // we've already checked that the group column exists so we can just unwrap it here
    let (entity_column_index, entity_key_column) = raw_schema
        .column_with_name(&config.group_column_name)
        .with_context(|| "")?;
    let slice_preparer = SlicePreparer::try_new(
        entity_column_index,
        entity_key_column.data_type().clone(),
        slice,
    )?;

    Ok(async_stream::try_stream! {
        let mut input_buffer = InputBuffer::new();
        while let Some(unfiltered_batch) = reader.next().await {
            let unfiltered_batch = unfiltered_batch.into_report().change_context(Error::PreparingColumn)?;
            let unfiltered_rows = unfiltered_batch.num_rows();

            // Keep a buffer of values with a bounded disorder window. This simple
            // hueristic allows for us to process unordered values directly from a stream, dropping
            // "late data" that is outside of the disorder window. The watermark is
            // configured as the max_event_time minus the bounded_lateness.
            //
            // There is an opportunity to refactor this step such that we can share the
            // column preparation logic between prepare and execute. We have three main operations:
            //
            // 1. The underlying reader reads from parquet/streams and assembles batches.
            // 2. Special late data handling.
            // 3. The column preparation logic for adding key columns, ordering, casting, etc.
            //
            // Currently, we're handling late data inside of the preparation logic, but we
            // could wrap the underlying reader with "late data handling" before this step.

            // 1. Drop all "late data" from the batch
            let time_column = unfiltered_batch
                .column_by_name(&config.time_column_name)
                .expect("time column");
            let time_column = arrow::compute::cast(time_column, &TimestampNanosecondType::DATA_TYPE)
                .into_report()
                .change_context(Error::PreparingColumn)?;
            let time_column: &TimestampNanosecondArray = downcast_primitive_array(time_column.as_ref())
                .into_report()
                .change_context(Error::PreparingColumn)?;

            if input_buffer.watermark <= 0 {
                // Initial watermark is the first event time - bounded lateness (or 0).
                input_buffer.watermark = std::cmp::max(0, time_column.value(0) - bounded_lateness);
            };

            // Find which indices to take from the batch (the non-late data)
            let take_indices = time_column
                .iter()
                .enumerate()
                .filter_map(|(index, time)| {
                    let time = time.expect("valid time");
                    if time > input_buffer.watermark {
                        input_buffer.watermark = std::cmp::max(input_buffer.watermark, time - bounded_lateness);
                        Some(index as u64)
                    } else {
                        // Late data - drop it
                        None
                    }
                })
                .collect::<Vec<u64>>();
            let take_indices: PrimitiveArray<UInt64Type> = PrimitiveArray::from_iter_values(take_indices);

            tracing::debug!("Watermark: {:?}", input_buffer.watermark);

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
            tracing::debug!("Dropped {} late messages", unfiltered_rows - record_batch.num_rows());

            // 2. Slicing may reduce the number of entities to operate and sort on.
            let record_batch = slice_preparer.slice_batch(record_batch)?;

            // 3. Prepare each of the columns by getting the column behavior result
            let mut prepared_columns: Vec<ArrayRef> = Vec::new();
            for c in columns.iter_mut() {
                let result = c.get_result(&record_batch).await?;
                prepared_columns.push(result);
            }

            // 4. Update the key hash mappings
            let key_column = record_batch.column(entity_column_index);
            let key_hashes = prepared_columns.get(2).expect("key column");
            update_key_inverse(key_column, key_hashes, key_hash_inverse.clone()).await?;

            let record_batch = RecordBatch::try_new(prepared_schema.clone(), prepared_columns)
                .into_report()
                .change_context(Error::PreparingColumn)?;

            // 5. After preparing the batch, concatenate the leftovers from the previous batch
            // Note this is done after slicing, since the leftovers were already sliced.
            let record_batch = if let Some(leftovers) = input_buffer.leftovers.take() {
                debug_assert!(input_buffer.leftovers.is_none());
                arrow::compute::concat_batches(&prepared_schema, &[leftovers, record_batch])
                    .into_report()
                    .change_context(Error::PreparingColumn)?
            } else {
                // The only time there should be no leftovers is the first batch, but we still need to handle this case.
                record_batch
            };

            // 6. Pull out the time, subsort, and key hash columns to sort the record batch
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

            // 7. Produce the fully ordered record batch by taking the indices out from the columns
            let sorted_columns: Vec<_> = record_batch
                .columns()
                .iter()
                .map(|column| arrow::compute::take(column, &sorted_indices, None))
                .try_collect()
                .into_report()
                .change_context(Error::SortingBatch)?;

            let record_batch = RecordBatch::try_new(prepared_schema.clone(), sorted_columns.clone())
                .into_report()
                .change_context(Error::Internal)?;

            // 8. Store the leftovers for the next batch
            // We cannot produce the entire batch; the watermark cannot advance past the max time in the batch,
            // and we cannot produce rows past the watermark.
            let time_column: &TimestampNanosecondArray =
                downcast_primitive_array(record_batch.column(0).as_ref())
                    .into_report()
                    .change_context(Error::PreparingColumn)?;
            let record_batch = if time_column.value(0) >= input_buffer.watermark {
                // Add entire batch to leftovers
                input_buffer.leftovers = Some(record_batch);
                None
            } else {
                // Split the batch at the watermark
                let times = time_column.values();
                debug_assert!(input_buffer.watermark < times[times.len() - 1]);

                let split_at = input_buffer.watermark;
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
                    input_buffer.leftovers =
                        Some(record_batch.slice(split_point, record_batch.num_rows() - split_point));
                };

                // The left split are the rows that are less than the watermark
                if split_point > 0 {
                    Some(record_batch.slice(0, split_point))
                } else {
                    None
                }
            };
            yield record_batch
        }

        tracing::error!("unexpected - loop has exited");
    }
    .boxed())
}

async fn update_key_inverse(
    keys: &ArrayRef,
    key_hashes: &ArrayRef,
    key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
) -> error_stack::Result<(), Error> {
    let key_hashes: &UInt64Array = downcast_primitive_array(key_hashes.as_ref())
        .into_report()
        .change_context(Error::PreparingColumn)?;
    key_hash_inverse
        .add(keys.as_ref(), key_hashes)
        .await
        .change_context(Error::PreparingColumn)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{Int64Array, TimestampNanosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use sparrow_api::kaskada::v1alpha::TableConfig;
    use sparrow_arrow::downcast::downcast_primitive_array;
    use static_init::dynamic;
    use uuid::Uuid;

    use crate::key_hash_inverse::ThreadSafeKeyHashInverse;
    use crate::prepare::execute_input_stream;
    use crate::RawMetadata;

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
        let config = Arc::new(TableConfig::new_with_table_source(
            "Table1",
            &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
            "time",
            Some("subsort"),
            "key",
            "",
        ));
        let batch1 = make_time_batch(&[3, 1, 10, 4, 7]);
        let batch2 = make_time_batch(&[6, 12, 10, 17, 11, 12]);
        let batch3 = make_time_batch(&[20]);
        let reader = futures::stream::iter(vec![Ok(batch1), Ok(batch2), Ok(batch3)]).boxed();
        let key_hash_inverse =
            Arc::new(ThreadSafeKeyHashInverse::from_data_type(&DataType::UInt64));

        let raw_metadata = RawMetadata::from_raw_schema(RAW_SCHEMA.clone()).unwrap();
        let mut stream = execute_input_stream::prepare_input(
            reader.boxed(),
            config,
            raw_metadata.raw_schema.clone(),
            raw_metadata.table_schema.clone(),
            0,
            None,
            key_hash_inverse.clone(),
            5,
        )
        .await
        .unwrap();

        let prepared1 = stream.next().await.unwrap().unwrap().unwrap();
        let prepared2 = stream.next().await.unwrap().unwrap().unwrap();
        let prepared3 = stream.next().await.unwrap().unwrap().unwrap();

        let times1: &TimestampNanosecondArray =
            downcast_primitive_array(prepared1.column(0).as_ref()).unwrap();
        assert_eq!(&[1, 3], times1.values());

        let times2: &TimestampNanosecondArray =
            downcast_primitive_array(prepared2.column(0).as_ref()).unwrap();
        assert_eq!(&[6, 7, 10, 10], times2.values());

        let times3: &TimestampNanosecondArray =
            downcast_primitive_array(prepared3.column(0).as_ref()).unwrap();
        assert_eq!(&[12], times3.values())
    }

    #[tokio::test]
    async fn test_when_watermark_does_not_advance() {
        let config = Arc::new(TableConfig::new_with_table_source(
            "Table1",
            &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
            "time",
            Some("subsort"),
            "key",
            "",
        ));
        let batch1 = make_time_batch(&[1, 3, 10]);
        let batch2 = make_time_batch(&[6, 7, 8, 9, 10]);
        let batch3 = make_time_batch(&[20]);

        let reader = futures::stream::iter(vec![Ok(batch1), Ok(batch2), Ok(batch3)]).boxed();
        let key_hash_inverse =
            Arc::new(ThreadSafeKeyHashInverse::from_data_type(&DataType::UInt64));

        let raw_metadata = RawMetadata::from_raw_schema(RAW_SCHEMA.clone()).unwrap();
        let mut stream = execute_input_stream::prepare_input(
            reader.boxed(),
            config,
            raw_metadata.raw_schema.clone(),
            raw_metadata.table_schema.clone(),
            0,
            None,
            key_hash_inverse.clone(),
            5,
        )
        .await
        .unwrap();

        let prepared1 = stream.next().await.unwrap().unwrap().unwrap();
        let prepared2 = stream.next().await.unwrap().unwrap();
        let prepared3 = stream.next().await.unwrap().unwrap().unwrap();

        // first batch
        let times1: &TimestampNanosecondArray =
            downcast_primitive_array(prepared1.column(0).as_ref()).unwrap();
        assert_eq!(&[1, 3], times1.values());

        // watermark did not advance, so cannot produce any
        assert!(prepared2.is_none());

        // watermark advances, so produce the leftovers that are before the new watermark
        let times3: &TimestampNanosecondArray =
            downcast_primitive_array(prepared3.column(0).as_ref()).unwrap();
        assert_eq!(&[6, 7, 8, 9, 10, 10], times3.values())
    }

    #[tokio::test]
    async fn test_when_first_batch_has_single_row() {
        let config = Arc::new(TableConfig::new_with_table_source(
            "Table1",
            &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
            "time",
            Some("subsort"),
            "key",
            "",
        ));
        let batch1 = make_time_batch(&[10]);
        let batch2 = make_time_batch(&[4]);
        let batch3 = make_time_batch(&[7, 17]);

        let reader = futures::stream::iter(vec![Ok(batch1), Ok(batch2), Ok(batch3)]).boxed();
        let key_hash_inverse =
            Arc::new(ThreadSafeKeyHashInverse::from_data_type(&DataType::UInt64));

        let raw_metadata = RawMetadata::from_raw_schema(RAW_SCHEMA.clone()).unwrap();
        let mut stream = execute_input_stream::prepare_input(
            reader.boxed(),
            config,
            raw_metadata.raw_schema.clone(),
            raw_metadata.table_schema.clone(),
            0,
            None,
            key_hash_inverse.clone(),
            5,
        )
        .await
        .unwrap();

        let prepared1 = stream.next().await.unwrap().unwrap();
        let prepared2 = stream.next().await.unwrap().unwrap();
        let prepared3 = stream.next().await.unwrap().unwrap().unwrap();

        // Can't produce row due to watermark not advancing past the row
        assert!(prepared1.is_none());
        // contained late data, so nothing to produce
        assert!(prepared2.is_none());

        // watermark advances, so produce the leftovers that are before the new watermark
        let times3: &TimestampNanosecondArray =
            downcast_primitive_array(prepared3.column(0).as_ref()).unwrap();
        assert_eq!(&[7, 10], times3.values())
    }
}
