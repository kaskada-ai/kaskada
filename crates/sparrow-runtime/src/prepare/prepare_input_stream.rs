use std::borrow::BorrowMut;

use anyhow::Context;
use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute::SortColumn;
use arrow::datatypes::{ArrowPrimitiveType, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::BoxStream;
use futures::StreamExt;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::{slice_plan, TableConfig};
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_core::TableSchema;

use crate::prepare::slice_preparer::SlicePreparer;
use crate::prepare::Error;
use crate::RawMetadata;

use super::column_behavior::ColumnBehavior;
use super::PrepareMetadata;

/// Creates a stream over unordered, unprepared batches that is responsible
/// for the following actions:
///
/// 1. Inserting the time column, subsort column, and key hash from source to
///    the batch
/// 2. Casting required columns
/// 3. Sorting the record batches by the time column, subsort column, and key hash
/// 4. Computing the key-hash and key batch metadata.
pub async fn prepare_input<'a>(
    mut reader: BoxStream<'a, error_stack::Result<RecordBatch, Error>>,
    config: &TableConfig,
    raw_metadata: RawMetadata,
    prepare_hash: u64,
    slice: &Option<slice_plan::Slice>,
) -> anyhow::Result<BoxStream<'a, error_stack::Result<(RecordBatch, RecordBatch), Error>>> {
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

    columns.push(ColumnBehavior::try_new_entity_key(
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
    let (entity_column_index, entity_key_column) = raw_metadata
        .raw_schema
        .column_with_name(&config.group_column_name)
        .with_context(|| "")?;
    let slice_preparer = SlicePreparer::try_new(
        entity_column_index,
        entity_key_column.data_type().clone(),
        slice.as_ref(),
    )?;

    let mut metadata = PrepareMetadata::new(entity_key_column.data_type().clone());

    Ok(async_stream::try_stream! {
        while let Some(Ok(batch)) = reader.next().await {
            // 1. Slicing may reduce the number of entities to operate and sort on.
            let read_batch = slice_preparer.slice_batch(batch)?;
            // 2. Prepare each of the columns by getting the column behavior result
            let mut prepared_columns = Vec::new();
            for c in columns.iter_mut() {
                let result = c
                    .get_result(&read_batch)
                    .await?;

                // update_key_metadata(c, &read_batch, &result, &mut metadata)?;
                prepared_columns.push(result);
            }

            // 3. Update the key hash mappings
            let key_column = read_batch.column(entity_column_index);
            let key_hashes = prepared_columns.get(2).expect("key column");
            update_key_metadata(key_column, key_hashes, &mut metadata)?;

            // 4. Pull out the time, subsort and key hash columns to sort the record batch
            let time_column = &prepared_columns[0];
            let subsort_column = &prepared_columns[1];
            let key_hash_column = &prepared_columns[2];
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

            // 5. Produce the fully ordered record batch by taking the indices out from the
            // columns
            let prepared_columns: Vec<_> = prepared_columns
                .iter()
                .map(|column| arrow::compute::take(column.as_ref(), &sorted_indices, None))
                .try_collect()
                .into_report()
                .change_context(Error::Internal)?;

            let batch = RecordBatch::try_new(prepared_schema.clone(), prepared_columns.clone())
                .into_report()
                .change_context(Error::Internal)?;
            let metadata = metadata.get_flush_metadata()?;
            let result = (batch, metadata);
            yield result;
        }
    }
    .boxed())
}

// Update the metadata with the key hash mapping
fn update_key_metadata(
    keys: &ArrayRef,
    key_hashes: &ArrayRef,
    metadata: &mut PrepareMetadata,
) -> error_stack::Result<(), Error> {
    let previous_keys = &mut metadata.previous_keys.borrow_mut();
    let key_hashes: &UInt64Array = downcast_primitive_array(key_hashes.as_ref())
        .into_report()
        .change_context(Error::PreparingColumn)?;
    let indices: UInt64Array = key_hashes
        .into_iter()
        .flatten()
        .enumerate()
        .filter_map(|(index, key_hash)| {
            if previous_keys.insert(key_hash) {
                Some(index as u64)
            } else {
                None
            }
        })
        .collect();
    let new_hash_keys = arrow::compute::take(&key_hashes, &indices, None)
        .into_report()
        .change_context(Error::PreparingColumn)?;
    let new_keys = arrow::compute::take(keys, &indices, None)
        .into_report()
        .change_context(Error::PreparingColumn)?;
    metadata
        .add_entity_keys(new_hash_keys, new_keys)
        .into_report()
        .change_context(Error::PreparingColumn)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::prepare::prepare_input_stream;
    use arrow::{
        array::{StringArray, TimestampNanosecondArray, UInt64Array},
        datatypes::{DataType, Field, Schema, TimeUnit},
    };
    use sparrow_api::kaskada::v1alpha::TableConfig;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_entity_key_mapping() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("id", DataType::Utf8, true),
        ]));
        let raw_metadata = RawMetadata::from_raw_schema(schema.clone()).unwrap();
        let time = TimestampNanosecondArray::from(vec![1, 3, 3, 7, 5, 6, 7]);
        let keys = StringArray::from(vec![
            "awkward", "tacos", "awkward", "tacos", "apples", "tacos", "tacos",
        ]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(time), Arc::new(keys)]).unwrap();
        let reader = futures::stream::iter(vec![Ok(batch)]);
        let config = TableConfig::new_with_table_source(
            "Table1",
            &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
            "time",
            None,
            "id",
            "grouping",
        );
        let stream =
            prepare_input_stream::prepare_input(reader.boxed(), &config, raw_metadata, 0, &None)
                .await
                .unwrap();
        let batches = stream.collect::<Vec<_>>().await;
        assert_eq!(batches.len(), 1);
        let (batch, metadata) = batches[0].as_ref().unwrap();
        let time: &TimestampNanosecondArray =
            downcast_primitive_array(batch.column(0).as_ref()).unwrap();

        assert_eq!(vec![1, 3, 3, 5, 6, 7, 7], time.values().to_vec());
        assert_eq!(metadata.num_rows(), 3);
    }

    #[tokio::test]
    async fn test_entity_key_mapping_int() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("id", DataType::UInt64, true),
        ]));
        let raw_metadata = RawMetadata::from_raw_schema(schema.clone()).unwrap();
        let time = TimestampNanosecondArray::from(vec![1, 3, 3, 7, 5, 6, 7]);
        let keys = UInt64Array::from(vec![1, 2, 3, 1, 2, 3, 4]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(time), Arc::new(keys)]).unwrap();
        let reader = futures::stream::iter(vec![Ok(batch)]);
        let config = TableConfig::new_with_table_source(
            "Table1",
            &Uuid::parse_str("936DA01F9ABD4d9d80C702AF85C822A8").unwrap(),
            "time",
            None,
            "id",
            "grouping",
        );
        let stream =
            prepare_input_stream::prepare_input(reader.boxed(), &config, raw_metadata, 0, &None)
                .await
                .unwrap();
        let batches = stream.collect::<Vec<_>>().await;
        assert_eq!(batches.len(), 1);
        let (batch, metadata) = batches[0].as_ref().unwrap();
        let time: &TimestampNanosecondArray =
            downcast_primitive_array(batch.column(0).as_ref()).unwrap();

        assert_eq!(vec![1, 3, 3, 5, 6, 7, 7], time.values().to_vec());
        assert_eq!(metadata.num_rows(), 4);
    }
}
