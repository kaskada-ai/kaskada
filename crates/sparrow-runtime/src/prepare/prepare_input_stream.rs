use std::borrow::BorrowMut;
use std::sync::atomic::AtomicU64;

use anyhow::Context;
use arrow::array::{ArrayRef, UInt64Array};
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::BoxStream;
use futures::StreamExt;
use sparrow_api::kaskada::v1alpha::{slice_plan, TableConfig};
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_core::TableSchema;

use crate::prepare::slice_preparer::SlicePreparer;
use crate::prepare::Error;
use crate::preparer::prepare_batch;
use crate::RawMetadata;

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
    config: &'a TableConfig,
    raw_metadata: RawMetadata,
    prepare_hash: u64,
    slice: &Option<slice_plan::Slice>,
    time_multiplier: Option<arrow_array::Scalar<ArrayRef>>,
) -> anyhow::Result<BoxStream<'a, error_stack::Result<(RecordBatch, RecordBatch), Error>>> {
    // This is a "hacky" way of adding the 3 key columns. We may just want
    // to manually do that (as part of deprecating `TableSchema`)?
    let prepared_schema = TableSchema::try_from_data_schema(raw_metadata.table_schema.clone())?;
    let prepared_schema = prepared_schema.schema_ref().clone();

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
    let next_subsort = AtomicU64::new(prepare_hash);

    Ok(async_stream::try_stream! {
        while let Some(Ok(batch)) = reader.next().await {
            // 1. Slicing may reduce the number of entities to operate and sort on.
            let read_batch = slice_preparer.slice_batch(batch)?;

            // 2. Prepare the batch
            let prepared_batch = prepare_batch(&read_batch, config, prepared_schema.clone(), &next_subsort, time_multiplier.as_ref()).unwrap();

            // 3. Update the key inverse
            let key_hash_column = prepared_batch.column(2);
            let key_column = prepared_batch.column(entity_column_index + 3);
            update_key_metadata(key_column, key_hash_column, &mut metadata)?;

            // 4. Produce the batch and associated metadata
            let metadata = metadata.get_flush_metadata()?;
            let result = (prepared_batch, metadata);
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
        let stream = prepare_input_stream::prepare_input(
            reader.boxed(),
            &config,
            raw_metadata,
            0,
            &None,
            None,
        )
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
        let stream = prepare_input_stream::prepare_input(
            reader.boxed(),
            &config,
            raw_metadata,
            0,
            &None,
            None,
        )
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
