use anyhow::Context;
use arrow::compute::SortColumn;
use arrow::datatypes::{ArrowPrimitiveType, TimestampNanosecondType};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, ResultExt};
use futures::stream::BoxStream;
use futures::StreamExt;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::{slice_plan, TableConfig};
use sparrow_core::TableSchema;

use crate::prepare::slice_preparer::SlicePreparer;
use crate::prepare::Error;
use crate::RawMetadata;

use super::column_behavior::ColumnBehavior;
use super::PrepareMetadata;

/// An stream over prepare batches and corresponding key hash metadata.
///
/// This stream reads unordered, unprepared batches from the `reader`, and is
/// responsible for:
/// 1. Inserting the time column, subsort column, and key hash from source to
///    the batch
/// 2. Casting required columns
/// 3. Sorting the record batches by the time column, subsort column, and key hash
/// 4. Computing the key-hash and key batch metadata.
pub async fn try_new<'a>(
    mut reader: BoxStream<'a, Result<RecordBatch, ArrowError>>,
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

    columns.push(ColumnBehavior::try_new_prepare_entity_key(
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
        SlicePreparer::try_new(source_index, field.data_type().clone(), slice.as_ref())?;

    let (_, entity_key_column) = raw_metadata
        .raw_schema
        .column_with_name(&config.group_column_name)
        .with_context(|| "")?;

    let mut metadata = PrepareMetadata::new(entity_key_column.data_type().clone());

    Ok(async_stream::try_stream! {
        while let Some(Ok(batch)) = reader.next().await {
            // 1. Slicing may reduce the number of entities to operate and sort on.
            let read_batch = slice_preparer.slice_batch(batch)?;
            // 2. Prepare each of the columns by getting the column behavior result
            let mut prepared_columns = Vec::new();
            for c in columns.iter_mut() {
                let result = c
                    .get_result(Some(&mut metadata), None, &read_batch)
                    .await?;
                prepared_columns.push(result);
            }

            // 3. Pull out the time, subsort and key hash columns to sort the record batch
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

            // Produce the fully ordered record batch by taking the indices out from the
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
