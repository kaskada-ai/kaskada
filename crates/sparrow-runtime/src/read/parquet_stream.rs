use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::BoxStream;
use futures::StreamExt;
use hashbrown::HashSet;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use sparrow_arrow::attachments::{RecordBatchAttachment, SchemaAttachment};
use sparrow_core::{KeyTriple, TableSchema};

use crate::data_manager::DataHandle;
use crate::{validate_batch_schema, Batch};

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to resolve data handle to path(s)")]
    ResolveDataHandle,
    #[display(fmt = "failed to open parquet file")]
    OpenParquetFile,
    #[display(fmt = "failed to determine columns to read")]
    DetermineColumns,
    #[display(
        fmt = "data appeared out of order (prev last row = {prev_last}, curr first row = {curr_first})"
    )]
    DataOutOfOrder {
        curr_first: KeyTriple,
        prev_last: KeyTriple,
    },
    #[display(fmt = "failed to read batch from parquet file")]
    ReadingBatch,
}

impl error_stack::Context for Error {}

pub(super) async fn new_parquet_stream(
    data_handle: &DataHandle,
    projected_schema: &TableSchema,
) -> error_stack::Result<BoxStream<'static, error_stack::Result<Batch, Error>>, Error> {
    let path = data_handle
        .get_path()
        .await
        .into_report()
        .change_context(Error::ResolveDataHandle)?;

    let file = tokio::fs::File::open(path)
        .await
        .into_report()
        .change_context(Error::OpenParquetFile)?;
    let builder = ParquetRecordBatchStreamBuilder::new(file)
        .await
        .into_report()
        .change_context(Error::OpenParquetFile)?;

    let file_schema = builder.schema();
    let reader_columns = get_columns_to_read(file_schema.as_ref(), projected_schema)
        .into_report()
        .change_context(Error::DetermineColumns)?;

    let mask = ProjectionMask::leaves(builder.parquet_schema(), reader_columns);
    let projected_reader = builder
        .with_batch_size(BATCH_SIZE)
        .with_projection(mask)
        .build()
        .into_report()
        .change_context(Error::OpenParquetFile)?;

    let projected_schema = projected_schema.schema_ref().clone();

    let mut max_element_seen = KeyTriple {
        time: 0,
        subsort: 0,
        key_hash: 0,
    };
    let stream = projected_reader.map(move |item| {
        let raw_batch = item.into_report().change_context(Error::ReadingBatch)?;

        // Recreate the RecordBatch, if needed, to adjust nullability of columns.
        // The schemas should be identical / compatible, but may have different
        // nullability.
        let batch = if raw_batch.schema() == projected_schema {
            raw_batch
        } else {
            RecordBatch::try_new(projected_schema.clone(), raw_batch.columns().to_vec())
                .into_report()
                .change_context(Error::ReadingBatch)
                .attach_printable_lazy(|| RecordBatchAttachment::new("raw_batch", &raw_batch))
                .attach_printable_lazy(|| {
                    SchemaAttachment::new("projected_schema", &projected_schema)
                })?
        };

        let batch = Batch::try_new_from_batch(batch)
            .into_report()
            .change_context(Error::ReadingBatch)?;

        // Make sure the batch appears in order within the file.
        error_stack::ensure!(
            batch.lower_bound >= max_element_seen,
            Error::DataOutOfOrder {
                curr_first: batch.lower_bound,
                prev_last: max_element_seen,
            }
        );
        max_element_seen = batch.upper_bound;
        Ok(batch)
    });

    Ok(stream.boxed())
}

/// Determine needed indices given a file schema and projected schema.
fn get_columns_to_read(
    file_schema: &Schema,
    projected_schema: &TableSchema,
) -> anyhow::Result<Vec<usize>> {
    let needed_columns: HashSet<_> = projected_schema
        .data_fields()
        .iter()
        .map(|field| field.name())
        .collect();

    let mut columns = Vec::with_capacity(3 + needed_columns.len());

    // The first 3 columns of the file should be the key columns
    // `_time`, `_subsort` and `_key_hash`, and we need to read them.
    validate_batch_schema(file_schema)?;
    columns.extend_from_slice(&[0, 1, 2]);

    for (index, column) in file_schema.fields().iter().enumerate().skip(3) {
        if needed_columns.contains(column.name()) {
            columns.push(index)
        }
    }

    Ok(columns)
}

const BATCH_SIZE: usize = 100_000;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;
    use sparrow_api::kaskada::v1alpha::PreparedFile;
    use static_init::dynamic;

    use super::*;
    use crate::data_manager::DataHandle;
    use crate::read::testing::write_parquet_file;
    use crate::s3::S3Helper;

    #[tokio::test]
    async fn test_parquet_file_source() {
        sparrow_testing::init_test_logging();

        // Create a Parquet file
        let file = write_parquet_file(&COMPLETE_BATCH, None);
        let metadata = write_parquet_file(&METADATA_BATCH, None);

        let prepared_file = PreparedFile {
            num_rows: 3,
            path: file.to_string_lossy().into_owned(),
            min_event_time: Some(prost_wkt_types::Timestamp {
                seconds: 0,
                nanos: 5,
            }),
            max_event_time: Some(prost_wkt_types::Timestamp {
                seconds: 0,
                nanos: 15,
            }),
            metadata_path: metadata.to_string_lossy().into_owned(),
        };
        let data_handle = DataHandle::try_new(S3Helper::new().await, &prepared_file).unwrap();

        // Test reading the file
        check_complete(&data_handle, &COMPLETE_BATCH).await;
        check_projected(&data_handle, &PROJECTED_BATCH).await;
    }

    async fn check_complete(data_handle: &DataHandle, expected: &RecordBatch) {
        let table_schema = TableSchema::from_sparrow_schema(expected.schema()).unwrap();
        let mut reader = new_parquet_stream(data_handle, &table_schema)
            .await
            .unwrap();

        assert_eq!(reader.try_next().await.unwrap().unwrap().data(), expected);
        assert!(reader.try_next().await.unwrap().is_none());
    }

    async fn check_projected(data_handle: &DataHandle, expected: &RecordBatch) {
        let table_schema = TableSchema::from_sparrow_schema(expected.schema()).unwrap();
        let mut reader = new_parquet_stream(data_handle, &table_schema)
            .await
            .unwrap();

        assert_eq!(reader.try_next().await.unwrap().unwrap().data(), expected);
        assert!(reader.try_next().await.unwrap().is_none());
    }

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
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]))
    };

    #[dynamic]
    static PROJECTED_SCHEMA: SchemaRef = {
        Arc::new(Schema::new(vec![
            Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("_subsort", DataType::UInt64, false),
            Field::new("_key_hash", DataType::UInt64, false),
            Field::new("a", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]))
    };

    #[dynamic]
    static METADATA_SCHEMA: SchemaRef = {
        Arc::new(Schema::new(vec![
            Field::new("_key_hash", DataType::UInt64, false),
            Field::new("_entity_key", DataType::Utf8, false),
        ]))
    };

    #[dynamic]
    static COMPLETE_BATCH: RecordBatch = {
        let time = TimestampNanosecondArray::from(vec![5, 10, 15]);
        let subsort = UInt64Array::from(vec![0, 0, 1]);
        let key = UInt64Array::from(vec![0, 1, 0]);
        let a = Int64Array::from(vec![2, 4, 6]);
        let b = Int64Array::from(vec![1, -2, 3]);
        let c = Int64Array::from(vec![Some(5), None, Some(7)]);

        RecordBatch::try_new(
            COMPLETE_SCHEMA.clone(),
            vec![
                Arc::new(time),
                Arc::new(subsort),
                Arc::new(key),
                Arc::new(a),
                Arc::new(b),
                Arc::new(c),
            ],
        )
        .unwrap()
    };

    #[dynamic]
    static METADATA_BATCH: RecordBatch = {
        let key_hash = UInt64Array::from(vec![0, 1]);
        let entity_keys = StringArray::from(vec!["0", "1"]);
        RecordBatch::try_new(
            METADATA_SCHEMA.clone(),
            vec![Arc::new(key_hash), Arc::new(entity_keys)],
        )
        .unwrap()
    };

    #[dynamic]
    static PROJECTED_BATCH: RecordBatch = {
        let time = TimestampNanosecondArray::from(vec![5, 10, 15]);
        let time = Arc::new(time);
        let subsort = UInt64Array::from(vec![0, 0, 1]);
        let subsort = Arc::new(subsort);
        let key = UInt64Array::from(vec![0, 1, 0]);
        let key = Arc::new(key);
        let a = Int64Array::from(vec![2, 4, 6]);
        let c = Int64Array::from(vec![Some(5), None, Some(7)]);

        RecordBatch::try_new(
            PROJECTED_SCHEMA.clone(),
            vec![time, subsort, key, Arc::new(a), Arc::new(c)],
        )
        .unwrap()
    };
}
