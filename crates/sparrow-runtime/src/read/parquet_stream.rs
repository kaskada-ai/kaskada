use std::str::FromStr;

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::BoxStream;
use futures::StreamExt;
use hashbrown::HashSet;
use sparrow_arrow::attachments::{RecordBatchAttachment, SchemaAttachment};
use sparrow_core::{KeyTriple, TableSchema};

use crate::read::parquet_file::ParquetFile;
use crate::stores::{ObjectStoreRegistry, ObjectStoreUrl};
use crate::{validate_batch_schema, Batch};

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to parse object URL")]
    ParseObjectUrl,
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
    object_stores: &ObjectStoreRegistry,
    object_path: &str,
    projected_schema: &TableSchema,
) -> error_stack::Result<BoxStream<'static, error_stack::Result<Batch, Error>>, Error> {
    let object_path =
        ObjectStoreUrl::from_str(object_path).change_context(Error::ParseObjectUrl)?;
    let parquet_file = ParquetFile::try_new(object_stores, object_path, None)
        .await
        .change_context(Error::OpenParquetFile)?;

    let reader_columns = get_columns_to_read(parquet_file.schema.as_ref(), projected_schema)
        .into_report()
        .change_context(Error::DetermineColumns)?;

    let stream = parquet_file
        .read_stream(None, Some(reader_columns))
        .await
        .change_context(Error::OpenParquetFile)?;

    let projected_schema = projected_schema.schema_ref().clone();

    let mut max_element_seen = KeyTriple {
        time: 0,
        subsort: 0,
        key_hash: 0,
    };
    let stream = stream.map(move |item| {
        let raw_batch = item.change_context(Error::ReadingBatch)?;

        // Recreate the RecordBatch, to adjust nullability of columns.
        // The schemas should be identical / compatible, but may have different
        // nullability.
        let batch = RecordBatch::try_new(projected_schema.clone(), raw_batch.columns().to_vec())
            .into_report()
            .change_context(Error::ReadingBatch)
            .attach_printable_lazy(|| RecordBatchAttachment::new("raw_batch", &raw_batch))
            .attach_printable_lazy(|| {
                SchemaAttachment::new("projected_schema", &projected_schema)
            })?;

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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, TimestampNanosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;
    use static_init::dynamic;

    use super::*;
    use crate::read::testing::write_parquet_file;

    #[tokio::test]
    async fn test_parquet_file_source() {
        sparrow_testing::init_test_logging();

        // Create a Parquet file
        let file = write_parquet_file(&COMPLETE_BATCH, None);

        let path = format!("file://{}", file.display());

        // Test reading the file
        check_complete(&path, &COMPLETE_BATCH).await;
        check_projected(&path, &PROJECTED_BATCH).await;
    }

    async fn check_complete(object_path: &str, expected: &RecordBatch) {
        let object_stores = ObjectStoreRegistry::default();
        let table_schema = TableSchema::from_sparrow_schema(expected.schema()).unwrap();
        let mut reader = new_parquet_stream(&object_stores, object_path, &table_schema)
            .await
            .unwrap();

        assert_eq!(reader.try_next().await.unwrap().unwrap().data(), expected);
        assert!(reader.try_next().await.unwrap().is_none());
    }

    async fn check_projected(object_path: &str, expected: &RecordBatch) {
        let object_stores = ObjectStoreRegistry::default();
        let table_schema = TableSchema::from_sparrow_schema(expected.schema()).unwrap();
        let mut reader = new_parquet_stream(&object_stores, object_path, &table_schema)
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
