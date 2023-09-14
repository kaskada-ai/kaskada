use std::io::{BufReader, Cursor};
use std::str::FromStr;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use sparrow_api::kaskada::v1alpha::{
    slice_plan, source_data, PreparedFile, SourceData, TableConfig,
};

mod column_behavior;
mod error;
pub(crate) mod execute_input_stream;
mod prepare_input_stream;
mod prepare_metadata;
pub mod preparer;
mod slice_preparer;

pub use error::*;
pub(crate) use prepare_metadata::*;

use crate::read::ParquetFile;
use crate::stores::{ObjectMetaExt, ObjectStoreRegistry, ObjectStoreUrl};
use crate::{
    PreparedMetadata, RawMetadata, DETERMINISTIC_RUNTIME_HASHER, UPLOAD_BUFFER_SIZE_IN_BYTES,
};

const GIGABYTE_IN_BYTES: usize = 1_000_000_000;

/// Prepare batches from the file according to the `config` and `slice`.
///
/// Returns a fallible iterator over pairs containing the data and metadata
/// batches.
pub async fn prepared_batches<'a>(
    object_stores: &ObjectStoreRegistry,
    source_data: &SourceData,
    config: &'a TableConfig,
    slice: &'a Option<slice_plan::Slice>,
) -> error_stack::Result<BoxStream<'a, error_stack::Result<(RecordBatch, RecordBatch), Error>>, Error>
{
    let prepare_iter = match source_data.source.as_ref() {
        None => error_stack::bail!(Error::MissingField("source")),
        Some(source) => match source {
            source_data::Source::ParquetPath(source) => {
                let url = ObjectStoreUrl::from_str(source)
                    .change_context(Error::InvalidUrl(source.to_owned()))?;
                let file = ParquetFile::try_new(object_stores, url, None)
                    .await
                    .change_context(Error::CreateReader)?;

                let file_size = file.object_meta.size;
                let num_rows = file.num_rows();
                let num_files = get_num_files(file_size);
                let batch_size = get_batch_size(num_rows, num_files);

                let prepare_hash = file.object_meta.etag_hash();

                let raw_metadata = RawMetadata::from_raw_schema(file.schema.clone())
                    .change_context(Error::ReadSchema)?;

                let reader = file
                    .read_stream(Some(batch_size), None)
                    .await
                    .change_context(Error::CreateReader)?
                    .map(|batch| batch.change_context(Error::ReadingBatch))
                    .boxed();
                prepare_input_stream::prepare_input(
                    reader,
                    config,
                    raw_metadata,
                    prepare_hash,
                    slice,
                )
                .await
                .into_report()
                .change_context(Error::CreateReader)?
            }
            source_data::Source::CsvPath(source) => {
                let url = ObjectStoreUrl::from_str(source)
                    .change_context(Error::InvalidUrl(source.to_owned()))?;
                let local_file = tempfile::Builder::new()
                    .suffix(".csv")
                    .tempfile()
                    .into_report()
                    .change_context(Error::CreateReader)?;

                // Get the prepare hash. This could be cleaned up if we had a better wrapper
                // around the object stores.
                let object_store = object_stores
                    .object_store(&url)
                    .change_context(Error::CreateReader)?;
                let location = url.path().change_context(Error::CreateReader)?;
                let object_meta = object_store
                    .head(&location)
                    .await
                    .into_report()
                    .change_context(Error::CreateReader)?;
                let prepare_hash = object_meta.etag_hash();

                // For CSV we need to download the file (for now) to perform inference.
                // We could improve this by looking at the size and creating an in-memory
                // buffer and/or looking at a prefix of the file...
                object_stores
                    .download(url, local_file.path())
                    .await
                    .change_context(Error::DownloadingObject)?;

                // Transfer the local file to the reader. When the CSV reader
                // completes the reader will be dropped, and the file deleted.
                let reader = BufReader::new(local_file);
                reader_from_csv(config, reader, prepare_hash, slice).await?
            }
            source_data::Source::CsvData(content) => {
                let prepare_hash = DETERMINISTIC_RUNTIME_HASHER.hash_one(content);

                let content = Cursor::new(content.to_string());
                let reader = BufReader::new(content);
                reader_from_csv(config, reader, prepare_hash, slice).await?
            }
        },
    };

    Ok(prepare_iter)
}

pub async fn prepare_file(
    object_stores: &ObjectStoreRegistry,
    source_data: &SourceData,
    output_path_prefix: &str,
    output_file_prefix: &str,
    table_config: &TableConfig,
    slice: &Option<slice_plan::Slice>,
) -> error_stack::Result<Vec<PreparedFile>, Error> {
    let output_url = ObjectStoreUrl::from_str(output_path_prefix)
        .change_context_lazy(|| Error::InvalidUrl(output_path_prefix.to_owned()))?;

    let object_store = object_stores
        .object_store(&output_url)
        .change_context(Error::Internal)?;

    let mut prepare_stream = prepared_batches(object_stores, source_data, table_config, slice)
        .await?
        .enumerate();

    let mut prepared_files = Vec::new();
    let mut uploads = FuturesUnordered::new();
    while let Some((n, next)) = prepare_stream.next().await {
        let (data, metadata) = next?;

        let data_url = output_url
            .join(&format!("{output_file_prefix}-{n}.parquet"))
            .change_context(Error::Internal)?;
        let metadata_url = output_url
            .join(&format!("{output_file_prefix}-{n}-metadata.parquet"))
            .change_context(Error::Internal)?;

        // Create the prepared file via PreparedMetadata.
        // TODO: We could probably do this directly, eliminating the PreparedMetadata struct.
        let prepared_file: PreparedFile =
            PreparedMetadata::try_from_data(data_url.to_string(), &data, metadata_url.to_string())
                .into_report()
                .change_context(Error::Internal)?
                .try_into()
                .change_context(Error::Internal)?;
        prepared_files.push(prepared_file);

        uploads.push(write_parquet(data, data_url, object_store.clone()));
        uploads.push(write_parquet(metadata, metadata_url, object_store.clone()));
    }

    // Wait for the uploads.
    while let Some(upload) = uploads.try_next().await? {
        tracing::info!("Finished uploading {upload}");
    }

    Ok(prepared_files)
}

async fn write_parquet(
    batch: RecordBatch,
    url: ObjectStoreUrl,
    object_store: Arc<dyn ObjectStore>,
) -> error_stack::Result<ObjectStoreUrl, Error> {
    let path = url
        .path()
        .change_context_lazy(|| Error::Write(url.url().clone()))?;
    let (upload_id, writer) = object_store
        .put_multipart(&path)
        .await
        .into_report()
        .change_context_lazy(|| Error::Write(url.url().clone()))?;
    tracing::info!("Multipart upload to {url} started with ID {upload_id}");

    let mut writer = parquet::arrow::AsyncArrowWriter::try_new(
        writer,
        batch.schema(),
        UPLOAD_BUFFER_SIZE_IN_BYTES,
        None,
    )
    .into_report()
    .change_context_lazy(|| Error::Write(url.url().clone()))?;

    writer
        .write(&batch)
        .await
        .into_report()
        .change_context_lazy(|| Error::Write(url.url().clone()))?;

    writer
        .close()
        .await
        .into_report()
        .change_context_lazy(|| Error::Write(url.url().clone()))?;

    Ok(url)
}

fn get_num_files(file_size: usize) -> usize {
    // The number of files is the ceiling of the number of gigabytes in the file
    // e.g. 2.5 gb -> 3 files or 1 gb -> 1 file
    (file_size + GIGABYTE_IN_BYTES - 1) / GIGABYTE_IN_BYTES
}

fn get_batch_size(num_rows: usize, num_files: usize) -> usize {
    // To get the files ~1GB in size, we assume that the ceiling of the total number
    // of rows divided by the number of files.
    (num_rows + num_files - 1) / num_files
}

const BATCH_SIZE: usize = 1_000_000;

async fn reader_from_csv<'a, R: std::io::Read + std::io::Seek + Send + 'static>(
    config: &'a TableConfig,
    mut reader: R,
    prepare_hash: u64,
    slice: &'a Option<slice_plan::Slice>,
) -> error_stack::Result<BoxStream<'a, error_stack::Result<(RecordBatch, RecordBatch), Error>>, Error>
{
    use arrow::csv::ReaderBuilder;

    let position = reader
        .stream_position()
        .into_report()
        .change_context(Error::Internal)?;
    let (raw_schema, _) = arrow::csv::reader::Format::default()
        .with_header(true)
        .infer_schema(&mut reader, None)
        .into_report()
        .change_context(Error::ReadSchema)?;
    let raw_schema = Arc::new(raw_schema);

    // Create the CSV reader.
    reader
        .seek(std::io::SeekFrom::Start(position))
        .into_report()
        .change_context(Error::Internal)?;

    let csv_reader = ReaderBuilder::new(raw_schema.clone())
        .has_header(true)
        .with_batch_size(BATCH_SIZE);
    let reader = csv_reader
        .build(reader)
        .into_report()
        .change_context(Error::CreateReader)?;
    let raw_metadata =
        RawMetadata::from_raw_schema(raw_schema.clone()).change_context(Error::ReadSchema)?;
    let reader = futures::stream::iter(reader)
        .map(|batch| batch.into_report().change_context(Error::ReadingBatch))
        .boxed();

    prepare_input_stream::prepare_input(reader, config, raw_metadata, prepare_hash, slice)
        .await
        .into_report()
        .change_context(Error::CreateReader)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use futures::StreamExt;
    use sparrow_api::kaskada::v1alpha::slice_plan::{EntityKeysSlice, Slice};
    use sparrow_api::kaskada::v1alpha::{source_data, SourceData, TableConfig};
    use uuid::Uuid;

    use crate::prepare::{get_batch_size, get_num_files, GIGABYTE_IN_BYTES};
    use crate::stores::ObjectStoreRegistry;

    #[test]
    fn test_get_num_files_one_file() {
        let file_size = 500_000_000;
        let expected_num_files = 1;

        let num_files = get_num_files(file_size);
        assert_eq!(num_files, expected_num_files)
    }

    #[test]
    fn test_get_num_files_round_up_files() {
        let file_size = GIGABYTE_IN_BYTES + 500_000_000;
        let expected_num_files = 2;

        let num_files = get_num_files(file_size);
        assert_eq!(num_files, expected_num_files)
    }

    #[test]
    fn test_get_num_files_zero_files() {
        let file_size = 0;
        let expected_num_files = 0;

        let num_files = get_num_files(file_size);
        assert_eq!(num_files, expected_num_files)
    }

    #[test]
    fn test_get_batch_size_single_batch() {
        // Test file size half a gig with a million rows
        let num_rows = 1_000_000;
        let file_size = 500_000_000;
        let num_files = get_num_files(file_size);

        // Since the file is less than 1 GB, the num rows is the expected batch size
        let expected_batch_size = 1_000_000;

        let batch_size = get_batch_size(num_rows, num_files);
        assert_eq!(batch_size, expected_batch_size)
    }

    #[test]
    fn test_get_batch_size_multiple_batches() {
        // Test file size is 2.5 gigs with a million rows
        let num_rows = 1_000_000;
        let file_size = 2_500_000_000;
        let num_files = get_num_files(file_size);

        // Since each batch is supposed to at most be ~1GB, and the file is 2.5GB
        // there should be 3 parts. Therefore the expected batch size should 1/3 of the
        let expected_batch_size = 333_334;
        let batch_size = get_batch_size(num_rows, num_files);
        assert_eq!(batch_size, expected_batch_size)
    }

    #[tokio::test]
    async fn test_timestamp_with_timezone_data_prepares() {
        let input_path = sparrow_testing::testdata_path("eventdata/sample_event_data.parquet");

        let input_path =
            source_data::Source::ParquetPath(format!("file:///{}", input_path.display()));
        let source_data = SourceData {
            source: Some(input_path),
        };

        let table_config = TableConfig::new_with_table_source(
            "Event",
            &Uuid::new_v4(),
            "timestamp",
            Some("subsort_id"),
            "anonymousId",
            "user",
        );

        let prepared_batches = super::prepared_batches(
            &ObjectStoreRegistry::default(),
            &source_data,
            &table_config,
            &None,
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
        assert_eq!(prepared_batches.len(), 1);
        let (prepared_batch, metadata) = prepared_batches[0].as_ref().unwrap();
        let _prepared_schema = prepared_batch.schema();
        let _metadata_schema = metadata.schema();
    }

    #[tokio::test]
    async fn test_prepare_csv() {
        let input_path =
            sparrow_testing::testdata_path("eventdata/2c889258-d676-4922-9a92-d7e9c60c1dde.csv");

        let input_path = source_data::Source::CsvPath(format!("file:///{}", input_path.display()));
        let source_data = SourceData {
            source: Some(input_path),
        };

        let table_config = TableConfig::new_with_table_source(
            "Segment",
            &Uuid::new_v4(),
            "timestamp",
            Some("subsort_id"),
            "anonymousId",
            "user",
        );

        let prepared_batches = super::prepared_batches(
            &ObjectStoreRegistry::default(),
            &source_data,
            &table_config,
            &None,
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
        assert_eq!(prepared_batches.len(), 1);
        let (prepared_batch, metadata) = prepared_batches[0].as_ref().unwrap();
        let _prepared_schema = prepared_batch.schema();
        let _metadata_schema = metadata.schema();
    }

    #[tokio::test]
    async fn test_preparation_single_entity_key_slicing() {
        let entity_keys = vec!["0b00083c-5c1e-47f5-abba-f89b12ae3cf4".to_owned()];
        let slice = Some(Slice::EntityKeys(EntityKeysSlice { entity_keys }));
        test_slicing_config(&slice, 23, 1).await;
    }

    #[tokio::test]
    async fn test_preparation_no_matching_entity_key_slicing() {
        let entity_keys = vec!["some-random-invalid-entity-key".to_owned()];
        let slice = Some(Slice::EntityKeys(EntityKeysSlice { entity_keys }));
        test_slicing_config(&slice, 0, 0).await;
    }

    #[tokio::test]
    async fn test_preparation_multiple_matching_entity_key_slicing() {
        let entity_keys = vec![
            "0b00083c-5c1e-47f5-abba-f89b12ae3cf4".to_owned(),
            "8a16beda-c07a-4625-a805-2d28f5934107".to_owned(),
        ];
        let slice = Some(Slice::EntityKeys(EntityKeysSlice { entity_keys }));
        test_slicing_config(&slice, 41, 2).await;
    }

    #[tokio::test]
    async fn test_slicing_issue() {
        let input_path = sparrow_testing::testdata_path("transactions/transactions_part1.parquet");

        let input_path =
            source_data::Source::ParquetPath(format!("file:///{}", input_path.display()));
        let source_data = SourceData {
            source: Some(input_path),
        };

        let table_config = TableConfig::new_with_table_source(
            "transactions_slicing",
            &Uuid::new_v4(),
            "transaction_time",
            Some("idx"),
            "purchaser_id",
            "",
        );

        let entity_keys = vec!["2798e270c7cab8c9eeacc046a3100a57".to_owned()];
        let slice = Some(Slice::EntityKeys(EntityKeysSlice { entity_keys }));

        let prepared_batches = super::prepared_batches(
            &ObjectStoreRegistry::default(),
            &source_data,
            &table_config,
            &slice,
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
        assert_eq!(prepared_batches.len(), 1);
        let (prepared_batch, metadata) = prepared_batches[0].as_ref().unwrap();
        assert_eq!(prepared_batch.num_rows(), 300);
        let _prepared_schema = prepared_batch.schema();
        assert_metadata_schema_eq(metadata.schema());
        assert_eq!(metadata.num_rows(), 1);
    }

    async fn test_slicing_config(
        slice: &Option<Slice>,
        num_prepared_rows: usize,
        num_metadata_rows: usize,
    ) {
        let input_path = sparrow_testing::testdata_path("eventdata/sample_event_data.parquet");

        let input_path =
            source_data::Source::ParquetPath(format!("file:///{}", input_path.display()));
        let source_data = SourceData {
            source: Some(input_path),
        };

        let table_config = TableConfig::new_with_table_source(
            "Event",
            &Uuid::new_v4(),
            "timestamp",
            Some("subsort_id"),
            "anonymousId",
            "user",
        );

        let prepared_batches = super::prepared_batches(
            &ObjectStoreRegistry::default(),
            &source_data,
            &table_config,
            slice,
        )
        .await
        .unwrap()
        .collect::<Vec<_>>()
        .await;
        assert_eq!(prepared_batches.len(), 1);
        let (prepared_batch, metadata) = prepared_batches[0].as_ref().unwrap();
        assert_eq!(prepared_batch.num_rows(), num_prepared_rows);
        let _prepared_schema = prepared_batch.schema();
        assert_metadata_schema_eq(metadata.schema());
        assert_eq!(metadata.num_rows(), num_metadata_rows);
    }

    fn assert_metadata_schema_eq(metadata_schema: Arc<Schema>) {
        let fields = vec![
            Field::new("_hash", DataType::UInt64, false),
            Field::new("_entity_key", DataType::Utf8, true),
        ];
        let schema = Arc::new(Schema::new(fields));
        assert_eq!(metadata_schema, schema);
    }
}
