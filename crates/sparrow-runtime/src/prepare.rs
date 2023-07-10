use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Cursor};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::{BoxStream, FuturesUnordered};
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use sha2::Digest;
use sparrow_api::kaskada::v1alpha::{
    slice_plan, source_data, PreparedFile, SourceData, TableConfig,
};

mod column_behavior;
mod error;
pub(crate) mod execute_input_stream;
mod prepare_input_stream;
mod prepare_metadata;
mod slice_preparer;

pub use error::*;
pub(crate) use prepare_metadata::*;

use crate::stores::{ObjectStoreRegistry, ObjectStoreUrl};
use crate::{PreparedMetadata, RawMetadata};

const GIGABYTE_IN_BYTES: usize = 1_000_000_000;

/// Initial size of the upload buffer.
///
/// This balances size (if we have multiple uploads in parallel) with
/// number of "parts" required to perform an upload.
const UPLOAD_BUFFER_SIZE_IN_BYTES: usize = 5_000_000;

/// Prepare batches from the file according to the `config` and `slice`.
///
/// Returns a fallible iterator over pairs containing the data and metadata
/// batches.
pub async fn prepared_batches<'a>(
    source_data: &SourceData,
    config: &'a TableConfig,
    slice: &'a Option<slice_plan::Slice>,
) -> error_stack::Result<BoxStream<'a, error_stack::Result<(RecordBatch, RecordBatch), Error>>, Error>
{
    let prepare_hash = get_prepare_hash(source_data)?;
    let prepare_iter = match source_data.source.as_ref() {
        None => error_stack::bail!(Error::MissingField("source")),
        Some(source) => match source {
            source_data::Source::ParquetPath(source) => {
                let reader = open_file(source)?;
                let file_size = reader
                    .metadata()
                    .into_report()
                    .change_context(Error::Internal)?
                    .len();
                reader_from_parquet(config, reader, file_size, prepare_hash, slice).await?
            }
            source_data::Source::CsvPath(source) => {
                let file = open_file(source)?;
                let reader = BufReader::new(file);
                reader_from_csv(config, reader, prepare_hash, slice).await?
            }
            source_data::Source::CsvData(content) => {
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

    let mut prepare_stream = prepared_batches(source_data, table_config, slice)
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

fn open_file(path: impl AsRef<Path>) -> error_stack::Result<File, Error> {
    fn inner(path: &Path) -> error_stack::Result<File, Error> {
        File::open(path)
            .into_report()
            .change_context_lazy(|| Error::OpenFile {
                path: path.to_owned(),
            })
    }
    inner(path.as_ref())
}

fn get_prepare_hash(source_data: &SourceData) -> error_stack::Result<u64, Error> {
    let source = source_data.source.as_ref().ok_or(Error::Internal)?;
    let hex_encoding = match source {
        source_data::Source::ParquetPath(source) => {
            let file = open_file(source)?;
            let mut file = BufReader::new(file);
            let mut hasher = sha2::Sha224::new();
            std::io::copy(&mut file, &mut hasher).unwrap();
            let hash = hasher.finalize();

            data_encoding::HEXUPPER.encode(&hash)
        }
        source_data::Source::CsvPath(source) => {
            let file = open_file(source)?;
            let mut file = BufReader::new(file);
            let mut hasher = sha2::Sha224::new();
            std::io::copy(&mut file, &mut hasher).unwrap();
            let hash = hasher.finalize();

            data_encoding::HEXUPPER.encode(&hash)
        }
        source_data::Source::CsvData(content) => {
            let content = Cursor::new(content.to_string());
            let mut file = BufReader::new(content);
            let mut hasher = sha2::Sha224::new();
            std::io::copy(&mut file, &mut hasher).unwrap();
            let hash = hasher.finalize();

            data_encoding::HEXUPPER.encode(&hash)
        }
    };
    Ok(get_u64_hash(&hex_encoding))
}

fn get_u64_hash(str: &str) -> u64 {
    let mut h = DefaultHasher::new();
    str.hash(&mut h);
    h.finish()
}

fn get_num_files(file_size: u64) -> usize {
    // The number of files is the ceiling of the number of gigabytes in the file
    // e.g. 2.5 gb -> 3 files or 1 gb -> 1 file
    let file_size = file_size as usize;
    (file_size + GIGABYTE_IN_BYTES - 1) / GIGABYTE_IN_BYTES
}

fn get_batch_size(num_rows: i64, num_files: usize) -> usize {
    let num_rows = num_rows as usize;
    // To get the files ~1GB in size, we assume that the ceiling of the total number
    // of rows divided by the number of files.
    (num_rows + num_files - 1) / num_files
}

async fn reader_from_parquet<'a, R: parquet::file::reader::ChunkReader + 'static>(
    config: &'a TableConfig,
    reader: R,
    file_size: u64,
    prepare_hash: u64,
    slice: &'a Option<slice_plan::Slice>,
) -> error_stack::Result<BoxStream<'a, error_stack::Result<(RecordBatch, RecordBatch), Error>>, Error>
{
    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(reader)
        .into_report()
        .change_context(Error::CreateParquetReader)?;
    let num_rows = parquet_reader.metadata().file_metadata().num_rows();
    let num_files = get_num_files(file_size);
    let batch_size = get_batch_size(num_rows, num_files);
    let parquet_reader = parquet_reader.with_batch_size(batch_size);

    let raw_metadata = RawMetadata::from_raw_schema(parquet_reader.schema().clone())
        .change_context_lazy(|| Error::ReadSchema)?;
    let reader = parquet_reader
        .build()
        .into_report()
        .change_context(Error::CreateParquetReader)?;
    // create a Stream from reader
    let stream_reader = futures::stream::iter(reader);

    prepare_input_stream::prepare_input(
        stream_reader.boxed(),
        config,
        raw_metadata,
        prepare_hash,
        slice,
    )
    .await
    .into_report()
    .change_context(Error::CreateParquetReader)
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
        .change_context(Error::CreateCsvReader)?;
    let raw_metadata =
        RawMetadata::from_raw_schema(raw_schema.clone()).change_context(Error::ReadSchema)?;
    let stream_reader = futures::stream::iter(reader);

    prepare_input_stream::prepare_input(
        stream_reader.boxed(),
        config,
        raw_metadata,
        prepare_hash,
        slice,
    )
    .await
    .into_report()
    .change_context(Error::CreateCsvReader)
}

pub fn file_sourcedata(path: source_data::Source) -> SourceData {
    SourceData { source: Some(path) }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use sparrow_api::kaskada::v1alpha::{source_data, SourceData, TableConfig};
    use uuid::Uuid;

    use crate::prepare::{get_batch_size, get_num_files, get_u64_hash, GIGABYTE_IN_BYTES};

    #[test]
    fn test_get_u64_hash() {
        let str = "time travel".to_string();
        let expected_hash: u64 = 15924138204015979085;
        let str_hash = get_u64_hash(&str);
        assert_eq!(expected_hash, str_hash)
    }

    #[test]
    fn test_get_num_files_one_file() {
        let file_size: u64 = 500_000_000;
        let expected_num_files = 1;

        let num_files = get_num_files(file_size);
        assert_eq!(num_files, expected_num_files)
    }

    #[test]
    fn test_get_num_files_round_up_files() {
        let file_size: u64 = GIGABYTE_IN_BYTES as u64 + 500_000_000;
        let expected_num_files = 2;

        let num_files = get_num_files(file_size);
        assert_eq!(num_files, expected_num_files)
    }

    #[test]
    fn test_get_num_files_zero_files() {
        let file_size: u64 = 0;
        let expected_num_files = 0;

        let num_files = get_num_files(file_size);
        assert_eq!(num_files, expected_num_files)
    }

    #[test]
    fn test_get_batch_size_single_batch() {
        // Test file size half a gig with a million rows
        let num_rows: i64 = 1_000_000;
        let file_size: u64 = 500_000_000;
        let num_files: usize = get_num_files(file_size);

        // Since the file is less than 1 GB, the num rows is the expected batch size
        let expected_batch_size: usize = 1_000_000;

        let batch_size = get_batch_size(num_rows, num_files);
        assert_eq!(batch_size, expected_batch_size)
    }

    #[test]
    fn test_get_batch_size_multiple_batches() {
        // Test file size is 2.5 gigs with a million rows
        let num_rows: i64 = 1_000_000;
        let file_size: u64 = 2_500_000_000;
        let num_files: usize = get_num_files(file_size);

        // Since each batch is supposed to at most be ~1GB, and the file is 2.5GB
        // there should be 3 parts. Therefore the expected batch size should 1/3 of the
        let expected_batch_size = 333_334;
        let batch_size = get_batch_size(num_rows, num_files);
        assert_eq!(batch_size, expected_batch_size)
    }

    #[tokio::test]
    async fn test_timestamp_with_timezone_data_prepares() {
        let input_path = sparrow_testing::testdata_path("eventdata/sample_event_data.parquet");

        let input_path = source_data::Source::ParquetPath(
            input_path
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string(),
        );
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

        let prepared_batches = super::prepared_batches(&source_data, &table_config, &None)
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

        let input_path = source_data::Source::CsvPath(
            input_path
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string(),
        );
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

        let prepared_batches = super::prepared_batches(&source_data, &table_config, &None)
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(prepared_batches.len(), 1);
        let (prepared_batch, metadata) = prepared_batches[0].as_ref().unwrap();
        let _prepared_schema = prepared_batch.schema();
        let _metadata_schema = metadata.schema();
    }
}
