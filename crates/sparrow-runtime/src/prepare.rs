use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Cursor};
use std::path::Path;

use arrow::record_batch::RecordBatch;
use avro_schema::read::fallible_streaming_iterator::FallibleStreamingIterator;
use error_stack::{FutureExt, IntoReport, IntoReportCompat, ResultExt};
use fallible_iterator::FallibleIterator;
use futures::executor::block_on;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use pulsar::{Consumer, DeserializeMessage, Pulsar, TokioExecutor};
use serde_yaml;
use sha2::Digest;
use sparrow_api::kaskada::v1alpha::{file_path, slice_plan, PreparedFile, TableConfig};

mod error;
mod prepare_iter;
mod slice_preparer;

pub use error::*;
pub use prepare_iter::*;
use tracing::Instrument;
use sparrow_api::kaskada::v1alpha::slice_plan::Slice;

use crate::s3::{S3Helper, S3Object};
use crate::{PreparedMetadata, RawMetadata};
use crate::execute::pulsar_reader::PulsarReader;

const GIGABYTE_IN_BYTES: usize = 1_000_000_000;

/// Prepare batches from the file according to the `config` and `slice`.
///
/// Returns a fallible iterator over pairs containing the data and metadata
/// batches.
pub fn prepared_batches(
    file_path: &file_path::Path,
    config: &TableConfig,
    slice: &Option<slice_plan::Slice>,
) -> error_stack::Result<PrepareIter, Error> {
    let prepare_hash = get_prepare_hash(file_path)?;
    let prepare_iter = match file_path {
        file_path::Path::ParquetPath(source) => {
            let reader = open_file(source)?;
            let file_size = reader
                .metadata()
                .into_report()
                .change_context(Error::Internal)?
                .len();
            reader_from_parquet(config, reader, file_size, prepare_hash, slice)?
        }
        file_path::Path::CsvPath(source) => {
            let file = open_file(source)?;
            let reader = BufReader::new(file);
            reader_from_csv(config, reader, prepare_hash, slice)?
        }
        file_path::Path::CsvData(content) => {
            let content = Cursor::new(content.to_string());
            let reader = BufReader::new(content);
            reader_from_csv(config, reader, prepare_hash, slice)?
        }
        file_path::Path::PulsarUri(uri) => {
            reader_from_pulsar(config, uri, prepare_hash, slice)?
        }
    };

    Ok(prepare_iter)
}

fn reader_from_pulsar(config: &TableConfig, uri: &String, prepare_hash: u64, slice: &Option<Slice>) -> error_stack::Result<PrepareIter, Error> {
    let raw_metadata = RawMetadata::try_from_pulsar(uri)
        .into_report()
        .change_context(Error::CreatePulsarReader)?;

    let consumer = block_on(crate::execute::pulsar_reader::pulsar_consumer(uri, raw_metadata.raw_schema.clone()))?;
    let reader = PulsarReader::new(raw_metadata.table_schema.clone(), consumer);
    PrepareIter::try_new(reader, config, raw_metadata, prepare_hash, slice)
        .into_report()
        .change_context(Error::CreatePulsarReader)
}

/// Prepare the given file and return the list of prepared files.
pub fn prepare_file(
    input_path: &file_path::Path,
    output_path: &Path,
    output_prefix: &str,
    table_config: &TableConfig,
    slice: &Option<slice_plan::Slice>,
) -> error_stack::Result<(Vec<PreparedMetadata>, Vec<PreparedFile>), Error> {
    let mut prepared_metadatas: Vec<_> = Vec::new();
    let mut prepared_files: Vec<_> = Vec::new();
    let preparer = prepared_batches(input_path, table_config, slice)?;
    let _span = tracing::info_span!("Preparing file", ?input_path).entered();
    for (batch_count, record_batch) in preparer.iterator().enumerate() {
        let (record_batch, metadata) = record_batch?;
        tracing::info!(
            "Prepared batch {batch_count} has {} rows",
            record_batch.num_rows()
        );
        if record_batch.num_rows() > 0 {
            let output = output_path.join(format!("{output_prefix}-{batch_count}.parquet"));
            let output_file = create_file(&output)?;

            let metadata_output =
                output_path.join(format!("{output_prefix}-{batch_count}-metadata.parquet"));
            let metadata_output_file = create_file(&metadata_output)?;

            write_batch(output_file, record_batch).change_context(Error::WriteParquetData)?;
            write_batch(metadata_output_file, metadata).change_context(Error::WriteMetadata)?;

            let prepared_metadata =
                PreparedMetadata::try_from_local_parquet_path(&output, &metadata_output)
                    .into_report()
                    .change_context(Error::DetermineMetadata)?;
            let prepared_file: PreparedFile = prepared_metadata
                .to_owned()
                .try_into()
                .change_context(Error::Internal)?;
            tracing::info!("Prepared batch {batch_count} with metadata {prepared_metadata:?}");
            prepared_metadatas.push(prepared_metadata);

            let metadata_yaml_output =
                output_path.join(format!("{output_prefix}-{batch_count}-metadata.yaml"));
            let metadata_yaml_output_file = create_file(&metadata_yaml_output)?;
            serde_yaml::to_writer(metadata_yaml_output_file, &prepared_file)
                .into_report()
                .change_context(Error::Internal)?;
            tracing::info!("Wrote metadata yaml to {:?}", metadata_yaml_output);

            prepared_files.push(prepared_file);
        }
    }

    Ok((prepared_metadatas, prepared_files))
}

pub async fn upload_prepared_files_to_s3(
    s3: S3Helper,
    prepared_metadata: &[PreparedMetadata],
    output_s3_prefix: &str,
    output_file_prefix: &str,
) -> error_stack::Result<Vec<PreparedFile>, Error> {
    let span = tracing::info_span!("Uploading prepared files to S3", ?output_s3_prefix);
    let _enter = span.enter();

    let mut parquet_uploads: Vec<_> = Vec::new();
    let mut prepared_files: Vec<_> = Vec::new();
    {
        for (batch_count, prepared_metadata) in prepared_metadata.iter().enumerate() {
            let prepared_file: PreparedFile = prepared_metadata
                .to_owned()
                .try_into()
                .change_context(Error::Internal)?;
            tracing::info!(
                "Prepared batch {batch_count} has {} rows",
                prepared_file.num_rows
            );

            tracing::info!(
                "Prepared batch {batch_count} with metadata {:?}",
                prepared_metadata
            );
            // Note that the `output_s3_prefix` is formatted as `s3://<bucket>/<output_prefix>`
            let output_prefix_uri = S3Object::try_from_uri(output_s3_prefix)
                .into_report()
                .change_context(Error::Internal)?;
            let output_bucket = output_prefix_uri.bucket;
            let output_key_prefix = output_prefix_uri.key;

            let upload_object = S3Object {
                bucket: output_bucket.clone(),
                key: format!("{output_key_prefix}/{output_file_prefix}-{batch_count}.parquet"),
            };

            let metadata_upload_object = S3Object {
                bucket: output_bucket.clone(),
                key: format!(
                    "{output_key_prefix}/{output_file_prefix}-{batch_count}-metadata.parquet"
                ),
            };
            let prepared_file: PreparedFile = prepared_metadata
                .to_owned()
                .with_s3_path(&upload_object)
                .with_s3_metadata_path(&metadata_upload_object)
                .try_into()
                .change_context(Error::Internal)?;
            parquet_uploads.push(s3.upload_s3(upload_object, Path::new(&prepared_metadata.path)));
            parquet_uploads.push(s3.upload_s3(
                metadata_upload_object,
                Path::new(&prepared_metadata.metadata_path),
            ));
            prepared_files.push(prepared_file)
        }
    }
    // TODO: We could (instead) use a loop and select, which would allow us
    // to fail early if anything failed. But it is more book-keeping.
    let parquet_uploads = futures::future::join_all(parquet_uploads).in_current_span();
    for result in parquet_uploads.await {
        result.into_report().change_context(Error::UploadResult)?;
    }
    Ok(prepared_files)
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

fn create_file(path: &Path) -> error_stack::Result<File, Error> {
    File::create(path)
        .into_report()
        .change_context_lazy(|| Error::OpenFile {
            path: path.to_owned(),
        })
}

fn write_batch(
    file: File,
    record_batch: RecordBatch,
) -> error_stack::Result<(), parquet::errors::ParquetError> {
    let mut writer = ArrowWriter::try_new(file, record_batch.schema(), None)?;
    writer.write(&record_batch)?;
    writer.close()?;
    Ok(())
}

fn get_prepare_hash(path: &file_path::Path) -> error_stack::Result<u64, Error> {
    let hex_encoding = match path {
        file_path::Path::ParquetPath(source) => {
            let file = open_file(source)?;
            let mut file = BufReader::new(file);
            let mut hasher = sha2::Sha224::new();
            std::io::copy(&mut file, &mut hasher).unwrap();
            let hash = hasher.finalize();

            data_encoding::HEXUPPER.encode(&hash)
        }
        file_path::Path::CsvPath(source) => {
            let file = open_file(source)?;
            let mut file = BufReader::new(file);
            let mut hasher = sha2::Sha224::new();
            std::io::copy(&mut file, &mut hasher).unwrap();
            let hash = hasher.finalize();

            data_encoding::HEXUPPER.encode(&hash)
        }
        file_path::Path::CsvData(content) => {
            let content = Cursor::new(content.to_string());
            let mut file = BufReader::new(content);
            let mut hasher = sha2::Sha224::new();
            std::io::copy(&mut file, &mut hasher).unwrap();
            let hash = hasher.finalize();

            data_encoding::HEXUPPER.encode(&hash)
        }
        file_path::Path::PulsarUri(uri) => {
            // TODO not sure what the right approach is here, we definitely don't want
            // to read the entire contents of the topic twice. (that's what the file
            // approaches do above, but reading from the broker would be much less
            // performant.)
            let mut hasher = sha2::Sha224::new();
            hasher.update(uri);
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

fn reader_from_parquet<R: parquet::file::reader::ChunkReader + 'static>(
    config: &TableConfig,
    reader: R,
    file_size: u64,
    prepare_hash: u64,
    slice: &Option<slice_plan::Slice>,
) -> error_stack::Result<PrepareIter, Error> {
    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(reader)
        .into_report()
        .change_context(Error::CreateParquetReader)?;
    let num_rows = parquet_reader.metadata().file_metadata().num_rows();
    let num_files = get_num_files(file_size);
    let batch_size = get_batch_size(num_rows, num_files);
    let parquet_reader = parquet_reader.with_batch_size(batch_size);

    let raw_metadata = RawMetadata::try_from_raw_schema(parquet_reader.schema().clone())
        .into_report()
        .change_context(Error::CreateParquetReader)?;
    let reader = parquet_reader
        .build()
        .into_report()
        .change_context(Error::CreateParquetReader)?;

    PrepareIter::try_new(reader, config, raw_metadata, prepare_hash, slice)
        .into_report()
        .change_context(Error::CreateParquetReader)
}

const BATCH_SIZE: usize = 1_000_000;

fn reader_from_csv<R: std::io::Read + std::io::Seek + 'static>(
    config: &TableConfig,
    reader: R,
    prepare_hash: u64,
    slice: &Option<slice_plan::Slice>,
) -> error_stack::Result<PrepareIter, Error> {
    use arrow::csv::ReaderBuilder;

    // Create the CSV reader.
    let csv_reader = ReaderBuilder::new()
        .has_header(true)
        .with_batch_size(BATCH_SIZE);
    let reader = csv_reader
        .build(reader)
        .into_report()
        .change_context(Error::CreateCsvReader)?;
    let raw_metadata = RawMetadata::try_from_raw_schema(reader.schema())
        .into_report()
        .change_context(Error::CreateCsvReader)?;

    PrepareIter::try_new(reader, config, raw_metadata, prepare_hash, slice)
        .into_report()
        .change_context(Error::CreateCsvReader)
}

#[cfg(test)]
mod tests {

    use fallible_iterator::FallibleIterator;
    use sparrow_api::kaskada::v1alpha::{file_path, TableConfig};
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

    #[test]
    fn test_timestamp_with_timezone_data_prepares() {
        let input_path = sparrow_testing::testdata_path("eventdata/sample_event_data.parquet");

        let input_path = file_path::Path::ParquetPath(
            input_path
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string(),
        );

        let table_config = TableConfig::new(
            "Event",
            &Uuid::new_v4(),
            "timestamp",
            Some("subsort_id"),
            "anonymousId",
            "user",
        );

        let prepared_batches = super::prepared_batches(&input_path, &table_config, &None).unwrap();
        let prepared_batches: Vec<_> = prepared_batches.collect().unwrap();
        assert_eq!(prepared_batches.len(), 1);
        let (prepared_batch, metadata) = &prepared_batches[0];
        let _prepared_schema = prepared_batch.schema();
        let _metadata_schema = metadata.schema();
    }

    #[test]
    fn test_prepare_csv() {
        let input_path =
            sparrow_testing::testdata_path("eventdata/2c889258-d676-4922-9a92-d7e9c60c1dde.csv");

        let input_path = file_path::Path::CsvPath(
            input_path
                .canonicalize()
                .unwrap()
                .to_string_lossy()
                .to_string(),
        );

        let table_config = TableConfig::new(
            "Segment",
            &Uuid::new_v4(),
            "timestamp",
            Some("subsort_id"),
            "anonymousId",
            "user",
        );

        let prepared_batches = super::prepared_batches(&input_path, &table_config, &None).unwrap();
        let prepared_batches: Vec<_> = prepared_batches.collect().unwrap();
        assert_eq!(prepared_batches.len(), 1);
        let (prepared_batch, metadata) = &prepared_batches[0];
        let _prepared_schema = prepared_batch.schema();
        let _metadata_schema = metadata.schema();
    }
}
