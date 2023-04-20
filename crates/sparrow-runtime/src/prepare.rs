use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Cursor};
use std::path::Path;

use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, IntoReportCompat, Report, ResultExt};
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

use serde_yaml;
use sha2::Digest;
use sparrow_api::kaskada::v1alpha::{
    slice_plan, source_data, PreparedFile, PulsarSubscription, SourceData, TableConfig,
};

mod error;
mod prepare_iter;
mod slice_preparer;

pub use error::*;
pub use prepare_iter::*;
use sparrow_api::kaskada::v1alpha::slice_plan::Slice;
use tracing::Instrument;

use crate::execute::pulsar_reader::read_pulsar_stream;
use crate::s3::{S3Helper, S3Object};
use crate::{PreparedMetadata, RawMetadata};

const GIGABYTE_IN_BYTES: usize = 1_000_000_000;

/// Prepare batches from the file according to the `config` and `slice`.
///
/// Returns a fallible iterator over pairs containing the data and metadata
/// batches.
pub async fn prepared_batches<'a>(
    source_data: &SourceData,
    config: &'a TableConfig,
    slice: &'a Option<slice_plan::Slice>,
) -> error_stack::Result<PrepareIter<'a>, Error> {
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
                reader_from_parquet(config, reader, file_size, prepare_hash, slice)?
            }
            source_data::Source::CsvPath(source) => {
                let file = open_file(source)?;
                let reader = BufReader::new(file);
                reader_from_csv(config, reader, prepare_hash, slice)?
            }
            source_data::Source::CsvData(content) => {
                let content = Cursor::new(content.to_string());
                let reader = BufReader::new(content);
                reader_from_csv(config, reader, prepare_hash, slice)?
            }
            source_data::Source::PulsarSubscription(ps) => {
                reader_from_pulsar(config, ps, prepare_hash, slice).await?
            }
        },
    };

    Ok(prepare_iter)
}

async fn reader_from_pulsar<'a>(
    config: &'a TableConfig,
    pulsar_subscription: &PulsarSubscription,
    prepare_hash: u64,
    slice: &'a Option<Slice>,
) -> error_stack::Result<PrepareIter<'a>, Error> {
    let pulsar_config = pulsar_subscription.config.as_ref().ok_or(Error::Internal)?;
    let pm = RawMetadata::try_from_pulsar(pulsar_config)
        .await
        .change_context(Error::CreatePulsarReader)?;

    let consumer =
        crate::execute::pulsar_reader::pulsar_consumer(pulsar_subscription, pm.user_schema.clone())
            .await?;
    let stream = read_pulsar_stream(
        pm.sparrow_metadata.raw_schema.clone(),
        consumer,
        pulsar_subscription.last_publish_time,
    );
    PrepareIter::try_new(stream, config, pm.sparrow_metadata, prepare_hash, slice)
        .into_report()
        .change_context(Error::CreatePulsarReader)
}

// this is to avoid putting pulsar auth info in the logs
pub struct SourceDataWrapper<'a>(&'a SourceData);

impl<'a> fmt::Display for SourceDataWrapper<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.source.as_ref() {
            Some(source_data::Source::ParquetPath(path)) => write!(f, "{}", path),
            Some(source_data::Source::CsvPath(path)) => write!(f, "{}", path),
            Some(source_data::Source::CsvData(_)) => write!(f, "csv data"),
            Some(source_data::Source::PulsarSubscription(ps)) => {
                let config = ps.config.as_ref().unwrap();
                write!(
                    f,
                    "pulsar subscription {} to {} @ {}",
                    ps.subscription_id,
                    match crate::execute::output::pulsar::format_topic_url(config) {
                        Ok(url) => url,
                        Err(_) => "invalid pulsar url".to_string(),
                    },
                    config.broker_service_url
                )
            }
            None => write!(f, "empty source (should never happen)"),
        }
    }
}

/// Prepare the given file and return the list of prepared files.
pub async fn prepare_file(
    source_data: &SourceData,
    output_path: &Path,
    output_prefix: &str,
    table_config: &TableConfig,
    slice: &Option<slice_plan::Slice>,
) -> error_stack::Result<(Vec<PreparedMetadata>, Vec<PreparedFile>), Error> {
    let preparer = prepared_batches(source_data, table_config, slice).await?;
    let batch_count = std::sync::atomic::AtomicUsize::new(0);

    let write_results = |output_ordinal: usize, records: RecordBatch, metadata: RecordBatch| {
        let output = output_path.join(format!("{output_prefix}-{output_ordinal}.parquet"));
        let output_file = create_file(&output)?;

        let metadata_output =
            output_path.join(format!("{output_prefix}-{output_ordinal}-metadata.parquet"));
        let metadata_output_file = create_file(&metadata_output)?;

        write_batch(output_file, records).change_context(Error::WriteParquetData)?;
        write_batch(metadata_output_file, metadata).change_context(Error::WriteMetadata)?;

        let prepared_metadata =
            PreparedMetadata::try_from_local_parquet_path(&output, &metadata_output)
                .into_report()
                .change_context(Error::DetermineMetadata)?;
        let prepared_file: PreparedFile = prepared_metadata
            .to_owned()
            .try_into()
            .change_context(Error::Internal)?;
        tracing::info!("Prepared batch {output_ordinal} with metadata {prepared_metadata:?}");

        let metadata_yaml_output =
            output_path.join(format!("{output_prefix}-{output_ordinal}-metadata.yaml"));
        let metadata_yaml_output_file = create_file(&metadata_yaml_output)?;
        serde_yaml::to_writer(metadata_yaml_output_file, &prepared_file)
            .into_report()
            .change_context(Error::Internal)?;
        tracing::info!("Wrote metadata yaml to {:?}", metadata_yaml_output);

        Ok((prepared_metadata, prepared_file))
    };

    let results = preparer.filter_map(|r| {
        let n = batch_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        async move {
            match r {
                Ok((records, metadata)) => {
                    tracing::debug!("Prepared batch {n} has {} rows", records.num_rows());

                    if records.num_rows() == 0 {
                        return None;
                    }
                    Some(write_results(n, records, metadata))
                }
                Err(e) => Some(Err(Report::new(e).change_context(Error::Internal))),
            }
        }
    });

    // unzip the stream of (prepared_file, prepared_metadata) into two vectors
    let (pm, pf): (Vec<_>, Vec<_>) = results.try_collect::<Vec<_>>().await?.into_iter().unzip();
    Ok((pm, pf))
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
            tracing::debug!(
                "Prepared batch {batch_count} has {} rows",
                prepared_file.num_rows
            );

            tracing::debug!(
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
        source_data::Source::PulsarSubscription(ps) => {
            let mut hasher = sha2::Sha224::new();
            let config = ps.config.as_ref().ok_or(Error::Internal)?;
            hasher.update(&config.broker_service_url);
            hasher.update(&config.tenant);
            hasher.update(&config.namespace);
            hasher.update(&config.topic_name);
            hasher.update(&ps.subscription_id);
            let mut bytes = [0u8; 8];
            bytes.copy_from_slice(&ps.last_publish_time.to_be_bytes());
            hasher.update(bytes);
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

fn reader_from_parquet<'a, R: parquet::file::reader::ChunkReader + 'static>(
    config: &'a TableConfig,
    reader: R,
    file_size: u64,
    prepare_hash: u64,
    slice: &'a Option<slice_plan::Slice>,
) -> error_stack::Result<PrepareIter<'a>, Error> {
    let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(reader)
        .into_report()
        .change_context(Error::CreateParquetReader)?;
    let num_rows = parquet_reader.metadata().file_metadata().num_rows();
    let num_files = get_num_files(file_size);
    let batch_size = get_batch_size(num_rows, num_files);
    let parquet_reader = parquet_reader.with_batch_size(batch_size);

    let raw_metadata = RawMetadata::from_raw_schema(parquet_reader.schema().clone());
    let reader = parquet_reader
        .build()
        .into_report()
        .change_context(Error::CreateParquetReader)?;
    // create a Stream from reader
    let stream_reader = futures::stream::iter(reader);

    PrepareIter::try_new(stream_reader, config, raw_metadata, prepare_hash, slice)
        .into_report()
        .change_context(Error::CreateParquetReader)
}

const BATCH_SIZE: usize = 1_000_000;

fn reader_from_csv<'a, R: std::io::Read + std::io::Seek + Send + 'static>(
    config: &'a TableConfig,
    reader: R,
    prepare_hash: u64,
    slice: &'a Option<slice_plan::Slice>,
) -> error_stack::Result<PrepareIter<'a>, Error> {
    use arrow::csv::ReaderBuilder;

    // Create the CSV reader.
    let csv_reader = ReaderBuilder::new()
        .has_header(true)
        .with_batch_size(BATCH_SIZE);
    let reader = csv_reader
        .build(reader)
        .into_report()
        .change_context(Error::CreateCsvReader)?;
    let raw_metadata = RawMetadata::from_raw_schema(reader.schema());
    let stream_reader = futures::stream::iter(reader);

    PrepareIter::try_new(stream_reader, config, raw_metadata, prepare_hash, slice)
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

        let table_config = TableConfig::new(
            "Event",
            &Uuid::new_v4(),
            "timestamp",
            Some("subsort_id"),
            "anonymousId",
            "user",
        );

        let prepared_batches = super::prepared_batches(&source_data, &table_config, &None)
            .await
            .unwrap();
        let prepared_batches = prepared_batches.collect::<Vec<_>>().await;
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

        let table_config = TableConfig::new(
            "Segment",
            &Uuid::new_v4(),
            "timestamp",
            Some("subsort_id"),
            "anonymousId",
            "user",
        );

        let prepared_batches = super::prepared_batches(&source_data, &table_config, &None)
            .await
            .unwrap();
        let prepared_batches = prepared_batches.collect::<Vec<_>>().await;
        assert_eq!(prepared_batches.len(), 1);
        let (prepared_batch, metadata) = prepared_batches[0].as_ref().unwrap();
        let _prepared_schema = prepared_batch.schema();
        let _metadata_schema = metadata.schema();
    }
}
