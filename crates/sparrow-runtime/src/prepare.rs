use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::{BufReader, Cursor};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::{fmt, path};

use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;

use serde_yaml;
use sha2::Digest;
use sparrow_api::kaskada::v1alpha::{
    slice_plan, source_data, PreparedFile, PulsarSubscription, SourceData, TableConfig,
};

mod column_behavior;
mod error;
pub(crate) mod execute_input_stream;
mod prepare_input_stream;
mod prepare_metadata;
mod slice_preparer;

pub use error::*;
pub(crate) use prepare_metadata::*;
use sparrow_api::kaskada::v1alpha::slice_plan::Slice;
use tracing::Instrument;

use crate::stores::object_store_url::ObjectStoreKey;
use crate::stores::{ObjectStoreRegistry, ObjectStoreUrl};
use crate::{streams, PreparedMetadata, RawMetadata};

const GIGABYTE_IN_BYTES: usize = 1_000_000_000;

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
) -> error_stack::Result<BoxStream<'a, error_stack::Result<(RecordBatch, RecordBatch), Error>>, Error>
{
    let pulsar_config = pulsar_subscription.config.as_ref().ok_or(Error::Internal)?;
    let pm = RawMetadata::try_from_pulsar(pulsar_config, true)
        .await
        .change_context(Error::CreatePulsarReader)?;

    let consumer =
        streams::pulsar::stream::consumer(pulsar_subscription, pm.user_schema.clone()).await?;
    let stream = streams::pulsar::stream::preparation_stream(
        pm.sparrow_metadata.raw_schema.clone(),
        consumer,
        pulsar_subscription.last_publish_time,
    );
    prepare_input_stream::prepare_input(
        stream.boxed(),
        config,
        pm.sparrow_metadata,
        prepare_hash,
        slice,
    )
    .await
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
    object_store_registry: &ObjectStoreRegistry,
    source_data: &SourceData,
    output_path_prefix: &str,
    output_file_prefix: &str,
    table_config: &TableConfig,
    slice: &Option<slice_plan::Slice>,
) -> error_stack::Result<(Vec<PreparedMetadata>, Vec<PreparedFile>), Error> {
    let output_url = ObjectStoreUrl::from_str(output_path_prefix)
        .change_context_lazy(|| Error::InvalidUrl(output_path_prefix.to_owned()))?;
    let output_key = output_url
        .key()
        .change_context_lazy(|| Error::InvalidUrl(format!("{}", output_url)))?;

    let temp_dir = tempfile::tempdir()
        .into_report()
        .change_context(Error::Internal)?;
    let temp_dir = temp_dir.path().to_str().ok_or(Error::Internal)?;

    let path = format!(
        "/{}",
        output_url
            .path()
            .change_context_lazy(|| Error::InvalidUrl(format!("{}", output_url)))?
    );

    // If the output path is to a local destination, we'll use that. If it's to
    // a remote destination, we'll first write to a local temp file, then upload
    // that to the remote destination.
    let local_output_prefix = match output_key {
        ObjectStoreKey::Local => path::Path::new(&path),
        _ => path::Path::new(temp_dir),
    };

    let prepare_stream = prepared_batches(source_data, table_config, slice).await?;
    let batch_count = std::sync::atomic::AtomicUsize::new(0);

    let write_results = |output_ordinal: usize, records: RecordBatch, metadata: RecordBatch| {
        // Defines the local path to the result file
        let local_result_path =
            local_output_prefix.join(format!("{output_file_prefix}-{output_ordinal}.parquet"));
        let local_result_file = create_file(&local_result_path)?;

        // Defines the local path to the metadata file
        let local_metadata_output = local_output_prefix.join(format!(
            "{output_file_prefix}-{output_ordinal}-metadata.parquet"
        ));
        let local_metadata_file = create_file(&local_metadata_output)?;

        // Defines the local path to the metadata yaml file
        let metadata_yaml_output = local_output_prefix.join(format!(
            "{output_file_prefix}-{output_ordinal}-metadata.yaml"
        ));
        let metadata_yaml_output_file = create_file(&metadata_yaml_output)?;

        // Write batches to the local output files
        write_batch(local_result_file, records).change_context(Error::WriteParquetData)?;
        write_batch(local_metadata_file, metadata).change_context(Error::WriteMetadata)?;

        let prepared_metadata = PreparedMetadata::try_from_local_parquet_path(
            &local_result_path,
            &local_metadata_output,
        )
        .into_report()
        .change_context(Error::DetermineMetadata)?;

        let prepared_file: PreparedFile = prepared_metadata
            .to_owned()
            .try_into()
            .change_context(Error::Internal)?;

        tracing::info!("Prepared batch {output_ordinal} with metadata {prepared_metadata:?}");

        serde_yaml::to_writer(metadata_yaml_output_file, &prepared_file)
            .into_report()
            .change_context(Error::Internal)?;
        tracing::info!("Wrote metadata yaml to {:?}", metadata_yaml_output);

        Ok((prepared_metadata, prepared_file))
    };

    let results = prepare_stream.filter_map(|r| {
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
                Err(e) => Some(Err(e.change_context(Error::Internal))),
            }
        }
    });

    // unzip the stream of (prepared_file, prepared_metadata) into two vectors
    let (pm, pf): (Vec<_>, Vec<_>) = results.try_collect::<Vec<_>>().await?.into_iter().unzip();
    match output_key {
        ObjectStoreKey::Local | ObjectStoreKey::Memory => {
            // Prepared files are stored locally
            Ok((pm, pf))
        }
        _ => {
            // Upload the local prepared files to the remote destination
            let upload_results = upload_prepared_files(
                object_store_registry,
                &pm,
                output_path_prefix,
                output_file_prefix,
            )
            .await?;
            Ok((pm, upload_results))
        }
    }
}

async fn upload_prepared_files(
    object_store_registry: &ObjectStoreRegistry,
    prepared_metadata: &[PreparedMetadata],
    output_path_prefix: &str,
    output_file_prefix: &str,
) -> error_stack::Result<Vec<PreparedFile>, Error> {
    let span = tracing::info_span!(
        "Uploading prepared files to object store",
        ?output_path_prefix
    );
    let _enter = span.enter();

    let mut parquet_uploads: Vec<_> = Vec::new();
    let mut prepared_files: Vec<_> = Vec::new();
    {
        for (batch_count, prepared_metadata) in prepared_metadata.iter().enumerate() {
            tracing::debug!(
                "Prepared batch {batch_count} has {} rows",
                prepared_metadata.num_rows
            );

            tracing::debug!(
                "Prepared batch {batch_count} with metadata {:?}",
                prepared_metadata
            );

            let prepared_url =
                format!("{output_path_prefix}/{output_file_prefix}-{batch_count}.parquet");
            let metadata_prepared_url =
                format!("{output_path_prefix}/{output_file_prefix}-{batch_count}-metadata.parquet");

            let prepare_object_store_url = ObjectStoreUrl::from_str(prepared_url.as_str())
                .change_context_lazy(|| Error::InvalidUrl(prepared_url.as_str().to_string()))?;
            let metadata_object_store_url =
                ObjectStoreUrl::from_str(metadata_prepared_url.as_str()).change_context_lazy(
                    || Error::InvalidUrl(metadata_prepared_url.as_str().to_string()),
                )?;

            parquet_uploads.push(
                object_store_registry
                    .upload(prepare_object_store_url, Path::new(&prepared_metadata.path)),
            );

            parquet_uploads.push(object_store_registry.upload(
                metadata_object_store_url,
                Path::new(&prepared_metadata.metadata_path),
            ));

            let result_prepared_file: PreparedFile = prepared_metadata
                .to_owned()
                .with_path(prepared_url)
                .with_metadata_path(metadata_prepared_url)
                .try_into()
                .change_context(Error::Internal)?;
            prepared_files.push(result_prepared_file)
        }
    }

    // TODO: We could (instead) use a loop and select, which would allow us
    // to fail early if anything failed. But it is more book-keeping.
    let parquet_uploads = futures::future::join_all(parquet_uploads).in_current_span();
    for result in parquet_uploads.await {
        result.change_context(Error::UploadResult)?;
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
    println!("FRAZ - parquet reader built");
    let num_rows = parquet_reader.metadata().file_metadata().num_rows();
    let num_files = get_num_files(file_size);
    let batch_size = get_batch_size(num_rows, num_files);
    let parquet_reader = parquet_reader.with_batch_size(batch_size);

    println!("FRAZ - metadta: {}", parquet_reader.schema());
    let raw_metadata = RawMetadata::from_raw_schema(parquet_reader.schema().clone())
        .change_context_lazy(|| Error::ReadSchema)?;
    let reader = parquet_reader
        .build()
        .into_report()
        .change_context(Error::CreateParquetReader)?;
    // create a Stream from reader
    let stream_reader = futures::stream::iter(reader);

    println!("FRAZ - preparing input now");
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
