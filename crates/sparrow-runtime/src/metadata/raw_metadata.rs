use std::io::{BufReader, Cursor};
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use error_stack::{IntoReport, ResultExt, IntoReportCompat};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sparrow_api::kaskada::v1alpha::file_path::Path;
use tempfile::NamedTempFile;
use tracing::info;

use crate::metadata::file_from_path;
use crate::object_store_url::ObjectStoreKey;
use crate::{ObjectStoreRegistry, ObjectStoreUrl};

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "object store error")]
    ObjectStore,
    #[display(fmt = "no object store registry")]
    NoObjectStoreRegistry,
    #[display(fmt = "local file error")]
    LocalFileError,
    #[display(fmt = "download error")]
    DownloadError,
    #[display(fmt = "reading schema error")]
    ReadSchemaError
}

impl error_stack::Context for Error {}

#[non_exhaustive]
pub struct RawMetadata {
    /// The raw schema of the file(s) backing this metadata.
    pub raw_schema: SchemaRef,
    /// The schema of the table as presented to the user.
    ///
    /// This is the result of applying schema conversions to the raw schema,
    /// such as removing time zones, dropping decimal columns, etc.
    pub table_schema: SchemaRef,
}

impl RawMetadata {
    /// Createa a `RawMetadata` from a `v1alpha::file_path::Path`.
    pub async fn try_from(
        source_path: &Path,
        object_store_registry: Option<&ObjectStoreRegistry>,
    ) -> error_stack::Result<Self, Error> {
        match source_path {
            Path::ParquetPath(path) => Self::try_from_parquet(path, object_store_registry).await,
            Path::CsvPath(path) => Self::try_from_csv(path, object_store_registry).await,
            Path::CsvData(content) => {
                let string_reader = BufReader::new(Cursor::new(content));
                Self::try_from_csv_reader(string_reader)
            },
        }
    }

    /// Create `RawMetadata` from a raw schema.
    pub fn from_raw_schema(raw_schema: SchemaRef) -> Self {
        // Convert the raw schema to a table schema.
        let table_schema = convert_schema(raw_schema.as_ref());

        Self {
            raw_schema,
            table_schema,
        }
    }

    /// Create a `RawMetadata` from a parquet string path and object store registry
    async fn try_from_parquet(path: &String, object_store_registry: Option<&ObjectStoreRegistry>) -> error_stack::Result<Self, Error>{
        let object_store_url = ObjectStoreUrl::from_str(path).change_context_lazy(|| Error::ObjectStore)?;
        let object_store_key = object_store_url.key().change_context_lazy(|| Error::ObjectStore)?;
        match (object_store_key, object_store_registry) {
            (ObjectStoreKey::Local, _) => {
                let path = object_store_url.path().change_context_lazy(|| Error::ObjectStore)?.to_string();
                let path = format!("/{}", path);
                let path = std::path::Path::new(&path);
                Self::try_from_parquet_path(path).into_report().change_context_lazy(|| Error::ReadSchemaError)
            },
            (_, None) => error_stack::bail!(Error::NoObjectStoreRegistry),
            (_, Some(object_store_registry)) => {
                let download_file = NamedTempFile::new().map_err(|_| Error::DownloadError)?;
                object_store_url
                .download(object_store_registry, download_file.path().to_path_buf())
                .await
                .change_context_lazy(|| Error::DownloadError)?;
                Self::try_from_parquet_path(download_file.path()).into_report().change_context_lazy(|| Error::ReadSchemaError)
            }
        }
    }

    /// Create a `RawMetadata` from a CSV string path and object store registry
    async fn try_from_csv(path: &String, object_store_registry: Option<&ObjectStoreRegistry>) -> error_stack::Result<Self, Error>{
        let object_store_url = ObjectStoreUrl::from_str(path).change_context_lazy(|| Error::ObjectStore)?;
        let object_store_key = object_store_url.key().change_context_lazy(|| Error::ObjectStore)?;
        match (object_store_key, object_store_registry) {
            (ObjectStoreKey::Local, _) => {
                let path = object_store_url.path().change_context_lazy(|| Error::ObjectStore)?.to_string();
                let path = format!("/{}", path);
                let file = file_from_path(std::path::Path::new(&path)).into_report().change_context_lazy(|| Error::LocalFileError)?;
                Self::try_from_csv_reader(file)
            },
            (_, None) => error_stack::bail!(Error::NoObjectStoreRegistry),
            (_, Some(object_store_registry)) => {
                let download_file = NamedTempFile::new().into_report().change_context_lazy(|| Error::DownloadError)?;
                object_store_url
                .download(object_store_registry, download_file.path().to_path_buf())
                .await
                .change_context_lazy(|| Error::DownloadError)?;
                let file = file_from_path(download_file.path()).into_report().change_context_lazy(|| Error::DownloadError)?;
                Self::try_from_csv_reader(file)
            }
        }
    }

    /// Create a `RawMetadata` fram a Parquet file path.
    fn try_from_parquet_path(path: &std::path::Path) -> anyhow::Result<Self> {
        let file = file_from_path(path)?;
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let raw_schema = parquet_reader.schema();
        Ok(Self::from_raw_schema(raw_schema.clone()))
    }

    /// Create a `RawMetadata` from a reader of a CSV file or string.
    fn try_from_csv_reader<R>(reader: R) -> error_stack::Result<Self, Error>
    where
        R: std::io::Read + std::io::Seek,
    {
        use arrow::csv::ReaderBuilder;

        let raw_reader = ReaderBuilder::new()
            .has_header(true)
            // We only need the first row to find the minimum timestamp.
            .with_batch_size(1)
            // Use up to 1000 records to infer schemas.
            //
            // CSV is mostly used for small tests, so we expect to get enough
            // information about a given CSV file pretty quick. If this doesn't,
            // we can increase, or allow the user to specify the schema.
            .infer_schema(Some(1000))
            .build(reader)
            .into_report()
            .change_context_lazy(|| Error::ReadSchemaError)?;

        let raw_schema = raw_reader.schema();
        Ok(Self::from_raw_schema(raw_schema))
    }
}

/// Converts the schema to special case Timestamps fields.
///
/// Arrow doesn't support time zones very well; it assumes all have a time zone
/// of `None`, which will use system time. Sparrow only operates on
/// [arrow::datatypes::TimestampNanosecondType], and currently cannot pass
/// through a time zone. In order to load and operate on time-zoned data, this
/// is a hack that forcibly casts all timestamp types to a time zone of `None`.
/// This can cause incorrect errors and possible ordering problems when multiple
/// input files have different time zones.
///
/// Arrow also does not support Decimal types. As of now, we are currently
/// dropping the columns that are Decimal types since we do not support at query
/// time either.
fn convert_schema(schema: &Schema) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .filter_map(|field| {
            match field.data_type() {
                DataType::Timestamp(time_unit, Some(tz)) => {
                    // TODO: We discard this because the conversion from an Arrow
                    // schema to the Schema protobuf currently fails on such timestamp columns.
                    info!(
                        "Discarding time zone {:?} on timestamp column '{}'",
                        tz,
                        field.name()
                    );
                    Some(Field::new(
                        field.name(),
                        DataType::Timestamp(time_unit.clone(), None),
                        field.is_nullable(),
                    ))
                }
                DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
                    // TODO: Support decimal columns
                    info!("Discarding decimal column '{}'", field.name());
                    None
                }
                _ => Some(field.clone()),
            }
        })
        .collect();
    Arc::new(Schema::new(fields))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use crate::RawMetadata;

    #[test]
    fn test_raw_metadata() {
        let raw_schema = Arc::new(Schema::new(vec![
            Field::new(
                "time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("subsort", DataType::UInt64, false),
            Field::new("key", DataType::UInt64, false),
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));

        let metadata = RawMetadata::from_raw_schema(raw_schema.clone());
        assert_eq!(metadata.raw_schema, raw_schema);
        assert_eq!(metadata.table_schema, raw_schema);
    }

    #[test]
    fn test_raw_metadata_conversion() {
        let raw_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Utf8, false),
            // Time zone should be removed.
            Field::new(
                "time_zone",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_owned())),
                false,
            ),
            // Decimal column should be dropped.
            Field::new("decimal", DataType::Decimal128(10, 12), false),
            Field::new("subsort", DataType::UInt64, false),
            Field::new("key", DataType::UInt64, false),
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));

        let converted_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Utf8, false),
            Field::new(
                "time_zone",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("subsort", DataType::UInt64, false),
            Field::new("key", DataType::UInt64, false),
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));

        let metadata = RawMetadata::from_raw_schema(raw_schema.clone());
        assert_eq!(metadata.raw_schema, raw_schema);
        assert_eq!(metadata.table_schema, converted_schema);
    }

    #[test]
    fn test_raw_metadata_timestamp_drop_timezones() {
        let raw_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Utf8, false),
            // Time zone should be removed.
            Field::new(
                "time_zone_micro",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".to_owned())),
                false,
            ),
            Field::new(
                "time_zone_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_owned())),
                false,
            ),
            Field::new(
                "time_zone_second",
                DataType::Timestamp(TimeUnit::Second, Some("UTC".to_owned())),
                false,
            ),
            Field::new(
                "time_zone_milli",
                DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".to_owned())),
                false,
            ),
        ]));

        let converted_schema = Arc::new(Schema::new(vec![
            Field::new("time", DataType::Utf8, false),
            Field::new(
                "time_zone_micro",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new(
                "time_zone_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "time_zone_second",
                DataType::Timestamp(TimeUnit::Second, None),
                false,
            ),
            Field::new(
                "time_zone_milli",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));

        let metadata = RawMetadata::from_raw_schema(raw_schema.clone());
        assert_eq!(metadata.raw_schema, raw_schema);
        assert_eq!(metadata.table_schema, converted_schema);
    }
}
