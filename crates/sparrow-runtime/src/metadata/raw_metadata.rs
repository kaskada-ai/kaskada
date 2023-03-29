use std::fs::File;
use std::io::{BufReader, Cursor};
use std::str::FromStr;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sparrow_api::kaskada::v1alpha::file_path::Path;
use tempfile::NamedTempFile;
use tracing::info;

use crate::metadata::file_from_path;
use crate::{ObjectStoreRegistry, ObjectStoreUrl};

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
    pub async fn try_from(
        source_path: &Path,
        object_store_registry: &ObjectStoreRegistry,
    ) -> anyhow::Result<Self> {
        let download_file = NamedTempFile::new()?;
        let download_file_path = download_file.into_temp_path();
        let source_path = match source_path {
            Path::ParquetPath(path) => {
                let object_store_url = ObjectStoreUrl::from_str(&path).unwrap();
                object_store_url
                    .download(object_store_registry, download_file_path.to_path_buf())
                    .await
                    .unwrap();
                let path = String::from(download_file_path.to_str().unwrap());
                Path::ParquetPath(path)
            }
            Path::CsvPath(path) => {
                let object_store_url = ObjectStoreUrl::from_str(&path).unwrap();
                object_store_url
                    .download(object_store_registry, download_file_path.to_path_buf())
                    .await
                    .unwrap();
                let path = String::from(download_file_path.to_str().unwrap());
                Path::CsvPath(path)
            }
            Path::CsvData(path) => Path::CsvData(path.to_owned()),
        };
        
        Self::try_from_local(&source_path)
    }

    pub fn try_from_local(
        source_path: &Path
    ) -> anyhow::Result<Self> {
        match source_path {
            Path::ParquetPath(path) => {
                let file = file_from_path(std::path::Path::new(&path))?;
                Self::try_from_parquet(file)
            }
            Path::CsvPath(path) => {
                let file = file_from_path(std::path::Path::new(&path))?;
                Self::try_from_csv(file)
            }
            Path::CsvData(content) => {
                let string_reader = BufReader::new(Cursor::new(content));
                Self::try_from_csv(string_reader)
            }
        }
    }

    /// Create `RawMetadata` from a raw schema.
    pub fn try_from_raw_schema(raw_schema: SchemaRef) -> anyhow::Result<Self> {
        // Convert the raw schema to a table schema.
        let table_schema = convert_schema(raw_schema.as_ref());

        Ok(Self {
            raw_schema,
            table_schema,
        })
    }

    /// Create a `RawMetadata` fram a Parquet File.
    pub fn try_from_parquet(file: File) -> anyhow::Result<Self> {
        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let raw_schema = parquet_reader.schema();
        Self::try_from_raw_schema(raw_schema.clone())
    }

    /// Create a `RawMetadata` from a reader of a CSV file or string.
    fn try_from_csv<R>(reader: R) -> anyhow::Result<Self>
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
            .build(reader)?;

        let raw_schema = raw_reader.schema();
        Self::try_from_raw_schema(raw_schema)
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

        let metadata = RawMetadata::try_from_raw_schema(raw_schema.clone()).unwrap();
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

        let metadata = RawMetadata::try_from_raw_schema(raw_schema.clone()).unwrap();
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

        let metadata = RawMetadata::try_from_raw_schema(raw_schema.clone()).unwrap();
        assert_eq!(metadata.raw_schema, raw_schema);
        assert_eq!(metadata.table_schema, converted_schema);
    }
}
