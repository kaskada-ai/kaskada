use std::fs::File;
use std::io::{BufReader, Cursor};
use std::sync::Arc;

use anyhow::Context;
use arrow::array::ArrowPrimitiveType;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimestampMillisecondType};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use sparrow_api::kaskada::v1alpha::source_data::{self, Source};

use sparrow_api::kaskada::v1alpha::PulsarConfig;
use tracing::info;

use crate::execute::pulsar_schema;
use crate::metadata::file_from_path;

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
    pub fn try_from(source: &Source) -> anyhow::Result<Self> {
        match source {
            source_data::Source::ParquetPath(path) => {
                let file = file_from_path(std::path::Path::new(&path))?;
                Self::try_from_parquet(file)
            }
            source_data::Source::CsvPath(path) => {
                let file = file_from_path(std::path::Path::new(&path))?;
                Self::try_from_csv(file)
            }
            source_data::Source::CsvData(content) => {
                let string_reader = BufReader::new(Cursor::new(content));
                Self::try_from_csv(string_reader)
            }
            source_data::Source::PulsarSubscription(ps) => {
                let config = ps.config.as_ref().context("missing config")?;
                Self::try_from_pulsar(config)
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

    /// Create a `RawMetadata` fram a Pulsar topic.
    pub(crate) fn try_from_pulsar(config: &PulsarConfig) -> anyhow::Result<Self> {
        let raw_schema = pulsar_schema::get_pulsar_schema(
            config.admin_service_url.as_str(),
            config.tenant.as_str(),
            config.namespace.as_str(),
            config.topic_name.as_str(),
            config.auth_params.as_str(),
        )
        .map_err(|e| anyhow::anyhow!("Failed to get pulsar schema: {:?}", e))?;

        let rm = Self::try_from_raw_schema(Arc::new(raw_schema))?;
        // inject _publish_time field so that we have a consistent column to sort on
        // (this will always be our time_column in Pulsar sources)
        let publish_time = Field::new("_publish_time", TimestampMillisecondType::DATA_TYPE, false);
        let mut new_fields = rm.table_schema.fields.clone();
        new_fields.push(publish_time);
        tracing::debug!("pulsar schema fields: {:?}", new_fields);
        Ok(Self {
            raw_schema: rm.raw_schema,
            table_schema: Arc::new(Schema::new(new_fields)),
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
