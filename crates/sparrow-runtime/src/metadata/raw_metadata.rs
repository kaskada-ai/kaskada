use std::io::{BufReader, Cursor};
use std::str::FromStr;
use std::sync::Arc;

use arrow::array::ArrowPrimitiveType;
use arrow::datatypes::{DataType, Field, FieldRef, Schema, SchemaRef, TimestampMillisecondType};
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use tempfile::NamedTempFile;

use sparrow_api::kaskada::v1alpha::source_data::{self, Source};

use sparrow_api::kaskada::v1alpha::{KafkaConfig, PulsarConfig};

use crate::metadata::file_from_path;
use crate::read::ParquetFile;
use crate::stores::{ObjectStoreRegistry, ObjectStoreUrl};
use crate::streams;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "object store error for path: {_0}")]
    ObjectStore(String),
    #[display(fmt = "no object store registry")]
    MissingObjectStoreRegistry,
    #[display(fmt = "local file error")]
    LocalFile,
    #[display(fmt = "download error")]
    Download,
    #[display(fmt = "reading schema error")]
    ReadSchema,
    #[display(fmt = "failed to get pulsar schema: {_0}")]
    PulsarSchema(String),
    #[display(fmt = "unsupport column detected: '{_0}")]
    UnsupportedColumn(String),
}

impl error_stack::Context for Error {}

#[non_exhaustive]
pub struct RawMetadata {
    /// The raw schema of the data source backing this metadata.
    pub raw_schema: SchemaRef,
    /// The schema of the data source as presented to the user.
    ///
    /// This is the result of applying schema conversions to the raw schema,
    /// such as removing time zones, dropping decimal columns, etc.
    pub table_schema: SchemaRef,
}

/// For Pulsar, we want to keep the original user_schema around for use
/// by the consumer.  This is because we want the RawMetadata.raw_schema
/// to include the publish time metadata, but if we include that when creating the
/// Pulsar consumer, Pulsar will correctly reject it as a schema mismatch.
pub struct PulsarMetadata {
    /// the schema as defined by the user on the topic, corresponding to the messages created
    /// with no additional metadata
    pub user_schema: SchemaRef,
    /// schema that includes metadata used by Sparrow
    pub sparrow_metadata: RawMetadata,
}

impl RawMetadata {
    pub async fn try_from(
        source: &Source,
        object_store_registry: &ObjectStoreRegistry,
    ) -> error_stack::Result<Self, Error> {
        match source {
            source_data::Source::ParquetPath(path) => {
                Self::try_from_parquet(path, object_store_registry).await
            }
            source_data::Source::CsvPath(path) => {
                Self::try_from_csv(path, object_store_registry).await
            }
            source_data::Source::CsvData(content) => {
                let string_reader = BufReader::new(Cursor::new(content));
                Self::try_from_csv_reader(string_reader)
            }
        }
    }

    pub async fn try_from_pulsar_subscription(
        ps: &PulsarConfig,
    ) -> error_stack::Result<Self, Error> {
        // The `_publish_time` is metadata on the pulsar message, and required
        // by the `prepare` step. However, that is not part of the user's schema.
        // The prepare path calls `try_from_pulsar` directly, so for all other cases
        // we explicitly set the schema to not include the `_publish_time` column.
        //
        // The "prepare from pulsar" step is an experimental feature, and will
        // likely change in the future, so we're okay with this hack for now.
        let should_include_publish_time = false;
        Ok(Self::try_from_pulsar(ps, should_include_publish_time)
            .await?
            .sparrow_metadata)
    }

    pub async fn try_from_kafka_subscription(_: &KafkaConfig) -> error_stack::Result<Self, Error> {
        todo!()
    }

    /// Create `RawMetadata` from a raw schema.
    pub fn from_raw_schema(raw_schema: SchemaRef) -> error_stack::Result<Self, Error> {
        // Convert the raw schema to a table schema.
        let table_schema = convert_schema(raw_schema.as_ref())?;

        Ok(Self {
            raw_schema,
            table_schema,
        })
    }

    /// Create a `RawMetadata` from a parquet string path and object store registry.
    ///
    /// This uses `object_store` to asynchronously retrieve only the Parquet
    /// footer to determine the schema.
    async fn try_from_parquet(
        path: &str,
        object_stores: &ObjectStoreRegistry,
    ) -> error_stack::Result<Self, Error> {
        let url = ObjectStoreUrl::from_str(path)
            .change_context_lazy(|| Error::ObjectStore(path.to_owned()))?;

        let parquet_file = ParquetFile::try_new(object_stores, url, None)
            .await
            .change_context_lazy(|| Error::ObjectStore(path.to_owned()))?;

        Self::from_raw_schema(parquet_file.schema)
    }

    /// Create a `RawMetadata` from a CSV string path and object store registry.
    ///
    /// For CSV, this currently needs to download a local copy of the file, since
    /// Arrow does not (as of 2023-July-06) support reading CSV using `AsyncWrite`.
    // TODO(https://github.com/kaskada-ai/kaskada/issues/486): Async CSV support.
    async fn try_from_csv(
        path: &str,
        object_stores: &ObjectStoreRegistry,
    ) -> error_stack::Result<Self, Error> {
        let object_store_url = ObjectStoreUrl::from_str(path)
            .change_context_lazy(|| Error::ObjectStore(path.to_owned()))?;

        if let Some(local_path) = object_store_url.local_path() {
            let file = file_from_path(local_path)
                .into_report()
                .change_context_lazy(|| Error::LocalFile)?;
            Self::try_from_csv_reader(file)
        } else {
            let download_file = NamedTempFile::new()
                .into_report()
                .change_context_lazy(|| Error::Download)?;
            object_stores
                .download(object_store_url, download_file.path())
                .await
                .change_context_lazy(|| Error::Download)?;
            // Pass the download_file (which implements read) directly.
            // The file will be deleted when the reader completes.
            Self::try_from_csv_reader(download_file)
        }
    }

    /// Create a `RawMetadata` from a Pulsar topic.
    pub(crate) async fn try_from_pulsar(
        config: &PulsarConfig,
        include_publish_time: bool,
    ) -> error_stack::Result<PulsarMetadata, Error> {
        // the user-defined schema in the topic
        let pulsar_schema = streams::pulsar::schema::get_pulsar_schema(
            config.admin_service_url.as_str(),
            config.tenant.as_str(),
            config.namespace.as_str(),
            config.topic_name.as_str(),
            config.auth_params.as_str(),
        )
        .await
        .change_context_lazy(|| Error::PulsarSchema("unable to get schema".to_owned()))?;

        let new_fields = if include_publish_time {
            // inject _publish_time field so that we have a consistent column to sort on
            // (this will always be our time_column in Pulsar sources)
            let publish_time = Arc::new(Field::new(
                "_publish_time",
                TimestampMillisecondType::DATA_TYPE,
                false,
            ));
            pulsar_schema
                .fields
                .iter()
                .cloned()
                .chain(std::iter::once(publish_time))
                .collect()
        } else {
            pulsar_schema.fields.clone()
        };

        tracing::debug!("pulsar schema fields: {:?}", new_fields);
        Ok(PulsarMetadata {
            user_schema: Arc::new(pulsar_schema),
            sparrow_metadata: Self::from_raw_schema(Arc::new(Schema::new(new_fields)))?,
        })
    }

    /// Create a `RawMetadata` from a reader of a CSV file or string.
    fn try_from_csv_reader<R>(reader: R) -> error_stack::Result<Self, Error>
    where
        R: std::io::Read + std::io::Seek,
    {
        let (raw_schema, _) = arrow::csv::reader::Format::default()
            .with_header(true)
            .infer_schema(reader, None)
            .into_report()
            .change_context(Error::ReadSchema)?;

        Self::from_raw_schema(Arc::new(raw_schema))
    }
}

/// Converts the schema to a table schema
fn convert_schema(schema: &Schema) -> error_stack::Result<SchemaRef, Error> {
    let fields = schema
        .fields()
        .iter()
        .map(convert_field)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

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
fn convert_field(field: &FieldRef) -> error_stack::Result<FieldRef, Error> {
    match field.data_type() {
        DataType::Timestamp(time_unit, Some(tz)) => {
            // TODO: We discard this because the conversion from an Arrow
            // schema to the Schema protobuf currently fails on such timestamp columns.
            tracing::warn!(
                "Time zones are unsupported. Interpreting column '{}' with time zone '{}' as UTC",
                tz,
                field.name()
            );
            Ok(Arc::new(Field::new(
                field.name(),
                DataType::Timestamp(time_unit.clone(), None),
                field.is_nullable(),
            )))
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => {
            tracing::warn!("Decimal columns are unsupported: '{}'", field.name());
            error_stack::bail!(Error::UnsupportedColumn(format!(
                "Decimal columns are unsupported: {}",
                field.name()
            )))
        }
        _ => Ok(field.clone()),
    }
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

        let metadata = RawMetadata::from_raw_schema(raw_schema.clone()).unwrap();
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
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
                false,
            ),
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

        let metadata = RawMetadata::from_raw_schema(raw_schema.clone()).unwrap();
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
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                false,
            ),
            Field::new(
                "time_zone_nano",
                DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
                false,
            ),
            Field::new(
                "time_zone_second",
                DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))),
                false,
            ),
            Field::new(
                "time_zone_milli",
                DataType::Timestamp(TimeUnit::Millisecond, Some(Arc::from("UTC"))),
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

        let metadata = RawMetadata::from_raw_schema(raw_schema.clone()).unwrap();
        assert_eq!(metadata.raw_schema, raw_schema);
        assert_eq!(metadata.table_schema, converted_schema);
    }

    #[test]
    fn test_raw_metadata_decimal_errors() {
        let raw_schema = Arc::new(Schema::new(vec![Field::new(
            "decimal_col",
            DataType::Decimal128(0, 0),
            false,
        )]));

        let metadata = RawMetadata::from_raw_schema(raw_schema);
        match metadata {
            Ok(_) => panic!("should not have succeeded"),
            Err(e) => {
                assert_eq!(
                    e.as_error().to_string(),
                    "unsupport column detected: 'Decimal columns are unsupported: decimal_col"
                )
            }
        }
    }
}
