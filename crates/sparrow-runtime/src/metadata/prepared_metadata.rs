use std::path::Path;
use std::sync::Arc;

use arrow::datatypes::{Schema, SchemaRef};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::ValueStatistics;
use sparrow_api::kaskada::v1alpha::PreparedFile;
use sparrow_core::TableSchema;

use crate::metadata::file_from_path;
use crate::s3::S3Object;

#[non_exhaustive]
#[derive(Clone, Debug)]
pub struct PreparedMetadata {
    pub path: String,

    /// The schema of the prepared file(s) backing this metadata.
    ///
    /// NOTE: This includes the key columns.
    ///
    /// If there were multiple files, this may represent the merged
    /// schema.
    pub prepared_schema: SchemaRef,

    /// The schema of the table as presented to the user.
    ///
    /// This is the result of applying schema conversions to the raw schema,
    /// such as removing time zones, dropping decimal columns, etc.
    pub table_schema: SchemaRef,

    /// The minimum value of the `_time` within the prepared file.
    pub min_time: i64,

    /// The maximum value of the `_time` within the prepared file.
    pub max_time: i64,

    /// The number of rows in the prepared file.
    pub num_rows: i64,

    /// The path to the metadata file.
    pub metadata_path: String,
}

fn get_time_statistics(
    metadata: &ParquetMetaData,
    row_group: usize,
) -> anyhow::Result<Option<&ValueStatistics<i64>>> {
    if metadata.file_metadata().num_rows() == 0 {
        Ok(None)
    } else {
        use parquet::file::statistics::Statistics;
        match metadata.row_group(row_group).column(0).statistics() {
            Some(Statistics::Int64(stats)) => {
                anyhow::ensure!(
                    stats.has_min_max_set(),
                    "Time column statistics missing min/max for row_group {row_group}",
                );

                Ok(Some(stats))
            }
            stats => Err(anyhow::anyhow!(
                "Time column missing or invalid for row_group {row_group}: {stats:?}"
            )),
        }
    }
}

impl PreparedMetadata {
    /// Create a `PreparedMetadata` from the path to a parquet file.
    pub fn try_from_local_parquet_path(
        parquet_path: &Path,
        metadata_parquet_path: &Path,
    ) -> anyhow::Result<Self> {
        let file = file_from_path(parquet_path)?;

        let parquet_reader = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let metadata = parquet_reader.metadata();
        let num_rows = metadata.file_metadata().num_rows();

        let prepared_schema = parquet_reader.schema();

        anyhow::ensure!(
            prepared_schema.field(0).name() == "_time",
            "First column of prepared files must be '_time'"
        );

        let min_time = get_time_statistics(metadata.as_ref(), 0)?
            .map(|stats| *stats.min())
            // Empty files contain no stats. We default to assuming the min time.
            .unwrap_or(i64::MIN);

        let max_time = get_time_statistics(metadata.as_ref(), metadata.num_row_groups() - 1)?
            .map(|stats| *stats.max())
            // Empty files contain no stats. We default to assuming the min time.
            .unwrap_or(i64::MIN);

        let path = parquet_path.to_string_lossy().into_owned();
        let metadata_path = metadata_parquet_path.to_string_lossy().into_owned();
        Self::try_from_prepared_schema(
            path,
            prepared_schema.clone(),
            min_time,
            max_time,
            num_rows,
            metadata_path,
        )
    }

    fn try_from_prepared_schema(
        path: String,
        prepared_schema: SchemaRef,
        min_time: i64,
        max_time: i64,
        num_rows: i64,
        metadata_path: String,
    ) -> anyhow::Result<Self> {
        // TODO: This uses TableSchema to validate that the key columns are present.
        // This is somewhat hacky, and should probably just be validated directly
        // when we're ready to eliminate TableSchema.
        let table_schema = TableSchema::from_sparrow_schema(prepared_schema.clone())?;
        let table_schema = Arc::new(Schema::new(table_schema.data_fields().to_vec()));

        Ok(Self {
            path,
            prepared_schema,
            table_schema,
            min_time,
            max_time,
            num_rows,
            metadata_path,
        })
    }

    pub fn with_s3_path(self, s3_path: &S3Object) -> Self {
        Self {
            path: s3_path.get_formatted_key(),
            ..self
        }
    }

    pub fn with_s3_metadata_path(self, s3_path: &S3Object) -> Self {
        Self {
            metadata_path: s3_path.get_formatted_key(),
            ..self
        }
    }
}

#[derive(derive_more::Display, Debug)]
#[display(fmt = "unable to convert prepared metadata")]
pub struct ConversionError;

impl error_stack::Context for ConversionError {}

impl TryFrom<PreparedMetadata> for PreparedFile {
    type Error = error_stack::Report<ConversionError>;

    fn try_from(metadata: PreparedMetadata) -> error_stack::Result<Self, ConversionError> {
        let min_event_time =
            arrow::temporal_conversions::timestamp_ns_to_datetime(metadata.min_time)
                .ok_or(ConversionError)?;
        let max_event_time =
            arrow::temporal_conversions::timestamp_ns_to_datetime(metadata.max_time)
                .ok_or(ConversionError)?;

        Ok(PreparedFile {
            path: metadata.path,
            min_event_time: Some(min_event_time.into()),
            max_event_time: Some(max_event_time.into()),
            num_rows: metadata.num_rows,
            metadata_path: metadata.metadata_path,
        })
    }
}
