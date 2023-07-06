use std::sync::Arc;

use arrow::array::TimestampNanosecondArray;
use arrow::datatypes::{ArrowPrimitiveType, Schema, SchemaRef, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;

use sparrow_api::kaskada::v1alpha::PreparedFile;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_core::TableSchema;

#[non_exhaustive]
#[derive(Clone, Debug, PartialEq)]
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

impl PreparedMetadata {
    pub fn try_from_data(
        data_path: String,
        data: &RecordBatch,
        metadata_path: String,
    ) -> anyhow::Result<Self> {
        let prepared_schema = data.schema();

        anyhow::ensure!(
            prepared_schema.field(0).name() == "_time",
            "First column of prepared files must be '_time'"
        );
        anyhow::ensure!(
            prepared_schema.field(0).data_type() == &TimestampNanosecondType::DATA_TYPE,
            "First column of prepared files must be TimestmapNanosecondType"
        );

        // Compute the time statistics directly from the data.
        //
        // TODO: We could instead just get this from the parquet metadata.
        let time = data.column(0);
        let time: &TimestampNanosecondArray = downcast_primitive_array(time.as_ref())?;

        let num_rows = data.num_rows() as i64;
        // Time column is sorted (since the file is already prepared).
        let min_time = time
            .values()
            .first()
            .copied()
            // Empty files contain no stats. We default to assuming the min time.
            .unwrap_or(i64::MIN);
        let max_time = time
            .values()
            .last()
            .copied()
            // Empty files contain no stats. We default to assuming the min time.
            .unwrap_or(i64::MIN);
        tracing::info!("Determined times {min_time} to {max_time} for file '{data_path}'");

        Self::try_from_prepared_schema(
            data_path,
            prepared_schema,
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

    pub fn with_path(self, path: String) -> Self {
        Self { path, ..self }
    }

    pub fn with_metadata_path(self, path: String) -> Self {
        Self {
            metadata_path: path,
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
