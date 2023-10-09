use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute::SortColumn;
use arrow::datatypes::{ArrowPrimitiveType, DataType, SchemaRef, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use arrow_array::{Array, Float64Array, Int64Array, Scalar};
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::FuturesUnordered;
use futures::{StreamExt, TryStreamExt};
use sparrow_api::kaskada::v1alpha::{PreparedFile, SourceData, TableConfig};
use std::sync::Arc;
use uuid::Uuid;

use crate::stores::{ObjectStoreRegistry, ObjectStoreUrl};
use crate::PreparedMetadata;

use super::{prepared_batches, write_parquet};

/// For now, this is a temporary location for the prepared files.
/// In the future, we'll want to move this path to a managed cache
/// so we can reuse state.
const KASKADA_PATH: &str = "kaskada";
const PREPARED_FILE_PREFIX: &str = "part";

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "batch missing required column '{_0}'")]
    BatchMissingRequiredColumn(String),
    #[display(fmt = "failed to convert time column from type {_0:?} to timestamp_ns")]
    ConvertTime(DataType),
    #[display(fmt = "failed to convert subsort column from type {_0:?} to uint64")]
    ConvertSubsort(DataType),
    #[display(fmt = "failed to hash key array")]
    HashingKeyArray,
    #[display(fmt = "failed to create batch")]
    CreatingBatch,
    #[display(fmt = "failed to sort batch")]
    SortingBatch,
    #[display(fmt = "unrecognized time unit")]
    UnrecognizedTimeUnit(String),
    #[display(fmt = "invalid url '{_0:?}'")]
    InvalidUrl(String),
    #[display(fmt = "internal error")]
    Internal,
}

impl error_stack::Context for Error {}

pub struct Preparer {
    prepared_schema: SchemaRef,
    table_config: Arc<TableConfig>,
    next_subsort: AtomicU64,
    time_multiplier: Option<Scalar<ArrayRef>>,
    object_stores: Arc<ObjectStoreRegistry>,
}

impl Preparer {
    /// Create a new prepare produce data with the given schema.
    pub fn new(
        table_config: Arc<TableConfig>,
        prepared_schema: SchemaRef,
        prepare_hash: u64,
        time_unit: Option<&str>,
        object_stores: Arc<ObjectStoreRegistry>,
    ) -> error_stack::Result<Self, Error> {
        let time_type = prepared_schema
            .field_with_name(&table_config.time_column_name)
            .into_report()
            .change_context_lazy(|| {
                Error::BatchMissingRequiredColumn(table_config.time_column_name.clone())
            })?
            .data_type();
        let time_multiplier = time_multiplier(time_type, time_unit)?;
        Ok(Self {
            prepared_schema,
            table_config,
            next_subsort: prepare_hash.into(),
            time_multiplier,
            object_stores,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.prepared_schema.clone()
    }

    /// Prepare a parquet file.
    ///
    /// - This computes and adds the key columns.
    /// - This sorts the files by time, subsort and key hash.
    /// - This adds or casts columns as needed.
    /// - This produces multiple parts if the input file is large.
    /// - This produces metadata files alongside data files.
    /// Parameters:
    /// - `to_prepare`: The path to the parquet file to prepare.
    /// - `prepare_prefix`: The prefix to write prepared files to.
    pub async fn prepare_parquet(
        &self,
        to_prepare: ObjectStoreUrl,
        prepare_prefix: &ObjectStoreUrl,
    ) -> error_stack::Result<Vec<PreparedFile>, Error> {
        // TODO: Support Slicing
        let output_url = self
            .prepared_output_url_prefix(prepare_prefix)
            .change_context_lazy(|| Error::InvalidUrl(prepare_prefix.to_string()))?;

        let object_store = self
            .object_stores
            .object_store(&output_url)
            .change_context(Error::Internal)?;

        let extension = Some("parquet");
        let source_data = SourceData {
            source: Some(
                SourceData::try_from_url(to_prepare.url(), extension)
                    .into_report()
                    .change_context(Error::Internal)?,
            ),
        };

        // TODO: Slicing
        // TODO: Opportunity to concatenate batches to reduce the number of prepared files.
        let mut prepare_stream = prepared_batches(
            &self.object_stores,
            &source_data,
            &self.table_config,
            &None,
            self.time_multiplier.clone(),
        )
        .await
        .change_context(Error::Internal)?
        .enumerate();

        let mut prepared_files = Vec::new();
        let mut uploads = FuturesUnordered::new();
        while let Some((n, next)) = prepare_stream.next().await {
            let (data, metadata) = next.change_context(Error::Internal)?;

            let data_url = output_url
                .join(&format!("{PREPARED_FILE_PREFIX}-{n}.parquet"))
                .change_context(Error::Internal)?;
            let metadata_url = output_url
                .join(&format!("{PREPARED_FILE_PREFIX}-{n}-metadata.parquet"))
                .change_context(Error::Internal)?;

            // Create the prepared file via PreparedMetadata.
            // TODO: We could probably do this directly, eliminating the PreparedMetadata struct.
            let prepared_file: PreparedFile = PreparedMetadata::try_from_data(
                data_url.to_string(),
                &data,
                metadata_url.to_string(),
            )
            .into_report()
            .change_context(Error::Internal)?
            .try_into()
            .change_context(Error::Internal)?;
            prepared_files.push(prepared_file);

            uploads.push(write_parquet(data, data_url, object_store.clone()));
            uploads.push(write_parquet(metadata, metadata_url, object_store.clone()));
        }

        // Wait for the uploads.
        while let Some(upload) = uploads.try_next().await.change_context(Error::Internal)? {
            tracing::info!("Finished uploading {upload}");
        }

        Ok(prepared_files)
    }

    /// Prepare a batch of data.
    ///
    /// - This computes and adds the key columns.
    /// - This sorts the batch by time, subsort and key hash.
    /// - This adds or casts columns as needed.
    pub fn prepare_batch(&self, batch: RecordBatch) -> error_stack::Result<RecordBatch, Error> {
        prepare_batch(
            &batch,
            &self.table_config,
            self.prepared_schema.clone(),
            &self.next_subsort,
            self.time_multiplier.as_ref(),
        )
    }

    /// Creates the output url prefix to use for prepared files.
    ///
    /// e.g. for osx: file:///<dir>/<KASKADA_PATH>/tables/<table_uuid>/prepared/<uuid>/
    ///      for s3:  s3://<path>/<KASKADA_PATH>/tables/<table_uuid>/prepared/<uuid>/
    fn prepared_output_url_prefix(
        &self,
        prefix: &ObjectStoreUrl,
    ) -> error_stack::Result<ObjectStoreUrl, crate::stores::registry::Error> {
        let uuid = Uuid::new_v4();
        let url = prefix
            .join(KASKADA_PATH)?
            .join("tables")?
            .join(&self.table_config.uuid.to_string())?
            .join("prepared")?
            .join(&uuid.to_string())?
            .ensure_directory()?;

        Ok(url)
    }
}

pub fn prepare_batch(
    batch: &RecordBatch,
    table_config: &TableConfig,
    prepared_schema: SchemaRef,
    next_subsort: &AtomicU64,
    time_multiplier: Option<&Scalar<ArrayRef>>,
) -> error_stack::Result<RecordBatch, Error> {
    let time_column_name = table_config.time_column_name.clone();
    let subsort_column_name = table_config.subsort_column_name.clone();
    let key_column_name = table_config.group_column_name.clone();

    let time = get_required_column(batch, &time_column_name)?;
    let time = cast_to_timestamp(time, time_multiplier)?;

    let num_rows = batch.num_rows();
    let subsort = if let Some(subsort_column_name) = subsort_column_name.as_ref() {
        let subsort = get_required_column(batch, subsort_column_name)?;
        arrow::compute::cast(subsort.as_ref(), &DataType::UInt64)
            .into_report()
            .change_context_lazy(|| Error::ConvertSubsort(subsort.data_type().clone()))?
    } else {
        let subsort_start = next_subsort.fetch_add(num_rows as u64, Ordering::SeqCst);
        let subsort: UInt64Array = (subsort_start..).take(num_rows).collect();
        Arc::new(subsort)
    };

    let key = get_required_column(batch, &key_column_name)?;
    let key_hash =
        sparrow_arrow::hash::hash(key.as_ref()).change_context(Error::HashingKeyArray)?;
    let key_hash: ArrayRef = Arc::new(key_hash);

    let mut columns = Vec::with_capacity(prepared_schema.fields().len());

    let indices = arrow::compute::lexsort_to_indices(
        &[
            SortColumn {
                values: time.clone(),
                options: None,
            },
            SortColumn {
                values: subsort.clone(),
                options: None,
            },
            SortColumn {
                values: key_hash.clone(),
                options: None,
            },
        ],
        None,
    )
    .into_report()
    .change_context(Error::SortingBatch)?;

    let sort = |array: &ArrayRef| {
        arrow::compute::take(array.as_ref(), &indices, None)
            .into_report()
            .change_context(Error::SortingBatch)
    };
    columns.push(sort(&time)?);
    columns.push(sort(&subsort)?);
    columns.push(sort(&key_hash)?);

    // TODO: Slicing?
    for field in prepared_schema.fields().iter().skip(3) {
        let column = if let Some(column) = batch.column_by_name(field.name()) {
            sort(column)?
        } else {
            arrow::array::new_null_array(field.data_type(), num_rows)
        };
        columns.push(column)
    }
    let prepared = RecordBatch::try_new(prepared_schema.clone(), columns)
        .into_report()
        .change_context(Error::CreatingBatch)?;
    Ok(prepared)
}

fn get_required_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> error_stack::Result<&'a ArrayRef, Error> {
    batch
        .column_by_name(name)
        .ok_or_else(|| error_stack::report!(Error::BatchMissingRequiredColumn(name.to_owned())))
}

fn time_multiplier(
    time_type: &DataType,
    time_unit: Option<&str>,
) -> error_stack::Result<Option<Scalar<ArrayRef>>, Error> {
    let multiplier = match time_unit.unwrap_or("ns") {
        "ns" => return Ok(None),
        "us" => 1_000,
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        unrecognized => error_stack::bail!(Error::UnrecognizedTimeUnit(unrecognized.to_owned())),
    };

    match time_type {
        DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64 => {
            // We'll multiply the i64 values, so create that kind of scalar.
            Ok(Some(Scalar::new(Arc::new(Int64Array::from(vec![
                multiplier,
            ])))))
        }
        DataType::Utf8 | DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
            Ok(None)
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            // We'll multiply the f64 values, so create that kind of scalar.
            Ok(Some(Scalar::new(Arc::new(Float64Array::from(vec![
                multiplier as f64,
            ])))))
        }
        other => {
            error_stack::bail!(Error::ConvertTime(other.clone()))
        }
    }
}

fn cast_to_timestamp(
    time: &ArrayRef,
    time_multiplier: Option<&Scalar<ArrayRef>>,
) -> error_stack::Result<ArrayRef, Error> {
    match time.data_type() {
        DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64 => {
            numeric_to_timestamp::<arrow_array::types::Int64Type>(time.as_ref(), time_multiplier)
        }
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            numeric_to_timestamp::<arrow_array::types::Float64Type>(time.as_ref(), time_multiplier)
        }
        DataType::Utf8 | DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _) => {
            arrow::compute::cast(time.as_ref(), &TimestampNanosecondType::DATA_TYPE)
                .into_report()
                .change_context_lazy(|| Error::ConvertTime(time.data_type().clone()))
        }
        other => {
            error_stack::bail!(Error::ConvertTime(other.clone()))
        }
    }
}

fn numeric_to_timestamp<T: arrow_array::ArrowNumericType>(
    raw: &dyn Array,
    time_multiplier: Option<&Scalar<ArrayRef>>,
) -> error_stack::Result<ArrayRef, Error> {
    let error = || Error::ConvertTime(raw.data_type().clone());

    // First, cast to `T::DATA_TYPE`.
    let time = arrow::compute::cast(raw, &T::DATA_TYPE)
        .into_report()
        .change_context_lazy(error)?;

    // Perform the multiplication on the `T::DATA_TYPE`.
    // Do this before conversion to int64 so we don't lose f64 precision.
    let time = if let Some(time_multiplier) = time_multiplier {
        arrow_arith::numeric::mul(&time, time_multiplier)
            .into_report()
            .change_context_lazy(error)?
    } else {
        time
    };

    // Convert to int64 (if necessary).
    let time = if T::DATA_TYPE == DataType::Int64 {
        time
    } else {
        arrow::compute::cast(time.as_ref(), &DataType::Int64)
            .into_report()
            .change_context_lazy(error)?
    };

    // Convert from int64 to nanosecond. This expects the units to already be converted, which they are.
    arrow::compute::cast(time.as_ref(), &TimestampNanosecondType::DATA_TYPE)
        .into_report()
        .change_context_lazy(error)
}
