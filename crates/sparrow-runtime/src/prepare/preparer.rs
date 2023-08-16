use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute::SortColumn;
use arrow::datatypes::{ArrowPrimitiveType, DataType, SchemaRef, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
use arrow_array::Array;
use error_stack::{IntoReport, ResultExt};

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
}

impl error_stack::Context for Error {}

pub struct Preparer {
    prepared_schema: SchemaRef,
    time_column_name: String,
    subsort_column_name: Option<String>,
    next_subsort: u64,
    key_column_name: String,
    time_multiplier: Option<i64>,
}

impl Preparer {
    /// Create a new prepare produce data with the given schema.
    pub fn new(
        time_column_name: String,
        subsort_column_name: Option<String>,
        key_column_name: String,
        prepared_schema: SchemaRef,
        prepare_hash: u64,
        time_unit: Option<&str>,
    ) -> error_stack::Result<Self, Error> {
        let time_multiplier = time_multiplier(time_unit)?;
        Ok(Self {
            prepared_schema,
            time_column_name,
            subsort_column_name,
            next_subsort: prepare_hash,
            key_column_name,
            time_multiplier,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.prepared_schema.clone()
    }

    /// Prepare a batch of data.
    ///
    /// - This computes and adds the key columns.
    /// - This sorts the batch by time, subsort and key hash.
    /// - This adds or casts columns as needed.
    ///
    /// Self is mutated as necessary to ensure the `subsort` column is increasing, if
    /// it is added.
    pub fn prepare_batch(&mut self, batch: RecordBatch) -> error_stack::Result<RecordBatch, Error> {
        let time = get_required_column(&batch, &self.time_column_name)?;
        let time = cast_to_timestamp(time, self.time_multiplier)?;

        let num_rows = batch.num_rows();
        let subsort = if let Some(subsort_column_name) = self.subsort_column_name.as_ref() {
            let subsort = get_required_column(&batch, subsort_column_name)?;
            arrow::compute::cast(time.as_ref(), &DataType::UInt64)
                .into_report()
                .change_context_lazy(|| Error::ConvertSubsort(subsort.data_type().clone()))?
        } else {
            let subsort: UInt64Array = (self.next_subsort..).take(num_rows).collect();
            self.next_subsort += num_rows as u64;
            Arc::new(subsort)
        };

        let key = get_required_column(&batch, &self.key_column_name)?;
        let key_hash =
            sparrow_arrow::hash::hash(key.as_ref()).change_context(Error::HashingKeyArray)?;
        let key_hash: ArrayRef = Arc::new(key_hash);

        let mut columns = Vec::with_capacity(self.prepared_schema.fields().len());

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
        for field in self.prepared_schema.fields().iter().skip(3) {
            let column = if let Some(column) = batch.column_by_name(field.name()) {
                sort(column)?
            } else {
                arrow::array::new_null_array(field.data_type(), num_rows)
            };
            columns.push(column)
        }
        let prepared = RecordBatch::try_new(self.prepared_schema.clone(), columns)
            .into_report()
            .change_context(Error::CreatingBatch)?;
        Ok(prepared)
    }
}

fn get_required_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
) -> error_stack::Result<&'a ArrayRef, Error> {
    batch
        .column_by_name(name)
        .ok_or_else(|| error_stack::report!(Error::BatchMissingRequiredColumn(name.to_owned())))
}

fn time_multiplier(time_unit: Option<&str>) -> error_stack::Result<Option<i64>, Error> {
    match time_unit.unwrap_or("ns") {
        "ns" => Ok(None),
        "us" => Ok(Some(1_000)),
        "ms" => Ok(Some(1_000_000)),
        "s" => Ok(Some(1_000_000_000)),
        unrecognized => error_stack::bail!(Error::UnrecognizedTimeUnit(unrecognized.to_owned())),
    }
}

fn cast_to_timestamp(
    time: &ArrayRef,
    time_multiplier: Option<i64>,
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
            numeric_to_timestamp::<arrow_array::types::Float64Type>(
                time.as_ref(),
                time_multiplier.map(|m| m as f64),
            )
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
    time_multiplier: Option<T::Native>,
) -> error_stack::Result<ArrayRef, Error> {
    let error = || Error::ConvertTime(raw.data_type().clone());

    // First, cast to `T::DATA_TYPE`.
    let time = arrow::compute::cast(raw, &T::DATA_TYPE)
        .into_report()
        .change_context_lazy(error)?;

    // Perform the multiplication on the `T::DATA_TYPE`.
    // Do this before conversion to int64 so we don't lose f64 precision.
    let time = if let Some(time_multiplier) = time_multiplier {
        arrow::compute::multiply_scalar_dyn::<T>(time.as_ref(), time_multiplier)
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
