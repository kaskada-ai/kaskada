use std::sync::Arc;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute::SortColumn;
use arrow::datatypes::{ArrowPrimitiveType, DataType, SchemaRef, TimestampNanosecondType};
use arrow::record_batch::RecordBatch;
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
}

impl error_stack::Context for Error {}

pub struct Preparer {
    prepared_schema: SchemaRef,
    time_column_name: String,
    subsort_column_name: Option<String>,
    next_subsort: u64,
    key_column_name: String,
}

impl Preparer {
    /// Create a new prepare produce data with the given schema.
    pub fn new(
        time_column_name: String,
        subsort_column_name: Option<String>,
        key_column_name: String,
        prepared_schema: SchemaRef,
        prepare_hash: u64,
    ) -> Self {
        Self {
            prepared_schema,
            time_column_name,
            subsort_column_name,
            next_subsort: prepare_hash,
            key_column_name,
        }
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
        let time = arrow::compute::cast(time.as_ref(), &TimestampNanosecondType::DATA_TYPE)
            .into_report()
            .change_context_lazy(|| Error::ConvertTime(time.data_type().clone()))?;

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
