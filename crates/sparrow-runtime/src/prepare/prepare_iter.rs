use std::pin::Pin;
use std::sync::PoisonError;
use std::task::Poll;

use anyhow::Context;
use arrow::array::ArrayRef;
use arrow::compute::SortColumn;
use arrow::datatypes::{ArrowPrimitiveType, SchemaRef, TimestampNanosecondType};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use error_stack::{IntoReport, Report, ResultExt};
use futures::stream::BoxStream;
use futures::{Stream, StreamExt};
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::{slice_plan, TableConfig};
use sparrow_core::TableSchema;

use crate::prepare::slice_preparer::SlicePreparer;
use crate::prepare::Error;
use crate::RawMetadata;

use super::column_behavior::ColumnBehavior;
use super::PrepareMetadata;

/// An iterator over prepare batches and corresponding key hash metadata.
///
/// In addition to iterating, this is responsible for the following:
///
/// 1. Inserting the time column, subsort column, and key hash from source to
///    the batch
/// 2. Casts required columns
/// 3. Sorts the record batches by the time column, subsort column, and key hash
/// 4. Computing the key-hash and key batch metadata.
pub struct PrepareIter<'a> {
    reader: BoxStream<'a, Result<RecordBatch, ArrowError>>,
    /// The final schema to produce, including the 3 key columns
    prepared_schema: SchemaRef,
    /// Instructions for creating the resulting batches from a read
    columns: Vec<ColumnBehavior>,
    /// The slice preparer to operate on a per batch basis
    slice_preparer: SlicePreparer,
    /// The metadata tracked during prepare
    metadata: PrepareMetadata,
}

// impl<'a> Stream for PrepareIter<'a> {
//     type Item = Result<(RecordBatch, RecordBatch), PrepareErrorWrapper>;

//     fn poll_next(
//         mut self: Pin<&mut Self>,
//         cx: &mut std::task::Context<'_>,
//     ) -> Poll<Option<Self::Item>> {
//         let reader = &mut self.reader;
//         Pin::new(reader).poll_next(cx).map(|opt| match opt {
//             Some(Ok(batch)) => {
//                 let result = self
//                     .get_mut()
//                     .prepare_next_batch(batch)
//                     .map_err(|err| err.change_context(Error::ReadingBatch).into());
//                 Some(result)
//             }
//             Some(Err(err)) => Some(Err(Report::new(err)
//                 .change_context(Error::ReadingBatch)
//                 .into())),
//             None => None,
//         })
//     }
// }

/// the Stream API works best with Result types that include an actual
/// std::error::Error.  Simple things work okay with arbitrary Results,
/// but things like TryForEach do not.  Since error_stack Errors do
/// not implement std::error::Error, we wrap them in this.
#[derive(Debug)]
pub struct PrepareErrorWrapper(pub Report<Error>);

impl std::fmt::Display for PrepareErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::error::Error for PrepareErrorWrapper {}

impl From<Report<Error>> for PrepareErrorWrapper {
    fn from(error: Report<Error>) -> Self {
        PrepareErrorWrapper(error)
    }
}

impl<T> From<PoisonError<T>> for PrepareErrorWrapper {
    fn from(error: PoisonError<T>) -> Self {
        PrepareErrorWrapper(Report::new(Error::Internal).attach_printable(format!("{:?}", error)))
    }
}

impl From<std::io::Error> for PrepareErrorWrapper {
    fn from(error: std::io::Error) -> Self {
        PrepareErrorWrapper(Report::new(Error::Internal).attach_printable(format!("{:?}", error)))
    }
}

impl From<anyhow::Error> for PrepareErrorWrapper {
    fn from(error: anyhow::Error) -> Self {
        PrepareErrorWrapper(Report::new(Error::Internal).attach_printable(format!("{:?}", error)))
    }
}

impl<'a> std::fmt::Debug for PrepareIter<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrepareIter")
            .field("prepared_schema", &self.prepared_schema)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

impl<'a> PrepareIter<'a> {
    // pub fn stream(
    //     self,
    // ) -> impl Stream<Item = error_stack::Result<(RecordBatch, RecordBatch), Error>> {
    //     // ) -> BoxStream<'a, error_stack::Result<(RecordBatch, RecordBatch), Error>> {
    //     // let reader = self.reader;
    //     async_stream::try_stream! {
    //         let mut reader = self.reader;
    //         while let Some(Ok(batch)) = reader.next().await {
    //             let result: (RecordBatch, RecordBatch) = self
    //                 .prepare_next_batch(batch)
    //                 .await?;
    //                 // .map_err(|err| err.change_context(Error::ReadingBatch));
    //             yield result;
    //         }
    //     }
    //     // .boxed()
    // }

    pub fn stream(
        mut self,
    ) -> impl Stream<Item = error_stack::Result<(RecordBatch, RecordBatch), Error>> {
        let reader = &mut self.reader;
        async_stream::try_stream! {
            while let Some(Ok(batch)) = reader.next().await {
                let result: (RecordBatch, RecordBatch) = self
                    .prepare_next_batch(batch)
                    .await?;
                    // .map_err(|err| err.change_context(Error::ReadingBatch));
                yield result;
            }
        }
    }

    pub fn try_new(
        reader: impl Stream<Item = Result<RecordBatch, ArrowError>> + Send + 'static,
        config: &TableConfig,
        raw_metadata: RawMetadata,
        prepare_hash: u64,
        slice: &Option<slice_plan::Slice>,
    ) -> anyhow::Result<Self> {
        // This is a "hacky" way of adding the 3 key columns. We may just want
        // to manually do that (as part of deprecating `TableSchema`)?
        let prepared_schema = TableSchema::try_from_data_schema(raw_metadata.table_schema.clone())?;
        let prepared_schema = prepared_schema.schema_ref().clone();

        // Add column behaviors for each of the 3 key columns.
        let mut columns = Vec::with_capacity(prepared_schema.fields().len());
        columns.push(ColumnBehavior::try_new_cast(
            &raw_metadata.raw_schema,
            &config.time_column_name,
            &TimestampNanosecondType::DATA_TYPE,
            false,
        )?);
        if let Some(subsort_column_name) = &config.subsort_column_name {
            columns.push(ColumnBehavior::try_new_subsort(
                &raw_metadata.raw_schema,
                subsort_column_name,
            )?);
        } else {
            columns.push(ColumnBehavior::try_default_subsort(prepare_hash)?);
        }

        columns.push(ColumnBehavior::try_new_prepare_entity_key(
            &raw_metadata.raw_schema,
            &config.group_column_name,
            false,
        )?);

        // Add column behaviors for each column.  This means we include the key columns
        // redundantly, but cleaning that up is a big refactor.
        // See https://github.com/riptano/kaskada/issues/90
        for field in raw_metadata.table_schema.fields() {
            columns.push(ColumnBehavior::try_cast_or_reference_or_null(
                &raw_metadata.raw_schema,
                field,
            )?);
        }

        // we've already checked that the group column exists so we can just unwrap it here
        let (source_index, field) = raw_metadata
            .raw_schema
            .column_with_name(&config.group_column_name)
            .unwrap();
        let slice_preparer =
            SlicePreparer::try_new(source_index, field.data_type().clone(), slice.as_ref())?;

        let (_, entity_key_column) = raw_metadata
            .raw_schema
            .column_with_name(&config.group_column_name)
            .with_context(|| "")?;

        let metadata = PrepareMetadata::new(entity_key_column.data_type().clone());

        Ok(Self {
            reader: reader.boxed(),
            prepared_schema,
            columns,
            slice_preparer,
            metadata,
        })
    }

    /// Convert a read batch to the merged batch format.
    async fn prepare_next_batch(
        &mut self,
        read_batch: RecordBatch,
    ) -> error_stack::Result<(RecordBatch, RecordBatch), Error> {
        // 1. Slicing may reduce the number of entities to operate and sort on.
        let read_batch = self.slice_preparer.slice_batch(read_batch)?;

        // 2. Prepare each of the columns by getting the column behavior result
        let mut prepared_columns = Vec::new();
        for c in self.columns.iter_mut() {
            let result = c
                .get_result(Some(&mut self.metadata), None, &read_batch)
                .await?;
            prepared_columns.push(result);
        }

        // 3. Pull out the time, subsort and key hash columns to sort the record batch
        let time_column = &prepared_columns[0];
        let subsort_column = &prepared_columns[1];
        let key_hash_column = &prepared_columns[2];
        let sorted_indices = arrow::compute::lexsort_to_indices(
            &[
                SortColumn {
                    values: time_column.clone(),
                    options: None,
                },
                SortColumn {
                    values: subsort_column.clone(),
                    options: None,
                },
                SortColumn {
                    values: key_hash_column.clone(),
                    options: None,
                },
            ],
            None,
        )
        .into_report()
        .change_context(Error::SortingBatch)?;

        // Produce the fully ordered record batch by taking the indices out from the
        // columns
        let prepared_columns: Vec<_> = prepared_columns
            .iter()
            .map(|column| arrow::compute::take(column.as_ref(), &sorted_indices, None))
            .try_collect()
            .into_report()
            .change_context(Error::Internal)?;

        let batch = RecordBatch::try_new(self.prepared_schema.clone(), prepared_columns.clone())
            .into_report()
            .change_context(Error::Internal)?;
        let metadata = self.metadata.get_flush_metadata()?;
        Ok((batch, metadata))
    }
}
