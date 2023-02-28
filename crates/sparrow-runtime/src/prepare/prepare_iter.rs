use std::borrow::BorrowMut;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use arrow::array::{Array, ArrayRef, UInt64Array};
use arrow::compute::SortColumn;
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Field, Schema, SchemaRef, TimestampNanosecondType,
};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use hashbrown::HashSet;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::{slice_plan, TableConfig};
use sparrow_core::utils::make_null_array;
use sparrow_core::{context_code, TableSchema};
use sparrow_kernels::order_preserving_cast_to_u64;

use crate::prepare::slice_preparer::SlicePreparer;
use crate::prepare::Error;
use crate::RawMetadata;

/// An iterator over prepare batches and corresponding key hash metadata.
///
/// In addition to iterating, this is responsible for the following:
///
/// 1. Inserting the time column, subsort column, and key hash from source to
///    the batch
/// 2. Casts required columns
/// 3. Sorts the record batches by the time column, subsort column, and key hash
/// 4. Computing the key-hash and key batch metadata.
pub struct PrepareIter {
    reader: Box<dyn Iterator<Item = Result<RecordBatch, ArrowError>> + 'static>,
    /// The final schema to produce, including the 3 key columns.
    prepared_schema: SchemaRef,
    /// Instructions for creating the resulting batches from a reade
    columns: Vec<ColumnBehavior>,
    /// The slice preparer to operate on a per batch basis
    slice_preparer: SlicePreparer,
    /// The metadata tracked during prepare
    metadata: PrepareMetadata,
}

impl fallible_iterator::FallibleIterator for PrepareIter {
    type Item = (RecordBatch, RecordBatch);

    type Error = error_stack::Report<Error>;

    fn next(&mut self) -> error_stack::Result<Option<Self::Item>, Error> {
        if let Some(next) = self.reader.next() {
            let next = next.into_report().change_context(Error::ReadingBatch)?;
            let prepare_batch = self.prepare_next_batch(next)?;
            Ok(Some(prepare_batch))
        } else {
            Ok(None)
        }
    }
}

impl std::fmt::Debug for PrepareIter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrepareIter")
            .field("prepared_schema", &self.prepared_schema)
            .field("columns", &self.columns)
            .finish_non_exhaustive()
    }
}

impl PrepareIter {
    pub fn try_new(
        reader: impl Iterator<Item = Result<RecordBatch, ArrowError>> + 'static,
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

        columns.push(ColumnBehavior::try_new_entity_key(
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
            SlicePreparer::try_new(source_index, field.data_type().clone(), slice)?;

        let (_, entity_key_column) = raw_metadata
            .raw_schema
            .column_with_name(&config.group_column_name)
            .with_context(|| "")?;

        let metadata = PrepareMetadata::new(entity_key_column.data_type().clone());

        Ok(Self {
            reader: Box::new(reader),
            prepared_schema,
            columns,
            slice_preparer,
            metadata,
        })
    }

    /// Convert a read batch to the merged batch format.
    fn prepare_next_batch(
        &mut self,
        read_batch: RecordBatch,
    ) -> error_stack::Result<(RecordBatch, RecordBatch), Error> {
        // 1. Slicing may reduce the number of entities to operate and sort on.
        let read_batch = self.slice_preparer.slice_batch(read_batch)?;

        // 2. Prepare each of the columns by getting the column behavior result
        let prepared_columns: Vec<_> = self
            .columns
            .iter_mut()
            .map(|column| column.get_result(&mut self.metadata, &read_batch))
            .try_collect()?;

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

pub struct PrepareMetadata {
    metadata_schema: SchemaRef,
    previous_keys: HashSet<u64>,
    metadata_batches: Vec<RecordBatch>,
}

impl PrepareMetadata {
    fn new(entity_key_type: DataType) -> PrepareMetadata {
        let metadata_schema: SchemaRef = {
            Arc::new(Schema::new(vec![
                Field::new("_hash", DataType::UInt64, false),
                Field::new("_entity_key", entity_key_type, true),
            ]))
        };
        PrepareMetadata {
            metadata_schema,
            previous_keys: HashSet::new(),
            metadata_batches: vec![],
        }
    }

    fn add_entity_keys(
        &mut self,
        hashes: Arc<dyn Array>,
        entity_keys: Arc<dyn Array>,
    ) -> anyhow::Result<()> {
        let new_batch =
            RecordBatch::try_new(self.metadata_schema.clone(), vec![hashes, entity_keys])
                .with_context(|| "unable to add entity keys to metadata")?;
        self.metadata_batches.push(new_batch);
        Ok(())
    }

    fn get_flush_metadata(&mut self) -> error_stack::Result<RecordBatch, Error> {
        let metadata =
            arrow::compute::concat_batches(&self.metadata_schema, &self.metadata_batches)
                .into_report()
                .change_context(Error::Internal);
        self.previous_keys.clear();
        self.metadata_batches.clear();
        metadata
    }
}

/// Defines how each column in the resulting prepared batch
/// is computed.
#[derive(Debug)]
pub enum ColumnBehavior {
    /// Cast the given column to the given data type.
    Cast {
        index: usize,
        data_type: DataType,
        nullable: bool,
    },
    /// Perform an "order preserving" cast from a primitive number to u64.
    OrderPreservingCastToU64 { index: usize, nullable: bool },
    /// Reference the given column.
    Reference { index: usize, nullable: bool },
    /// Hash the given column.
    EntityKey { index: usize, nullable: bool },
    /// A random column
    SequentialU64 { next_offset: u64 },
    /// Create a column of nulls.
    ///
    /// The `DataType` indicates the type of column to produce.
    Null(DataType),
}

impl ColumnBehavior {
    /// Create a column behavior that references the given field as the
    /// specified type.
    ///
    /// This may be a reference (if the source already has that type) or a cast.
    ///
    /// # Errors
    /// Internal error if the source field doesn't exist or is not convertible
    /// to the given type.
    fn try_new_cast(
        source_schema: &SchemaRef,
        source_name: &str,
        to_type: &DataType,
        nullable: bool,
    ) -> anyhow::Result<Self> {
        let (source_index, source_field) = source_schema
            .column_with_name(source_name)
            .with_context(|| {
                context_code!(
                    tonic::Code::Internal,
                    "Required column '{}' not present in schema {:?}",
                    source_name,
                    source_schema
                )
            })?;

        match (source_field.data_type(), to_type) {
            (from, to) if from == to => Ok(Self::Reference {
                index: source_index,
                nullable,
            }),
            (DataType::Timestamp(_from_unit_, Some(_)), DataType::Timestamp(to_unit, None)) => {
                Ok(Self::Cast {
                    index: source_index,
                    data_type: DataType::Timestamp(to_unit.clone(), None),
                    nullable,
                })
            }
            (from, to) if arrow::compute::can_cast_types(from, to) => Ok(Self::Cast {
                index: source_index,
                data_type: to_type.clone(),
                nullable,
            }),
            (_, _) => Err(anyhow!(
                "Expected column '{}' to be castable to {:?}, but {:?} was not",
                source_field.name(),
                to_type,
                source_field.data_type(),
            )
            .context(tonic::Code::Internal)),
        }
    }

    /// Create a column behavior specifically for the subsort.
    ///
    /// This is a bit different from other columns since we want to attempt
    /// to preserve the order.
    ///
    /// # Errors
    /// Internal error if the source field doesn't exist or is not valid
    /// as the subsort.
    fn try_new_subsort(source_schema: &SchemaRef, source_name: &str) -> anyhow::Result<Self> {
        let (source_index, source_field) = source_schema
            .column_with_name(source_name)
            .with_context(|| {
                context_code!(
                    tonic::Code::Internal,
                    "Required column '{}' not present in schema {:?}",
                    source_name,
                    source_schema
                )
            })?;

        match source_field.data_type() {
            DataType::UInt64 => Ok(Self::Reference {
                index: source_index,
                nullable: false,
            }),
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 => Ok(Self::Cast {
                index: source_index,
                data_type: DataType::UInt64,
                nullable: false,
            }),

            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                Ok(Self::OrderPreservingCastToU64 {
                    index: source_index,
                    nullable: false,
                })
            }

            _ => Err(anyhow!(
                "Expected subsort column '{}' to be numeric but was {:?}",
                source_field.name(),
                source_field.data_type(),
            )
            .context(tonic::Code::Internal)),
        }
    }

    /// Create a column behavior that generates a random u64.
    pub fn try_default_subsort(prepare_hash: u64) -> anyhow::Result<Self> {
        Ok(Self::SequentialU64 {
            next_offset: prepare_hash,
        })
    }

    /// Create a column behavior that hashes the given field (and index) to
    /// `u64`. This is only used for the entity key.
    ///
    /// # Errors
    /// Internal error if the source field doesn't exist.
    pub fn try_new_entity_key(
        source_schema: &SchemaRef,
        source_name: &str,
        nullable: bool,
    ) -> anyhow::Result<Self> {
        let (source_index, _) = source_schema
            .column_with_name(source_name)
            .with_context(|| {
                context_code!(
                    tonic::Code::Internal,
                    "Required column '{}' not present in schema {:?}",
                    source_name,
                    source_schema
                )
            })?;

        Ok(Self::EntityKey {
            index: source_index,
            nullable,
        })
    }

    /// Create a behavior that projects a field from the source schema to the
    /// result field.
    ///
    /// If the `result_field` doesn't exist in the `source_schema` the result
    /// is a column of nulls.
    ///
    /// In the special case of a `Timestamp` with a time zone, this will cast to
    /// a `Timestamp` with no time zone.
    ///
    /// # Errors
    /// Internal error if the type of the column in the source schema is
    /// different than in the result schema.
    fn try_cast_or_reference_or_null(
        source_schema: &SchemaRef,
        result_field: &Field,
    ) -> anyhow::Result<Self> {
        if let Some((column, source_field)) = source_schema.column_with_name(result_field.name()) {
            match (source_field.data_type(), result_field.data_type()) {
                (DataType::Timestamp(_, Some(_)), DataType::Timestamp(to_unit, None)) => {
                    Ok(Self::Cast {
                        index: column,
                        data_type: DataType::Timestamp(to_unit.clone(), None),
                        nullable: true,
                    })
                }
                (source_type, expected_type) if source_type == expected_type => {
                    Ok(Self::Reference {
                        index: column,
                        nullable: true,
                    })
                }
                (source_type, expected_type) => Err(anyhow!(
                    "Unable to get field '{}' as type {:?} from file containing {:?}",
                    result_field.name(),
                    expected_type,
                    source_type,
                )
                .context(tonic::Code::Internal)),
            }
        } else {
            anyhow::ensure!(
                result_field.is_nullable(),
                "Result field must be nullable if absent in source, but was {:?}",
                result_field
            );
            Ok(Self::Null(result_field.data_type().clone()))
        }
    }

    pub fn get_result(
        &mut self,
        metadata: &mut PrepareMetadata,
        batch: &RecordBatch,
    ) -> error_stack::Result<ArrayRef, Error> {
        let result = match self {
            ColumnBehavior::Cast {
                index,
                data_type,
                nullable,
            } => {
                let column = batch.column(*index);
                error_stack::ensure!(
                    *nullable || column.null_count() == 0,
                    Error::NullInNonNullableColumn {
                        field: batch.schema().field(*index).name().to_owned(),
                        null_count: column.null_count()
                    }
                );
                arrow::compute::cast(column, data_type)
                    .into_report()
                    .change_context(Error::PreparingColumn)?
            }
            ColumnBehavior::OrderPreservingCastToU64 { index, nullable } => {
                let column = batch.column(*index);
                error_stack::ensure!(
                    *nullable || column.null_count() == 0,
                    Error::NullInNonNullableColumn {
                        field: batch.schema().field(*index).name().to_owned(),
                        null_count: column.null_count()
                    }
                );

                order_preserving_cast_to_u64(column)
                    .into_report()
                    .change_context(Error::PreparingColumn)?
            }
            ColumnBehavior::Reference { index, nullable } => {
                let column = batch.column(*index);
                error_stack::ensure!(
                    *nullable || column.null_count() == 0,
                    Error::NullInNonNullableColumn {
                        field: batch.schema().field(*index).name().to_owned(),
                        null_count: column.null_count()
                    }
                );
                column.clone()
            }
            ColumnBehavior::EntityKey { index, nullable } => {
                let column = batch.column(*index);
                error_stack::ensure!(
                    *nullable || column.null_count() == 0,
                    Error::NullInNonNullableColumn {
                        field: batch.schema().field(*index).name().to_owned(),
                        null_count: column.null_count()
                    }
                );
                let previous_keys = metadata.previous_keys.borrow_mut();
                // 1. Hash the current entity key column to a UInt64Array
                let entity_column = sparrow_kernels::hash::hash(column)
                    .into_report()
                    .change_context(Error::PreparingColumn)?;
                // 2. Convert the hash column specifically to a UInt64Array from a dyn array.
                let indices: UInt64Array = entity_column
                    .iter()
                    .flatten()
                    .enumerate()
                    .filter_map(|(index, key_hash)| {
                        if previous_keys.insert(key_hash) {
                            Some(index as u64)
                        } else {
                            None
                        }
                    })
                    .collect();
                let new_hash_keys = arrow::compute::take(&entity_column, &indices, None)
                    .into_report()
                    .change_context(Error::PreparingColumn)?;
                let new_keys = arrow::compute::take(column, &indices, None)
                    .into_report()
                    .change_context(Error::PreparingColumn)?;
                metadata
                    .add_entity_keys(new_hash_keys, new_keys)
                    .into_report()
                    .change_context(Error::PreparingColumn)?;
                Arc::new(entity_column)
            }
            ColumnBehavior::Null(result_type) => make_null_array(result_type, batch.num_rows()),
            ColumnBehavior::SequentialU64 { next_offset } => {
                // 1. The result is going to be [next_offset, next_offset + length).
                let length = batch.num_rows() as u64;
                // TODO: There is a potential u64 overflow. If an overflow will happen
                // the subsort will start at 0 to length.
                let (start, end) = if let Some(end) = next_offset.checked_add(length) {
                    (*next_offset, end)
                } else {
                    (0, length)
                };

                let result = UInt64Array::from_iter_values(start..end);

                // 2. Update next_offset so the *next batch* gets new values.
                *next_offset = end;

                Arc::new(result)
            }
        };
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray, TimestampNanosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use static_init::dynamic;

    use super::{ColumnBehavior, PrepareMetadata};

    #[dynamic]
    static COMPLETE_SCHEMA: SchemaRef = {
        Arc::new(Schema::new(vec![
            Field::new(
                "_time",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new("_subsort", DataType::UInt64, false),
            Field::new("_key_hash", DataType::UInt64, false),
            Field::new("a", DataType::Int64, true),
        ]))
    };

    fn make_test_batch(num_rows: usize) -> RecordBatch {
        let time = TimestampNanosecondArray::from_iter_values(0..num_rows as i64);
        let subsort = UInt64Array::from_iter_values(0..num_rows as u64);
        let key = UInt64Array::from_iter_values(0..num_rows as u64);
        let a = Int64Array::from_iter_values(0..num_rows as i64);

        RecordBatch::try_new(
            COMPLETE_SCHEMA.clone(),
            vec![
                Arc::new(time),
                Arc::new(subsort),
                Arc::new(key),
                Arc::new(a),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_sequential_u64_zero() {
        let mut behavior = ColumnBehavior::SequentialU64 { next_offset: 0 };
        assert_eq!(
            behavior
                .get_result(
                    &mut PrepareMetadata::new(DataType::UInt64.clone()),
                    &make_test_batch(5)
                )
                .unwrap()
                .as_ref(),
            &UInt64Array::from(vec![0, 1, 2, 3, 4])
        );
        assert_eq!(
            behavior
                .get_result(
                    &mut PrepareMetadata::new(DataType::UInt64.clone()),
                    &make_test_batch(3)
                )
                .unwrap()
                .as_ref(),
            &UInt64Array::from(vec![5, 6, 7])
        );
    }

    #[test]
    fn test_sequential_u64_overflow() {
        let mut behavior = ColumnBehavior::SequentialU64 {
            next_offset: u64::MAX - 3,
        };
        // Current behavior is to immediately wrap.
        assert_eq!(
            behavior
                .get_result(
                    &mut PrepareMetadata::new(DataType::UInt64.clone()),
                    &make_test_batch(5)
                )
                .unwrap()
                .as_ref(),
            &UInt64Array::from(vec![0, 1, 2, 3, 4])
        );
        assert_eq!(
            behavior
                .get_result(
                    &mut PrepareMetadata::new(DataType::UInt64.clone()),
                    &make_test_batch(3)
                )
                .unwrap()
                .as_ref(),
            &UInt64Array::from(vec![5, 6, 7])
        );
    }

    #[test]
    fn test_sequential_u64_nonzero() {
        let mut behavior = ColumnBehavior::SequentialU64 { next_offset: 100 };
        assert_eq!(
            behavior
                .get_result(
                    &mut PrepareMetadata::new(DataType::UInt64.clone()),
                    &make_test_batch(5)
                )
                .unwrap()
                .as_ref(),
            &UInt64Array::from(vec![100, 101, 102, 103, 104])
        );
        assert_eq!(
            behavior
                .get_result(
                    &mut PrepareMetadata::new(DataType::UInt64.clone()),
                    &make_test_batch(3)
                )
                .unwrap()
                .as_ref(),
            &UInt64Array::from(vec![105, 106, 107])
        );
    }

    #[test]
    fn test_entity_key_mapping() {
        let mut behavior = ColumnBehavior::EntityKey {
            index: 0,
            nullable: false,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("names", DataType::Utf8, true)]));
        let data = StringArray::from(vec!["awkward", "tacos", "awkward", "tacos", "apples"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(data)]).unwrap();
        let mut metadata = PrepareMetadata::new(DataType::Utf8.clone());
        behavior.get_result(&mut metadata, &batch).unwrap();
        assert_eq!(metadata.get_flush_metadata().unwrap().num_rows(), 3);
        assert!(metadata.previous_keys.is_empty());
        assert!(metadata.metadata_batches.is_empty());
    }
    #[test]
    fn test_entity_key_mapping_int() {
        let mut behavior = ColumnBehavior::EntityKey {
            index: 0,
            nullable: false,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, true)]));
        let data = UInt64Array::from(vec![1, 2, 3, 1, 2, 3]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(data)]).unwrap();
        let mut metadata = PrepareMetadata::new(DataType::UInt64.clone());
        behavior.get_result(&mut metadata, &batch).unwrap();
        assert_eq!(metadata.get_flush_metadata().unwrap().num_rows(), 3);
        assert!(metadata.previous_keys.is_empty());
        assert!(metadata.metadata_batches.is_empty());
    }
}
