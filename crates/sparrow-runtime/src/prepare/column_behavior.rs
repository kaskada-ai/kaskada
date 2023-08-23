use anyhow::Context;
use arrow::{
    array::ArrayRef,
    datatypes::{DataType, Field, SchemaRef},
    record_batch::RecordBatch,
};
use sparrow_core::context_code;

use std::sync::Arc;

use anyhow::anyhow;
use arrow::array::{Array, UInt64Array};

use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use sparrow_arrow::utils::make_null_array;
use sparrow_kernels::order_preserving_cast_to_u64;

use crate::prepare::Error;

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
    /// Generates a row of monotically increasing u64s, starting
    /// at the defined offset.
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
    pub fn try_new_cast(
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
                    "column to cast '{}' not present in schema {:?}",
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
    pub fn try_new_subsort(source_schema: &SchemaRef, source_name: &str) -> anyhow::Result<Self> {
        let (source_index, source_field) = source_schema
            .column_with_name(source_name)
            .with_context(|| {
                context_code!(
                    tonic::Code::Internal,
                    "subsort column '{}' not present in schema {:?}",
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
                    "entity key column '{}' not present in schema {:?}",
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
    pub fn try_cast_or_reference_or_null(
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

    pub async fn get_result(
        &mut self,
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

                let entity_column =
                    sparrow_arrow::hash::hash(column).change_context(Error::PreparingColumn)?;

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

    use super::ColumnBehavior;
    use arrow::array::{Int64Array, TimestampNanosecondArray, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use static_init::dynamic;

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

    #[tokio::test]
    async fn test_sequential_u64_zero() {
        let mut behavior = ColumnBehavior::SequentialU64 { next_offset: 0 };
        assert_eq!(
            behavior
                .get_result(&make_test_batch(5))
                .await
                .unwrap()
                .as_ref(),
            &UInt64Array::from(vec![0, 1, 2, 3, 4])
        );
        assert_eq!(
            behavior
                .get_result(&make_test_batch(3))
                .await
                .unwrap()
                .as_ref(),
            &UInt64Array::from(vec![5, 6, 7])
        );
    }

    #[tokio::test]
    async fn test_sequential_u64_overflow() {
        let mut behavior = ColumnBehavior::SequentialU64 {
            next_offset: u64::MAX - 3,
        };
        // Current behavior is to immediately wrap.
        assert_eq!(
            behavior
                .get_result(&make_test_batch(5))
                .await
                .unwrap()
                .as_ref(),
            &UInt64Array::from(vec![0, 1, 2, 3, 4])
        );
        assert_eq!(
            behavior
                .get_result(&make_test_batch(3))
                .await
                .unwrap()
                .as_ref(),
            &UInt64Array::from(vec![5, 6, 7])
        );
    }

    #[tokio::test]
    async fn test_sequential_u64_nonzero() {
        let mut behavior = ColumnBehavior::SequentialU64 { next_offset: 100 };
        assert_eq!(
            behavior
                .get_result(&make_test_batch(5))
                .await
                .unwrap()
                .as_ref(),
            &UInt64Array::from(vec![100, 101, 102, 103, 104])
        );
        assert_eq!(
            behavior
                .get_result(&make_test_batch(3))
                .await
                .unwrap()
                .as_ref(),
            &UInt64Array::from(vec![105, 106, 107])
        );
    }
}
