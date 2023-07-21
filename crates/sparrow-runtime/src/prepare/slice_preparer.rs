use std::sync::Arc;

use arrow::array::{Array, ArrayRef, StringArray, UInt64Array};
use arrow::compute::eq_scalar;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, ResultExt};
use hashbrown::HashSet;
use sparrow_api::kaskada::v1alpha::slice_plan;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_core::context_code;

use crate::prepare::Error;

pub(super) struct SlicePreparer {
    entity_column_index: usize,
    prepare_filter: PrepareFilter,
}

enum PrepareFilter {
    NoFilter,
    EntityKeys { entity_keys: HashSet<u64> },
    PercentFilter { percent: f64 },
}

impl SlicePreparer {
    pub(super) fn try_new(
        entity_column_index: usize,
        entity_type: DataType,
        slice: Option<&slice_plan::Slice>,
    ) -> anyhow::Result<Self> {
        let prepare_filter = match slice {
            None => PrepareFilter::NoFilter,
            Some(slice_plan::Slice::Percent(percent)) => PrepareFilter::PercentFilter {
                percent: percent.percent,
            },
            Some(slice_plan::Slice::EntityKeys(entity_keys)) => {
                let entity_keys: ArrayRef =
                    Arc::new(StringArray::from(entity_keys.entity_keys.clone()));
                let entity_keys = arrow::compute::cast(&entity_keys, &entity_type)?;
                anyhow::ensure!(
                    entity_keys.null_count() == 0,
                    context_code!(
                        tonic::Code::InvalidArgument,
                        "Casting provided entity keys to type {} resulted in {} null keys.",
                        entity_type,
                        entity_keys.null_count()
                    )
                );
                let entity_key_hashes =
                    sparrow_arrow::hash::hash(&entity_keys).map_err(|e| e.into_error())?;
                let entity_key_hashes: &UInt64Array = downcast_primitive_array(&entity_key_hashes)?;
                let desired_keys: HashSet<u64> =
                    entity_key_hashes.values().iter().copied().collect();
                PrepareFilter::EntityKeys {
                    entity_keys: desired_keys,
                }
            }
        };

        Ok(Self {
            entity_column_index,
            prepare_filter,
        })
    }

    pub(super) fn slice_batch(
        &self,
        record_batch: RecordBatch,
    ) -> error_stack::Result<RecordBatch, Error> {
        let include_filter = match &self.prepare_filter {
            // No filter applied results in the same record batch
            PrepareFilter::NoFilter => return Ok(record_batch),
            PrepareFilter::PercentFilter { percent } => {
                // We are using hash and modulus math to determine if the entity
                // is included in the slice.
                //
                // 1. Hash the entity key which results in a UInt64
                // 2. Calculate the upper bound for inclusive hashes. If the
                // hashed entity key is less than the upper bound, the entity
                // key is included in the slice.
                // 3. Filter the record batch with only the entity keys that are
                // included in the slice.
                //
                // Potential issue: Since we are using hash modulus logic, we
                // run into issues with the distribution of the samples across
                // the UInt64 distribution. Probably want a more guaranteed
                // solution since this can result in unexpected results such as
                // more than 10% (all the entity keys hash to below the upper
                // bound) or less than 10%.
                let upper_bound = (percent / 100.0 * (u64::MAX as f64)).round() as u64;
                let entity_column = self.hash_entity_column(&record_batch)?;

                arrow::compute::lt_eq_scalar(&entity_column, upper_bound)
                    .into_report()
                    .change_context(Error::SlicingBatch)?
            }
            PrepareFilter::EntityKeys { entity_keys } => {
                let entity_column = self.hash_entity_column(&record_batch)?;
                // We want a boolean column indicating whether the entity key
                // hash is in the set of requested entities. No kernel exists
                // for this, so we use unary, which must return a primitive. We
                // return an integer (0 or 1) and then separately convert to a
                // boolean using eq_scalar.
                let include: UInt64Array =
                    arrow::compute::kernels::arity::unary(&entity_column, |key_hash| {
                        u64::from(entity_keys.contains(&key_hash))
                    });

                // Convert that from a primitive array to a boolean array by comparing to the
                // scalar value 1.
                eq_scalar(&include, 1)
                    .into_report()
                    .change_context(Error::SlicingBatch)?
            }
        };

        let result = arrow::compute::filter_record_batch(&record_batch, &include_filter)
            .into_report()
            .change_context(Error::SlicingBatch)?;
        Ok(result)
    }

    fn hash_entity_column(
        &self,
        record_batch: &RecordBatch,
    ) -> error_stack::Result<UInt64Array, Error> {
        let entity_column = record_batch.column(self.entity_column_index);
        sparrow_arrow::hash::hash(entity_column).change_context(Error::SlicingBatch)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use sparrow_arrow::hash::hash;

    use super::*;

    #[test]
    fn test_preparer_slice_batch_100_percent() {
        let entity_column_index = 0;
        let percent = 100.0;
        let expected_batch_size = 6;

        let preparer = SlicePreparer {
            entity_column_index,
            prepare_filter: PrepareFilter::PercentFilter { percent },
        };

        let entity_key_column = StringArray::from(vec!["a", "b", "c", "d", "e", "f"]);
        let data_column = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(entity_key_column), Arc::new(data_column)],
        )
        .unwrap();

        let sliced_batch = preparer.slice_batch(batch).unwrap();
        assert_eq!(sliced_batch.num_rows(), expected_batch_size);
    }

    #[test]
    fn test_preparer_slice_batch_50_percent() {
        let entity_column_index = 0;
        let percent = 50.0;

        let preparer = SlicePreparer {
            entity_column_index,
            prepare_filter: PrepareFilter::PercentFilter { percent },
        };

        let entity_key_column = StringArray::from(vec!["a", "a", "c", "c"]);
        let data_column = Int32Array::from(vec![1, 2, 3, 4]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(entity_key_column), Arc::new(data_column)],
        )
        .unwrap();

        let sliced_batch = preparer.slice_batch(batch).unwrap();
        let expected_batch_size = 2;
        assert_eq!(sliced_batch.num_rows(), expected_batch_size);
    }

    #[test]
    fn test_preparer_slice_batch_20_percent() {
        let entity_column_index = 0;
        let percent = 20.0;

        let preparer = SlicePreparer {
            entity_column_index,
            prepare_filter: PrepareFilter::PercentFilter { percent },
        };

        let entity_key_column =
            StringArray::from(vec!["a", "b", "c", "d", "e", "f", "g", "h", "i"]);
        let data_column = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(entity_key_column), Arc::new(data_column)],
        )
        .unwrap();

        let sliced_batch = preparer.slice_batch(batch).unwrap();
        let expected_batch_size = 2;
        assert_eq!(sliced_batch.num_rows(), expected_batch_size);
    }

    #[test]
    fn test_preparer_slice_specific_entity_key() {
        let entity_column_index = 0;

        let entity_key_column =
            StringArray::from(vec!["a", "b", "a", "b", "e", "f", "g", "h", "i"]);
        let hashes = hash(&entity_key_column).unwrap();

        let mut hash_set = HashSet::new();
        hash_set.insert(hashes.value(0));
        hash_set.insert(hashes.value(1));

        let preparer = SlicePreparer {
            entity_column_index,
            prepare_filter: PrepareFilter::EntityKeys {
                entity_keys: hash_set,
            },
        };

        let data_column = Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9]);
        let schema = Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(entity_key_column), Arc::new(data_column)],
        )
        .unwrap();

        let sliced_batch = preparer.slice_batch(batch).unwrap();
        let expected_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Utf8, false),
                Field::new("count", DataType::Int32, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "b"])),
                Arc::new(Int32Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();
        assert_eq!(sliced_batch, expected_batch);
    }
}
