use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::{DataType, FieldRef};
use binary_merge::BinaryMergeInput;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use sparrow_batch::Batch;
use sparrow_instructions::GroupingIndices;

use crate::{gather::Gatherer, spread::Spread};

use super::binary_merge;

/// Manages the merge of two heterogeneous inputs.
pub struct HeterogeneousMerge {
    result_type: DataType,
    spread_left: Spread,
    spread_right: Spread,

    /// Gathers batches from both sides and produces [GatheredBatches]
    /// up to a valid watermark.
    gatherer: Gatherer,
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "internal error merging: {}", _0)]
    Internal(&'static str),
}

impl error_stack::Context for Error {}

impl HeterogeneousMerge {
    /// Arguments:
    /// - result_type: the result type of the merge
    /// - datatype_l: the datatype of the left input
    /// - datatype_r: the datatype of the right input
    /// TODO: Interpolation
    pub fn new(result_type: &DataType, datatype_l: &DataType, datatype_r: &DataType) -> Self {
        Self {
            result_type: result_type.clone(),
            spread_left: Spread::try_new(false, datatype_l).expect("spread"),
            spread_right: Spread::try_new(false, datatype_r).expect("spread"),
            gatherer: Gatherer::new(2),
        }
    }

    /// Returns true if all inputs are closed.
    pub fn all_closed(&self) -> bool {
        self.gatherer.all_closed()
    }

    /// Returns the index of the input we need to advance.
    ///
    /// If all inputs have been closed, returns `None`.
    pub fn blocking_input(&self) -> Option<usize> {
        self.gatherer.blocking_input()
    }

    /// Adds a batch to the merge for the specific input side.
    pub fn add_batch(&mut self, input: usize, batch: Batch) -> bool {
        self.gatherer.add_batch(input, batch)
    }

    /// Merges the next batch.
    ///
    /// Note that this method can only be called on the active index,
    /// obtained from `blocking_input`.
    pub fn merge(&mut self) -> error_stack::Result<Batch, Error> {
        let gathered_batches = self.gatherer.next_batch();
        if let Some(gathered_batches) = gathered_batches {
            let concat_batches = gathered_batches.concat();
            let left: &Batch = &concat_batches[0];
            let right: &Batch = &concat_batches[1];

            // TODO: Assumes batch data is non-empty.
            let left_merge_input = BinaryMergeInput::new(
                left.time().expect("time"),
                left.subsort().expect("subsort"),
                left.key_hash().expect("key_hash"),
            );
            let right_merge_input = BinaryMergeInput::new(
                right.time().expect("time"),
                right.subsort().expect("subsort"),
                right.key_hash().expect("key_hash"),
            );
            let merged_result = crate::binary_merge(left_merge_input, right_merge_input)
                .into_report()
                .change_context(Error::Internal("TODO"))?;

            let left_spread_bits = arrow::compute::is_not_null(&merged_result.take_a)
                .into_report()
                .change_context(Error::Internal("TODO"))?;
            let right_spread_bits = arrow::compute::is_not_null(&merged_result.take_b)
                .into_report()
                .change_context(Error::Internal("TODO"))?;

            let merged_time = Arc::new(merged_result.time);
            let merged_subsort = Arc::new(merged_result.subsort);
            let merged_key_hash = Arc::new(merged_result.key_hash);

            // TODO: Grouping
            let grouping = GroupingIndices::new_empty();
            // TODO: Handle empty batches
            let spread_left = self
                .spread_left
                .spread_signaled(&grouping, left.data().expect("data"), &left_spread_bits)
                .into_report()
                .change_context(Error::Internal("TODO"))?;
            let spread_right = self
                .spread_right
                .spread_signaled(&grouping, right.data().expect("data"), &right_spread_bits)
                .into_report()
                .change_context(Error::Internal("TODO"))?;

            // println!("Spread left: {:?}", spread_left);
            // println!("Spread right: {:?}", spread_right);
            assert_eq!(spread_left.len(), spread_right.len());
            let num_rows = spread_left.len();

            // The result type of the merge does not flatten the structs.
            //
            // e.g. have X.a + Y.a then we have left: { a: i64 } and right: { a: i64 }.
            // If we flatten that to {a: i64} we can no longer perform the arithmetic.
            // Instead, we want {left: {a: i64}, right: {a: i64 }} so we can do
            // merged.left.a + merged.right.
            let fields: Vec<(FieldRef, ArrayRef)> = match &self.result_type {
                DataType::Struct(fields) => {
                    // The result type should always have two fields -- the left and the right.
                    assert_eq!(fields.len(), 2);
                    vec![
                        (fields[0].clone(), spread_left),
                        (fields[1].clone(), spread_right),
                    ]
                }
                other => {
                    tracing::error!("expected struct, got {:?}", other);
                    error_stack::bail!(Error::Internal("merge result type should be a struct"))
                }
            };
            let merged_data = Arc::new(sparrow_arrow::utils::make_struct_array(num_rows, fields));
            // Since we're merging batches, the up_to_time is the last time in the merged batch.
            let up_to_time = merged_time.value(merged_time.len() - 1);

            Ok(Batch::new_with_data(
                merged_data,
                merged_time,
                merged_subsort,
                merged_key_hash,
                up_to_time.into(),
            ))
        } else {
            error_stack::bail!(Error::Internal("expected batch -- "))
        }
    }

    /// Closes the given side of the input.
    ///
    /// This allows the remaining side to progress unbounded.
    pub fn close(&mut self, index: usize) {
        self.gatherer.close(index);
    }
}
#[cfg(test)]
mod tests {

    use arrow_array::{
        types::{ArrowPrimitiveType, TimestampNanosecondType, UInt32Type, UInt64Type},
        RecordBatch, StructArray, TimestampNanosecondArray, UInt64Array, UInt8Array,
    };
    use arrow_schema::{Field, Fields};
    use proptest::prelude::*;
    use sparrow_core::TableSchema;

    use super::*;

    #[test]
    fn test_merge() {
        // Batches share the same datatype in this case
        let dt = DataType::Struct(Fields::from(vec![
            Field::new("time", TimestampNanosecondType::DATA_TYPE, true),
            Field::new("key_hash", UInt64Type::DATA_TYPE, true),
        ]));
        let result_type = DataType::Struct(Fields::from(vec![
            Field::new("step_0", dt.clone(), true),
            Field::new("step_1", dt.clone(), true),
        ]));

        let mut merge = HeterogeneousMerge::new(&result_type, &dt, &dt);
        assert_eq!(merge.blocking_input(), Some(1));
        let lb_1 = Batch::minimal_from(vec![0, 1, 2], vec![0, 0, 0], 2);
        let rb_1 = Batch::minimal_from(vec![0, 4], vec![0, 0], 4);

        let can_produce = merge.add_batch(1, rb_1);
        assert!(!can_produce);

        assert_eq!(merge.blocking_input(), Some(0));
        let can_produce = merge.add_batch(0, lb_1);
        assert!(can_produce);

        let merged = merge.merge().unwrap();
        let step_0_fields: Vec<(FieldRef, ArrayRef)> = vec![
            (
                Arc::new(Field::new("time", TimestampNanosecondType::DATA_TYPE, true)),
                Arc::new(TimestampNanosecondArray::from(vec![0, 1])),
            ),
            (
                Arc::new(Field::new("key_hash", UInt64Type::DATA_TYPE, true)),
                Arc::new(UInt64Array::from(vec![0, 0])),
            ),
        ];
        let step_1_fields: Vec<(FieldRef, ArrayRef)> = vec![
            (
                Arc::new(Field::new("time", TimestampNanosecondType::DATA_TYPE, true)),
                Arc::new(TimestampNanosecondArray::from(vec![Some(0), None])),
            ),
            (
                Arc::new(Field::new("key_hash", UInt64Type::DATA_TYPE, true)),
                Arc::new(UInt64Array::from(vec![Some(0), None])),
            ),
        ];
        let expected_fields: Vec<(FieldRef, ArrayRef)> = vec![
            (
                Arc::new(Field::new("step_0", dt.clone(), true)),
                Arc::new(StructArray::from(step_0_fields)),
            ),
            (
                Arc::new(Field::new("step_1", dt.clone(), true)),
                Arc::new(StructArray::from(step_1_fields)),
            ),
        ];
        let expected_data = Arc::new(sparrow_arrow::utils::make_struct_array(2, expected_fields));
        let expected_time = Arc::new(TimestampNanosecondArray::from(vec![0, 1]));
        let expected_subsort = Arc::new(UInt64Array::from(vec![0, 1]));
        let expected_key = Arc::new(UInt64Array::from(vec![0, 0]));

        let expected = Batch::new_with_data(
            expected_data,
            expected_time,
            expected_subsort,
            expected_key,
            1.into(),
        );

        // TODO: struct equality failing?
        assert_eq!(merged, expected);
        assert_eq!(merge.blocking_input(), Some(0));
    }

    #[test]
    fn test_fails_if_not_working_on_active_input() {}
    #[test]
    fn test_all_closed() {}

    #[ignore = "spread not implemented"]
    fn test_spreads() {}
    #[test]
    fn test_merging_structs() {}

    #[test]
    fn test_merge_non_structs() {}

    #[test]
    fn test_merge_empty_batches() {}
}
