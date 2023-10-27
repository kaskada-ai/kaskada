use arrow_array::{new_null_array, ArrayRef, StructArray};
use arrow_schema::{DataType, FieldRef};
use binary_merge::BinaryMergeInput;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use sparrow_batch::{Batch, RowTime};
use sparrow_instructions::GroupingIndices;
use std::sync::Arc;

use crate::{gather::Gatherer, spread::Spread};

use super::binary_merge;

/// Manages the merge of two heterogeneous inputs.
pub struct HeterogeneousMerge {
    result_type: DataType,
    left_data_type: DataType,
    right_data_type: DataType,
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
            left_data_type: datatype_l.clone(),
            right_data_type: datatype_r.clone(),
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

    /// Returns a boolean indicating if the merge can produce a batch.
    pub fn can_produce(&self) -> bool {
        self.gatherer.can_produce()
    }

    /// Merges the next batch.
    ///
    /// Note that this method can only be called on the active index,
    /// obtained from `blocking_input`.
    pub fn merge(&mut self) -> error_stack::Result<Batch, Error> {
        let gathered_batches = self.gatherer.next_batch();
        if let Some(gathered_batches) = gathered_batches {
            let mut concat_batches = gathered_batches.concat()?;
            let right: Option<Batch> = concat_batches.remove(1);
            let left: Option<Batch> = concat_batches.remove(0);

            // There are two layers here:
            // 1) A batch does not exist, which occurs when the gatherer does not have any batches
            //    for a particular side. This is expected, and we can just produce the non-empty.
            // 2) A batch exists, but is empty. It still has an up_to_time, which needs to be accounted for.
            match (left, right) {
                (Some(left), Some(right)) => match (left.time(), right.time()) {
                    (None, None) => self.handle_empty_merge(left.up_to_time, right.up_to_time),
                    (Some(_), None) => {
                        self.handle_left_merge(left.up_to_time.max(right.up_to_time), left)
                    }
                    (None, Some(_)) => {
                        self.handle_right_merge(left.up_to_time.max(right.up_to_time), right)
                    }
                    (Some(_), Some(_)) => self.handle_merge(left, right),
                },
                (Some(left), None) => {
                    let up_to_time = left.up_to_time;
                    self.handle_left_merge(up_to_time, left)
                }
                (None, Some(right)) => {
                    let up_to_time = right.up_to_time;
                    self.handle_right_merge(up_to_time, right)
                }
                (None, None) => {
                    // This is in unexpected state -- if we call merge, we should be getting at least
                    // one batch, even if it's empty.
                    error_stack::bail!(Error::Internal("expected at least one batch"))
                }
            }
        } else {
            error_stack::bail!(Error::Internal("expected gathered batch"))
        }
    }

    fn handle_empty_merge(
        &mut self,
        left: RowTime,
        right: RowTime,
    ) -> error_stack::Result<Batch, Error> {
        let up_to_time = left.max(right);
        Ok(Batch::new_empty(up_to_time))
    }

    /// Handles merging where the right batch is empty.
    ///
    /// Arguments:
    /// - up_to_time: the up_to_time of the merged batch
    /// - batch: the left side non-empty batch
    fn handle_left_merge(
        &mut self,
        up_to_time: RowTime,
        batch: Batch,
    ) -> error_stack::Result<Batch, Error> {
        // TODO: Grouping
        let grouping = GroupingIndices::new_empty();

        // SAFETY: Verified batch is non-empty
        let left_data = self
            .spread_left
            .spread_true(&grouping, batch.data().expect("data"))
            .into_report()
            .change_context(Error::Internal("TODO"))?;
        let num_rows = left_data.len();
        assert_eq!(num_rows, batch.time().expect("time").len());

        let right_data = new_null_array(&self.right_data_type, num_rows);
        let merged_data = self.create_merged_data(num_rows, left_data, right_data)?;

        let batch = batch.with_data(merged_data);
        let batch = batch.with_up_to_time(up_to_time);
        Ok(batch)
    }

    /// Handles merging where the left batch is empty.
    ///
    /// Arguments:
    /// - up_to_time: the up_to_time of the merged batch
    /// - batch: the right side non-empty batch
    fn handle_right_merge(
        &mut self,
        up_to_time: RowTime,
        batch: Batch,
    ) -> error_stack::Result<Batch, Error> {
        // TODO: Grouping
        let grouping = GroupingIndices::new_empty();

        // SAFETY: Verified batch is non-empty
        let right_data = self
            .spread_right
            .spread_true(&grouping, batch.data().expect("data"))
            .into_report()
            .change_context(Error::Internal("TODO"))?;
        let num_rows = right_data.len();
        assert_eq!(num_rows, batch.time().expect("time").len());

        let left_data = new_null_array(&self.left_data_type, num_rows);
        let merged_data = self.create_merged_data(num_rows, left_data, right_data)?;

        let batch = batch.with_data(merged_data);
        let batch = batch.with_up_to_time(up_to_time);
        Ok(batch)
    }

    fn handle_merge(&mut self, left: Batch, right: Batch) -> error_stack::Result<Batch, Error> {
        let up_to_time = left.up_to_time.max(right.up_to_time);

        // SAFETY: Verified non-empty arrays
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

        assert_eq!(spread_left.len(), spread_right.len());
        let num_rows = spread_left.len();

        let merged_data = self.create_merged_data(num_rows, spread_left, spread_right)?;

        Ok(Batch::new_with_data(
            merged_data,
            merged_time,
            merged_subsort,
            merged_key_hash,
            up_to_time,
        ))
    }

    /// Merges the left and right data into a struct array matching the
    /// expected result type.
    fn create_merged_data(
        &self,
        num_rows: usize,
        left_data: ArrayRef,
        right_data: ArrayRef,
    ) -> error_stack::Result<Arc<StructArray>, Error> {
        // The result type of the merge does not flatten the structs.
        //
        // e.g. we have [X.a + Y.a] then we have left: { a: i64 } and right: { a: i64 }.
        // If we flatten that to { a: i64 } we can no longer perform the arithmetic.
        // Instead, we want {left: {a: i64}, right: {a: i64 }} so we can do
        // merged.left.a + merged.right.
        let fields: Vec<(FieldRef, ArrayRef)> = match &self.result_type {
            DataType::Struct(fields) => {
                // The result type should always have two fields -- the left and the right.
                assert_eq!(fields.len(), 2);
                vec![
                    (fields[0].clone(), left_data),
                    (fields[1].clone(), right_data),
                ]
            }
            other => {
                tracing::error!("expected struct, got {:?}", other);
                error_stack::bail!(Error::Internal("merge result type should be a struct"))
            }
        };
        Ok(Arc::new(sparrow_arrow::utils::make_struct_array(
            num_rows, fields,
        )))
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
    use super::*;
    use arrow::buffer::NullBuffer;
    use arrow_array::{
        types::{ArrowPrimitiveType, TimestampNanosecondType, UInt64Type},
        Array, Int64Array, StructArray, TimestampNanosecondArray, UInt32Array, UInt64Array,
    };
    use arrow_schema::{Field, Fields};

    fn minimal_struct_type() -> DataType {
        DataType::Struct(Fields::from(vec![
            Field::new("time", TimestampNanosecondType::DATA_TYPE, true),
            Field::new("key_hash", UInt64Type::DATA_TYPE, true),
        ]))
    }

    fn merge(left: DataType, right: DataType) -> HeterogeneousMerge {
        let result_type = DataType::Struct(Fields::from(vec![
            Field::new("step_0", left.clone(), true),
            Field::new("step_1", right.clone(), true),
        ]));
        HeterogeneousMerge::new(&result_type, &left, &right)
    }

    #[test]
    fn test_merge() {
        let dt = minimal_struct_type();
        let mut merge = merge(dt.clone(), dt.clone());

        assert_eq!(merge.blocking_input(), Some(1));
        let lb_1 = Batch::minimal_from(vec![0, 1, 2], vec![0, 0, 0], 2);
        let rb_1 = Batch::minimal_from(vec![0, 4], vec![0, 0], 4);

        let can_produce = merge.add_batch(1, rb_1);
        assert!(!can_produce);

        assert_eq!(merge.blocking_input(), Some(0));
        let can_produce = merge.add_batch(0, lb_1);
        assert!(can_produce);

        let merged = merge.merge().unwrap();
        let step_0_array = StructArray::new(
            vec![
                Arc::new(Field::new("time", TimestampNanosecondType::DATA_TYPE, true)),
                Arc::new(Field::new("key_hash", UInt64Type::DATA_TYPE, true)),
            ]
            .into(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![0, 1])),
                Arc::new(UInt64Array::from(vec![Some(0), Some(0)])),
            ],
            None,
        );
        let step_1_array = StructArray::new(
            vec![
                Arc::new(Field::new("time", TimestampNanosecondType::DATA_TYPE, true)),
                Arc::new(Field::new("key_hash", UInt64Type::DATA_TYPE, true)),
            ]
            .into(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![Some(0), None])),
                Arc::new(UInt64Array::from(vec![Some(0), None])),
            ],
            Some(NullBuffer::from(vec![true, false])),
        );
        let expected_fields: Vec<(FieldRef, ArrayRef)> = vec![
            (
                Arc::new(Field::new("step_0", dt.clone(), true)),
                Arc::new(step_0_array),
            ),
            (
                Arc::new(Field::new("step_1", dt.clone(), true)),
                Arc::new(step_1_array),
            ),
        ];
        let expected_data = Arc::new(sparrow_arrow::utils::make_struct_array(2, expected_fields));
        let expected_time = Arc::new(TimestampNanosecondArray::from(vec![0, 1]));
        let expected_subsort = Arc::new(UInt64Array::from(vec![0, 1]));
        let expected_key = Arc::new(UInt64Array::from(vec![0, 0]));

        let expected = Batch::new_with_data(
            expected_data.clone(),
            expected_time,
            expected_subsort,
            expected_key,
            2.into(),
        );

        assert_eq!(merged, expected);
        assert_eq!(merge.blocking_input(), Some(0));
    }

    #[test]
    fn test_merge_non_structs() {
        let left = DataType::Int64;
        let right = DataType::UInt32;
        let mut merge = merge(left.clone(), right.clone());

        let left_time = TimestampNanosecondArray::from(vec![0, 1, 2]);
        let left_subsort = UInt64Array::from(vec![0, 1, 2]);
        let left_key = UInt64Array::from(vec![0, 0, 0]);
        let left_data = Int64Array::from(vec![10, 4, 22]);
        let left_batch = Batch::new_with_data(
            Arc::new(left_data),
            Arc::new(left_time),
            Arc::new(left_subsort),
            Arc::new(left_key),
            2.into(),
        );

        let right_time = TimestampNanosecondArray::from(vec![0, 1, 4]);
        let right_subsort = UInt64Array::from(vec![0, 1, 2]);
        let right_key = UInt64Array::from(vec![0, 1, 0]);
        let right_data = UInt32Array::from(vec![100, 101, 102]);
        let right_batch = Batch::new_with_data(
            Arc::new(right_data),
            Arc::new(right_time),
            Arc::new(right_subsort),
            Arc::new(right_key),
            4.into(),
        );

        assert_eq!(merge.blocking_input(), Some(1));
        let can_produce = merge.add_batch(1, right_batch);
        assert!(!can_produce);

        let can_produce = merge.add_batch(0, left_batch);
        assert!(can_produce);

        let merged = merge.merge().unwrap();
        let expected_fields: Vec<(FieldRef, ArrayRef)> = vec![
            (
                Arc::new(Field::new("step_0", left.clone(), true)),
                Arc::new(Int64Array::from(vec![Some(10), Some(4), None])),
            ),
            (
                Arc::new(Field::new("step_1", right.clone(), true)),
                Arc::new(UInt32Array::from(vec![Some(100), None, Some(101)])),
            ),
        ];
        let expected_data = Arc::new(sparrow_arrow::utils::make_struct_array(3, expected_fields));
        let expected_time = Arc::new(TimestampNanosecondArray::from(vec![0, 1, 1]));
        let expected_subsort = Arc::new(UInt64Array::from(vec![0, 1, 1]));
        let expected_key = Arc::new(UInt64Array::from(vec![0, 0, 1]));

        let expected = Batch::new_with_data(
            expected_data,
            expected_time,
            expected_subsort,
            expected_key,
            2.into(),
        );

        assert_eq!(merged, expected);
        assert_eq!(merge.blocking_input(), Some(0));
    }

    #[test]
    #[should_panic]
    fn test_fails_if_not_working_on_active_input() {
        let dt = minimal_struct_type();
        let mut merge = merge(dt.clone(), dt.clone());

        assert_eq!(merge.blocking_input(), Some(1));
        let batch = Batch::minimal_from(vec![0, 1, 2], vec![0, 0, 0], 2);

        // Active input is 1, but we attempt to add to 0. Expect panic
        merge.add_batch(0, batch);
    }

    #[test]
    fn test_all_closed() {
        let dt = minimal_struct_type();
        let mut merge = merge(dt.clone(), dt.clone());

        merge.close(1);
        assert!(!merge.all_closed());
        merge.close(0);
        assert!(merge.all_closed())
    }

    #[ignore = "spread not implemented"]
    #[allow(dead_code)]
    fn test_spreads() {
        todo!()
    }

    #[test]
    fn test_merge_empty_batches() {
        let left = DataType::Int64;
        let right = DataType::UInt32;
        let mut merge = merge(left.clone(), right.clone());

        let left_time = TimestampNanosecondArray::from(vec![0, 1, 2]);
        let left_subsort = UInt64Array::from(vec![0, 1, 2]);
        let left_key = UInt64Array::from(vec![0, 0, 0]);
        let left_data = Int64Array::from(vec![10, 4, 22]);
        let left_batch = Batch::new_with_data(
            Arc::new(left_data),
            Arc::new(left_time),
            Arc::new(left_subsort),
            Arc::new(left_key),
            2.into(),
        );

        let right_batch = Batch::new_empty(4.into());

        assert_eq!(merge.blocking_input(), Some(1));
        let can_produce = merge.add_batch(1, right_batch);
        assert!(!can_produce);

        let can_produce = merge.add_batch(0, left_batch);
        assert!(can_produce);

        let merged = merge.merge().unwrap();
        let expected_fields: Vec<(FieldRef, ArrayRef)> = vec![
            (
                Arc::new(Field::new("step_0", left.clone(), true)),
                Arc::new(Int64Array::from(vec![Some(10), Some(4)])),
            ),
            (
                Arc::new(Field::new("step_1", right.clone(), true)),
                Arc::new(UInt32Array::from(vec![None, None])),
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
            2.into(),
        );

        assert_eq!(merged, expected);
        assert_eq!(merge.blocking_input(), Some(0));
    }

    #[test]
    fn test_multiple_merges() {
        let left = minimal_struct_type();
        let right = DataType::UInt32;
        let mut merge = merge(left.clone(), right.clone());

        let left_batch = Batch::minimal_from(vec![0, 1, 2], vec![0, 0, 0], 10);
        let right_batch = Batch::new_empty(5.into());

        assert_eq!(merge.blocking_input(), Some(1));
        let can_produce = merge.add_batch(1, right_batch);
        assert!(!can_produce);

        let can_produce = merge.add_batch(0, left_batch);
        assert!(can_produce);

        let merged = merge.merge().unwrap();
        let step_0_array = StructArray::new(
            vec![
                Arc::new(Field::new("time", TimestampNanosecondType::DATA_TYPE, true)),
                Arc::new(Field::new("key_hash", UInt64Type::DATA_TYPE, true)),
            ]
            .into(),
            vec![
                Arc::new(TimestampNanosecondArray::from(vec![0, 1, 2])),
                Arc::new(UInt64Array::from(vec![0, 0, 0])),
            ],
            None,
        );
        let step_0_datatype = step_0_array.data_type().clone();
        let expected_fields: Vec<(FieldRef, ArrayRef)> = vec![
            (
                Arc::new(Field::new("step_0", left.clone(), true)),
                Arc::new(step_0_array),
            ),
            (
                Arc::new(Field::new("step_1", right.clone(), true)),
                Arc::new(UInt32Array::from(vec![None, None, None])),
            ),
        ];
        let expected_data = Arc::new(sparrow_arrow::utils::make_struct_array(3, expected_fields));
        let expected_time = Arc::new(TimestampNanosecondArray::from(vec![0, 1, 2]));
        let expected_subsort = Arc::new(UInt64Array::from(vec![0, 1, 2]));
        let expected_key = Arc::new(UInt64Array::from(vec![0, 0, 0]));
        let expected = Batch::new_with_data(
            expected_data,
            expected_time,
            expected_subsort,
            expected_key,
            5.into(),
        );

        assert_eq!(merged, expected);
        assert_eq!(merge.blocking_input(), Some(1));

        // Add another batch for the right side
        let right_time = TimestampNanosecondArray::from(vec![5, 6, 7]);
        let right_subsort = UInt64Array::from(vec![0, 0, 0]);
        let right_key = UInt64Array::from(vec![0, 1, 0]);
        let right_data = UInt32Array::from(vec![100, 101, 102]);
        let right_batch = Batch::new_with_data(
            Arc::new(right_data),
            Arc::new(right_time),
            Arc::new(right_subsort),
            Arc::new(right_key),
            7.into(),
        );

        let can_produce = merge.add_batch(1, right_batch);
        assert!(can_produce);

        let merged = merge.merge().unwrap();

        let step_0_array = new_null_array(&step_0_datatype, 2);
        let expected_fields: Vec<(FieldRef, ArrayRef)> = vec![
            (
                Arc::new(Field::new("step_0", left.clone(), true)),
                Arc::new(step_0_array),
            ),
            (
                Arc::new(Field::new("step_1", right.clone(), true)),
                Arc::new(UInt32Array::from(vec![100, 101])),
            ),
        ];
        let expected_data = Arc::new(sparrow_arrow::utils::make_struct_array(2, expected_fields));
        let expected_time = Arc::new(TimestampNanosecondArray::from(vec![5, 6]));
        let expected_subsort = Arc::new(UInt64Array::from(vec![0, 0]));
        let expected_key = Arc::new(UInt64Array::from(vec![0, 1]));
        let expected = Batch::new_with_data(
            expected_data,
            expected_time,
            expected_subsort,
            expected_key,
            7.into(),
        );

        assert_eq!(merged, expected);
        assert_eq!(merge.blocking_input(), Some(1));
    }
}
