use std::sync::Arc;

use super::BoxedOperation;
use crate::execute::operation::expression_executor::InputColumn;
use crate::execute::operation::spread::Spread;
use crate::execute::operation::{InputBatch, Operation};
use crate::execute::Error;
use crate::key_hash_index::KeyHashIndex;
use crate::Batch;
use anyhow::Context;
use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::DataType;
use async_trait::async_trait;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use futures::stream::StreamExt;
use futures::FutureExt;
use itertools::Itertools;
use sparrow_api::kaskada::v1alpha::operation_input_ref::Interpolation;
use sparrow_api::kaskada::v1alpha::operation_plan;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_core::{KeyTriple, KeyTriples};
use sparrow_instructions::{ComputeStore, GroupingIndices, StoreKey};
use sparrow_merge::old::{binary_merge, BinaryMergeInput};
use tokio_stream::wrappers::ReceiverStream;

#[derive(Debug)]
pub(super) struct MergeOperation {
    /// Indices (and side) of columns needed within the merge operation.
    input_columns: Vec<MergeInputColumn>,
    /// The type each input column is expected to produce.
    ///
    /// We can't store these in the `MergeInputColumn` because we serialize
    /// that. In addition to being potentially inefficient to serialize the
    /// static information that we already know, it doesn't work due to the use
    /// of `bincode` (or other binary serialization formats) and the fact that
    /// `DataType` has a skipped field, due to
    /// https://github.com/serde-rs/serde/issues/1732 and
    /// https://github.com/apache/arrow-rs/issues/3082.
    input_types: Vec<DataType>,
    left_state: MergeState,
    right_state: MergeState,
    left_stream: ReceiverStream<Batch>,
    right_stream: ReceiverStream<Batch>,
    key_hash_index: KeyHashIndex,
}

#[async_trait]
impl Operation for MergeOperation {
    fn restore_from(
        &mut self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()> {
        // TODO: Restore the spread buffer state.

        self.key_hash_index
            .restore_from(operation_index, compute_store)?;

        if let Some(input_columns) =
            compute_store.get(&StoreKey::new_merge_state(operation_index))?
        {
            self.input_columns = input_columns;
        } else {
            // TODO: Clear the state.
        }
        Ok(())
    }

    fn store_to(&self, operation_index: u8, compute_store: &ComputeStore) -> anyhow::Result<()> {
        self.key_hash_index
            .store_to(operation_index, compute_store)?;
        compute_store.put(
            &StoreKey::new_merge_state(operation_index),
            &self.input_columns,
        )?;
        Ok(())
    }

    async fn execute(
        &mut self,
        sender: tokio::sync::mpsc::Sender<InputBatch>,
    ) -> error_stack::Result<(), Error> {
        while let Some(input) = self
            .try_next()
            .await
            .into_report()
            .change_context(Error::internal())?
        {
            sender
                .send(input)
                .await
                .into_report()
                .change_context(Error::internal())?;
        }
        Ok(())
    }
}

impl MergeOperation {
    /// Create an operation which merges two input streams.
    ///
    /// The result of an `OperationInput` expression will be the input from the
    /// corresponding input operation "spread out" to the merged domain.
    ///
    /// # Note on Approach
    /// There are a variety of approaches for this, depending on what the goal
    /// is. On one end, we could output batches as soon as possible regardless
    /// of the number of rows, while on the other end we could collect inputs
    /// until we're able to produce an output of a certain size. Additionally,
    /// if we need to "wait" to produce output, when we're next able to produce
    /// an output we could attempt to make it as large as possible (to minimize
    /// the data "held up here").
    ///
    /// For now, we use relatively simple heuristics.
    ///
    /// ## Improvements
    ///
    /// Before significant improvements are made, to how rows are collected,
    /// we would ideally first make some improvements to the implementation.
    ///
    /// 1. Instead of using `take` and indices, we could instead have a `spread`
    ///    kernel that just uses true/false. The benefit is that we could
    ///    concatenate bits to get a configuration that would work on
    ///    concatenated input. With `take` and indices, we would need to offset
    ///    everything.
    ///
    /// 2. Identifying which input columns are needed from the merge, so that we
    ///    can merge those columns eagerly, allowing us to just buffer and
    ///    concatenate the merged output.
    pub(super) fn create(
        merge_operation: operation_plan::MergeOperation,
        input_channels: Vec<tokio::sync::mpsc::Receiver<Batch>>,
        input_columns: &[InputColumn],
    ) -> error_stack::Result<BoxedOperation, super::Error> {
        let (left_rx, right_rx) = input_channels
            .into_iter()
            .collect_tuple()
            .ok_or_else(|| Error::internal_msg("expected 2 input channels"))
            .into_report()?;

        let left_stream = ReceiverStream::new(left_rx);
        let right_stream = ReceiverStream::new(right_rx);

        let left_operation = merge_operation.left;
        let right_operation = merge_operation.right;

        let input_types = input_columns
            .iter()
            .map(|input_column| input_column.data_type.clone())
            .collect();

        let input_columns = input_columns
            .iter()
            .map(|input_column| {
                let input_column_index = input_column.input_ref.input_column;

                let requested_operation = input_column.input_ref.producing_operation;
                let side = if requested_operation == left_operation {
                    MergeSide::Left
                } else if requested_operation == right_operation {
                    MergeSide::Right
                } else {
                    error_stack::bail!(Error::internal_msg(
                        "Expected input to be from left({left_operation}) or right({right_operation}) but was {requested_operation}"
                    ));
                };
                let spread = match input_column.input_ref.interpolation() {
                    Interpolation::Unspecified => {
                        error_stack::bail!(Error::internal_msg("Unspecified interpolation"))
                    }
                    Interpolation::Null => Spread::try_new(false, &input_column.data_type).into_report().change_context(Error::internal_msg("spead failure"))?,
                    Interpolation::AsOf => Spread::try_new(true, &input_column.data_type).into_report().change_context(Error::internal_msg("spread failure"))?,
                };
                Ok(MergeInputColumn {
                    side,
                    index: input_column_index as usize,
                    spread,
                })
            })
            .try_collect()?;

        Ok(Box::new(Self {
            input_columns,
            input_types,
            left_state: MergeState::None,
            right_state: MergeState::None,
            left_stream,
            right_stream,
            key_hash_index: KeyHashIndex::default(),
        }))
    }

    /// Produce the next input batch.
    ///
    /// TODO: This is an artifact of the old operation API. It may be possible
    /// to cleanup the `merge` implementation by moving it to occur directly
    /// within the `execute` logic.
    async fn try_next(&mut self) -> anyhow::Result<Option<InputBatch>> {
        // First, make sure both sides have batches (or have reached the end of
        // the stream). This relies on the fact that a `Fuse::terminated()`
        // future will never be started by the select to indicate
        // "nothing to do for this side".
        let mut left = if self.left_state.is_none() {
            self.left_stream.next().map(MergeState::try_new).fuse()
        } else {
            futures::future::Fuse::terminated()
        };
        let mut right = if self.right_state.is_none() {
            self.right_stream.next().map(MergeState::try_new).fuse()
        } else {
            futures::future::Fuse::terminated()
        };

        loop {
            futures::select! {
                next_left = left => {
                    self.left_state = next_left?;
                    left = if self.left_state.is_none() {
                        self.left_stream.next().map(MergeState::try_new).fuse()
                    } else {
                         futures::future::Fuse::terminated()
                    };
                }
                next_right = right => {
                    self.right_state = next_right?;
                    right = if self.right_state.is_none() {
                        self.right_stream.next().map(MergeState::try_new).fuse()
                    } else {
                        futures::future::Fuse::terminated()
                    };
                }
                complete => {
                    // Both `left` and `right` have been updated to `Fuse::terminated()`.
                    // This means that we have input (or `Done`) on both sides and can
                    // return the next merged batch.
                    return self.merge();
                }
            }
        }
    }

    /// Perform the actual merging of the `left` and `right` state.
    ///
    /// The states are updated to reflect the *remaining* rows after the merge.
    /// The actual merged input batch is returned (if any). Returning `Ok(None)`
    /// indicates that merging is done.
    ///
    /// The rows that are merged are chosen so that any later merged batches are
    /// guaranteed to include only rows greater than those already merged. Since
    /// each side of the merge produces rows in order, we know that any future
    /// rows on the left are greater than the current `left_max`. Similarly,
    /// future rows on the right are greater than `right_max`. It follows
    /// that future rows from either side are greater than `min(left_max,
    /// right_max)`, which is how the maximum row included in the merge is
    /// determined.
    fn merge(&mut self) -> anyhow::Result<Option<InputBatch>> {
        let left_state = std::mem::replace(&mut self.left_state, MergeState::None);
        let right_state = std::mem::replace(&mut self.right_state, MergeState::None);

        match (left_state, right_state) {
            (MergeState::Done, MergeState::Done) => {
                // Both sides are done -- nothing more to output.
                Ok(None)
            }
            (MergeState::Some(left_batch), MergeState::Some(right_batch)) => {
                // The maximum element in the merged result is the minimum of the left and
                // right batches' upper bounds. This ensures that any future elements
                // are after the maximum included element.
                // Note the last key in the batch may be less than the batch's upper bound.
                let max_merged = left_batch
                    .batch
                    .upper_bound
                    .min(right_batch.batch.upper_bound);
                let min_merged = left_batch
                    .batch
                    .lower_bound
                    .min(right_batch.batch.lower_bound);

                if left_batch.batch.num_rows() == 0 && right_batch.batch.num_rows() == 0 {
                    let input_columns = self
                        .input_columns
                        .iter_mut()
                        .zip(self.input_types.iter())
                        .map(|(input, data_type)| {
                            input.input_column(
                                &GroupingIndices::new_empty(),
                                &MergeInputData::None,
                                &MergeInputData::None,
                                data_type,
                            )
                        })
                        .try_collect()?;

                    return Ok(Some(InputBatch {
                        time: left_batch.batch.column(0).clone(),
                        subsort: left_batch.batch.column(1).clone(),
                        key_hash: left_batch.batch.column(2).clone(),
                        grouping: GroupingIndices::new_empty(),
                        input_columns,
                        lower_bound: min_merged,
                        upper_bound: max_merged,
                    }));
                }

                let (left_batch, left_remainder) = left_batch.split_at(&max_merged)?;
                let (right_batch, right_remainder) = right_batch.split_at(&max_merged)?;

                // Install the remainders before returning.
                self.left_state = left_remainder;
                self.right_state = right_remainder;

                anyhow::ensure!(self.left_state.is_none() || self.right_state.is_none());

                Ok(Some(self.create_left_and_right(
                    left_batch,
                    right_batch,
                    min_merged,
                    max_merged,
                )?))
            }
            (MergeState::Some(left_batch), MergeState::Done) => {
                // Replace the right state back with [MergeState::Done].
                self.right_state = MergeState::Done;

                Ok(Some(self.create_one(MergeSide::Left, left_batch.batch)?))
            }
            (MergeState::Done, MergeState::Some(right_batch)) => {
                // Replace the left state back with [MergeState::Done].
                self.left_state = MergeState::Done;

                Ok(Some(self.create_one(MergeSide::Right, right_batch.batch)?))
            }
            (left, right) => Err(anyhow::anyhow!(
                "Unexpected merge states: left={left:?}, right={right:?}"
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn create_input(
        &mut self,
        time: ArrayRef,
        subsort: ArrayRef,
        key_hash: ArrayRef,
        left: MergeInputData,
        right: MergeInputData,
        lower_bound: KeyTriple,
        upper_bound: KeyTriple,
    ) -> anyhow::Result<InputBatch> {
        let grouping = self
            .key_hash_index
            .get_or_update_indices(downcast_primitive_array(key_hash.as_ref())?)?;

        let input_columns = self
            .input_columns
            .iter_mut()
            .zip(self.input_types.iter())
            .map(|(input, data_type)| input.input_column(&grouping, &left, &right, data_type))
            .try_collect()?;

        Ok(InputBatch {
            time,
            subsort,
            key_hash,
            grouping,
            input_columns,
            lower_bound,
            upper_bound,
        })
    }

    /// Create an input when there is only one input.
    ///
    /// This means that one side of the stream is completely done.
    /// Create the batch to operate on by re-using the columns.
    fn create_one(&mut self, side: MergeSide, batch: Batch) -> anyhow::Result<InputBatch> {
        let time = batch.column(0).clone();
        let subsort = batch.column(1).clone();
        let key_hash = batch.column(2).clone();

        let lower_bound = batch.lower_bound;
        let upper_bound = batch.upper_bound;

        let (left, right) = match side {
            MergeSide::Left => (MergeInputData::Only(batch), MergeInputData::None),
            MergeSide::Right => (MergeInputData::None, MergeInputData::Only(batch)),
        };

        self.create_input(
            time,
            subsort,
            key_hash,
            left,
            right,
            lower_bound,
            upper_bound,
        )
    }

    /// Create an input when there is input on both sides.
    fn create_left_and_right(
        &mut self,
        left: Batch,
        right: Batch,
        lower_bound: KeyTriple,
        upper_bound: KeyTriple,
    ) -> anyhow::Result<InputBatch> {
        let left_input = BinaryMergeInput::from_batch(&left.data)?;
        let right_input = BinaryMergeInput::from_batch(&right.data)?;
        let merged_result = binary_merge(left_input, right_input)?;

        // TODO: Optimization
        //  We only need take bits (not take indices). We can simplify `binary_merge` to
        // only return these.
        let left_spread_bits = arrow::compute::is_not_null(&merged_result.take_a)?;
        let right_spread_bits = arrow::compute::is_not_null(&merged_result.take_b)?;

        let time = Arc::new(merged_result.time);
        let subsort = Arc::new(merged_result.subsort);
        let key_hash = Arc::new(merged_result.key_hash);

        self.create_input(
            time,
            subsort,
            key_hash,
            MergeInputData::Both(left, left_spread_bits),
            MergeInputData::Both(right, right_spread_bits),
            lower_bound,
            upper_bound,
        )
    }
}

/// State for one side of the merge.
#[derive(Debug)]
enum MergeState {
    /// The stream is done and no more input is expected.
    ///
    /// The other side should output batches as they arrive.
    Done,
    /// The stream has no buffered input on this side.
    ///
    /// The next batch from this side must be received before merging with the
    /// other side.
    None,
    /// The stream has a buffered batch on this side.
    ///
    /// This batch may be merged with a batch from the other side without
    /// waiting for more input.
    Some(KeyedBatch),
}

impl MergeState {
    /// Creates a new `MergeState` from the next batch in the stream.
    ///
    /// When the next batch is `None` we have reached the end of the stream.
    fn try_new(next_batch: Option<Batch>) -> anyhow::Result<Self> {
        match next_batch {
            None => Ok(Self::Done),
            Some(batch) => Ok(Self::Some(KeyedBatch::try_new(batch)?)),
        }
    }

    fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }
}

/// A batch to-be-merged associated with the keys.
#[derive(Debug)]
struct KeyedBatch {
    batch: Batch,
    keys: KeyTriples,
    // If empty, this batch has no rows.
    last_key: Option<KeyTriple>,
}

impl KeyedBatch {
    fn try_new(batch: Batch) -> anyhow::Result<KeyedBatch> {
        let keys = KeyTriples::try_new(
            batch.column(0).clone(),
            batch.column(1).clone(),
            batch.column(2).clone(),
        )?;
        let last_key = keys.last();
        Ok(Self {
            batch,
            keys,
            last_key,
        })
    }

    fn is_empty(&self) -> bool {
        self.batch.data.num_rows() == 0
    }

    /// Split this keyed batch at the given key.
    ///
    /// If the given key is greater than the max key in the batch, the entire
    /// batch is returned and there is no remainder.
    ///
    /// Returns the rows that are less than or equal to the key (ready to be
    /// merged) and the updated `MergeState` containing the remaining (unmerged)
    /// rows.
    fn split_at(self, key_inclusive: &KeyTriple) -> anyhow::Result<(Batch, MergeState)> {
        if let Some(last_key) = &self.last_key {
            if key_inclusive >= last_key {
                // If we're including everything from this batch, there is no remainder.
                Ok((self.batch, MergeState::None))
            } else {
                // The number of elements in <= to key.
                let split_length = self.keys.run_length(0, key_inclusive);
                let to_merge = self.batch.data.slice(0, split_length);

                let new_upper_bound = if split_length == 0 {
                    self.keys.value(0)
                } else {
                    self.keys.value(split_length - 1)
                };
                let to_merge =
                    Batch::try_new_with_bounds(to_merge, self.batch.lower_bound, new_upper_bound)?;

                let residual_offset = split_length;
                let residual_length = self.batch.num_rows() - residual_offset;
                debug_assert!(residual_length > 0);

                // The residual batch should maintain the same upper bound, but create
                // a new lower bound.
                let slice = self.batch.data.slice(residual_offset, residual_length);
                let new_lower_bound = KeyTriples::try_from(&slice)?
                    .first()
                    .context("First key triple")?;
                let residual_batch =
                    Batch::try_new_with_bounds(slice, new_lower_bound, self.batch.upper_bound)?;
                let residual = Self {
                    batch: residual_batch,
                    keys: self.keys.slice(residual_offset, residual_length),
                    last_key: self.last_key,
                };

                debug_assert_eq!(
                    self.batch.num_rows(),
                    to_merge.num_rows() + residual.batch.num_rows()
                );
                debug_assert!(residual.batch.num_rows() > 0);
                Ok((to_merge, MergeState::Some(residual)))
            }
        } else {
            // If the batch is empty, there is no remainder.
            debug_assert!(self.is_empty());
            Ok((self.batch, MergeState::None))
        }
    }
}

/// An input column needed within the merge operation.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct MergeInputColumn {
    side: MergeSide,
    index: usize,
    /// Implementation of `spread` to use for this column.
    spread: Spread,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
enum MergeSide {
    Left,
    Right,
}

enum MergeInputData {
    /// The side of the merge with this input is done,
    /// and will never produce input again.
    None,
    /// There was nothing on the *other* side of the merge.
    ///
    /// The input columns can be used directly.
    Only(Batch),
    /// There was data on both sides of the merge.
    /// Data from this side needs to be spread.
    Both(Batch, BooleanArray),
}

impl std::fmt::Debug for MergeInputData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeInputData::None => f.write_str("None"),
            MergeInputData::Only(_) => f.write_str("Only"),
            MergeInputData::Both(_, _) => f.write_str("Both"),
        }
    }
}

impl MergeInputColumn {
    fn input_column(
        &mut self,
        grouping: &GroupingIndices,
        left: &MergeInputData,
        right: &MergeInputData,
        data_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        let data = match self.side {
            MergeSide::Left => left,
            MergeSide::Right => right,
        };

        let spread = &mut self.spread;
        match data {
            MergeInputData::None => spread.spread_false(grouping, data_type),
            MergeInputData::Only(batch) => spread.spread_true(grouping, batch.column(self.index)),
            MergeInputData::Both(batch, signal) => {
                spread.spread_signaled(grouping, batch.column(self.index), signal)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::TimestampNanosecondArray;
    use sparrow_api::kaskada::v1alpha::{self, data_type};
    use sparrow_api::kaskada::v1alpha::{
        expression_plan, operation_input_ref, operation_plan, ExpressionPlan, OperationInputRef,
        OperationPlan,
    };
    use sparrow_arrow::downcast::downcast_primitive_array;
    use sparrow_core::KeyTriple;

    use super::KeyedBatch;
    use crate::execute::operation::testing::{batch_from_csv, run_operation};
    use crate::Batch;

    // The (hypothetical) producing operations this merge receives from are:
    //
    // - A scan that outputs a single column at (0.0) containing an i64
    // - A scan that outputs two columns from (1.0) and (1.2). The latter contains
    //   an i64 that is read by the merge and added to the 0.0.
    fn default_plan() -> OperationPlan {
        OperationPlan {
            expressions: vec![
                ExpressionPlan {
                    arguments: vec![],
                    result_type: Some(v1alpha::DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::I64 as i32,
                        )),
                    }),
                    output: false,
                    operator: Some(expression_plan::Operator::Input(OperationInputRef {
                        producing_operation: 0,
                        column: None,
                        input_column: 3,
                        interpolation: operation_input_ref::Interpolation::Null as i32,
                    })),
                },
                ExpressionPlan {
                    arguments: vec![],
                    result_type: Some(v1alpha::DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::I64 as i32,
                        )),
                    }),
                    output: false,
                    operator: Some(expression_plan::Operator::Input(OperationInputRef {
                        producing_operation: 1,
                        column: None,
                        input_column: 4,
                        interpolation: operation_input_ref::Interpolation::Null as i32,
                    })),
                },
                ExpressionPlan {
                    arguments: vec![0, 1],
                    result_type: Some(v1alpha::DataType {
                        kind: Some(data_type::Kind::Primitive(
                            data_type::PrimitiveType::I64 as i32,
                        )),
                    }),
                    output: true,
                    operator: Some(expression_plan::Operator::Instruction("add".to_owned())),
                },
            ],
            operator: Some(operation_plan::Operator::Merge(
                operation_plan::MergeOperation { left: 0, right: 1 },
            )),
        }
    }

    #[tokio::test]
    async fn test_merge() {
        let plan = default_plan();

        let operation_0 = batch_from_csv(
            "
        _time,_subsort,_key_hash,e0
        1970-01-01T00:00:00.000002000,0,1,1
        1970-01-01T00:00:00.000003000,1,1,2
        1970-01-01T00:00:00.000004000,0,2,3",
            None,
        )
        .unwrap();
        let operation_1 = batch_from_csv(
            "
        _time,_subsort,_key_hash,e0,e2
        1970-01-01T00:00:00.000002500,0,2,1,3
        1970-01-01T00:00:00.000003000,0,1,2,2
        1970-01-01T00:00:00.000003000,1,1,4,4
        1970-01-01T00:00:00.000004000,0,2,3,2",
            None,
        )
        .unwrap();

        insta::assert_snapshot!(run_operation(vec![operation_0, operation_1], plan).await.unwrap(), @r###"
        _time,_subsort,_key_hash,e2
        1970-01-01T00:00:00.000002000,0,1,
        1970-01-01T00:00:00.000002500,0,2,
        1970-01-01T00:00:00.000003000,0,1,
        1970-01-01T00:00:00.000003000,1,1,6
        1970-01-01T00:00:00.000004000,0,2,5
        "###)
    }

    #[tokio::test]
    #[ignore = "https://github.com/kaskada-ai/kaskada/issues/524"]
    async fn test_merge_drops_duplicate_rows() {
        let plan = default_plan();
        let operation_0 = batch_from_csv(
            "
        _time,_subsort,_key_hash,e0
        1970-01-01T00:00:00.000002000,0,1,1
        1970-01-01T00:00:00.000003000,1,1,2
        1970-01-01T00:00:00.000004000,0,2,3",
            None,
        )
        .unwrap();
        let operation_1 = batch_from_csv(
            "
        _time,_subsort,_key_hash,e0
        1970-01-01T00:00:00.000002000,0,1,1",
            None,
        )
        .unwrap();

        insta::assert_snapshot!(run_operation(vec![operation_0, operation_1], plan).await.unwrap(), @r###"
        _time,_subsort,_key_hash,e2
        "###)
    }

    #[test]
    fn test_split_at() {
        let times = [0, 1, 2, 3, 4, 5, 10];
        let times = TimestampNanosecondArray::from_iter_values(times.iter().copied());
        let batch = Batch::test_batch(times, 0, 0);
        let keyed_batch = KeyedBatch::try_new(batch).unwrap();
        let split = KeyTriple {
            time: 3,
            subsort: 0,
            key_hash: 0,
        };
        let result = keyed_batch.split_at(&split).unwrap();

        let expected_times = [0, 1, 2, 3];
        let expected_times =
            TimestampNanosecondArray::from_iter_values(expected_times.iter().copied());
        let actual_times: &TimestampNanosecondArray =
            downcast_primitive_array(result.0.data.column(0).as_ref()).unwrap();
        assert_eq!(&expected_times, actual_times);

        let expected_lower = KeyTriple {
            time: 0,
            subsort: 0,
            key_hash: 0,
        };
        assert_eq!(expected_lower, result.0.lower_bound);

        let expected_upper = KeyTriple {
            time: 3,
            subsort: 0,
            key_hash: 0,
        };
        assert_eq!(expected_upper, result.0.upper_bound);
    }
}
