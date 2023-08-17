use std::sync::Arc;

use crate::ValueRef;
use anyhow::anyhow;
use arrow::array::{Array, ArrayRef, BooleanArray, PrimitiveArray, UInt32Array};
use arrow::datatypes::{ArrowNativeType, Int64Type};
use itertools::izip;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sparrow_arrow::downcast::downcast_primitive_array;

use crate::{
    AggregationArgs, ArrowAggFn, Evaluator, RuntimeInfo, StateToken, TwoStacks,
    TwoStacksPrimitiveAccumToken,
};

/// Evaluator for arrow aggregations.
///
/// This evaluator is specialized for aggregation functions `AggF`.
///
/// This evaluator supports both non-windowed aggregations and aggregations
/// with a `since` window, as they have similar aggregation implementations.
pub(crate) struct TwoStacksArrowAggEvaluator<AggF>
where
    AggF: ArrowAggFn,
    AggF::InT: ArrowNativeType,
    AggF::AccT: Serialize + DeserializeOwned,
{
    pub(crate) args: AggregationArgs<ValueRef>,
    pub(crate) token: TwoStacksPrimitiveAccumToken<AggF>,
}

impl<AggF> Evaluator for TwoStacksArrowAggEvaluator<AggF>
where
    AggF: ArrowAggFn + Send,
    AggF::InT: ArrowNativeType,
    AggF::AccT: Serialize + DeserializeOwned + Sync,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        // Note: Consider splitting the State/Evaluator into a windowed and non-windowed
        // implementation in order to avoid matching the arguments each evaluation.
        match &self.args {
            AggregationArgs::Sliding {
                input,
                ticks,
                duration,
            } => {
                // Get the stored state of the accum
                let mut accum = self.token.get_primitive_accum()?;

                let grouping = info.grouping();
                let input_vals = info.value(input)?.array_ref()?;
                let ticks = info.value(ticks)?.boolean_array()?;
                let duration = info
                    .value(duration)?
                    .try_primitive_literal::<Int64Type>()?
                    .ok_or_else(|| anyhow!("Expected non-null literal duration"))?;
                if duration <= 0 {
                    anyhow::bail!(
                        "Expected positive duration for sliding window, saw {:?}",
                        duration
                    );
                }
                let result = Self::aggregate(
                    &mut accum,
                    grouping.num_groups(),
                    grouping.group_indices(),
                    &input_vals,
                    duration,
                    ticks.as_ref(),
                );

                // Store the new state
                self.token.put_primitive_accum(accum)?;

                result
            }
            AggregationArgs::NoWindow { .. } | AggregationArgs::Since { .. } => {
                unreachable!(
                    "Expected Sliding Window aggregation, saw Non-windowed or Since window \
                     aggregation."
                )
            }
        }
    }

    fn state_token(&self) -> Option<&dyn StateToken> {
        Some(&self.token)
    }

    fn state_token_mut(&mut self) -> Option<&mut dyn StateToken> {
        Some(&mut self.token)
    }
}

impl<AggF> TwoStacksArrowAggEvaluator<AggF>
where
    AggF: ArrowAggFn + Send,
    AggF::InT: ArrowNativeType,
    AggF::AccT: Serialize + DeserializeOwned,
{
    fn ensure_entity_capacity(
        accum: &mut Vec<TwoStacks<AggF>>,
        entity_capacity: usize,
        window_parts: i64,
    ) {
        if entity_capacity > accum.len() {
            accum.resize(entity_capacity, TwoStacks::new(window_parts));
        }
    }

    #[inline]
    fn update_two_stacks_accum(
        accum: &mut [TwoStacks<AggF>],
        entity_index: u32,
        input_is_valid: bool,
        sliding_is_valid: bool,
        input: &AggF::InT,
        sliding: bool,
    ) -> Option<AggF::OutT> {
        let entity_index = entity_index as usize;
        let accum = &mut accum[entity_index];

        if input_is_valid {
            accum.add_input(input);
        };

        let value_to_emit = accum.accum_value();

        if sliding_is_valid && sliding {
            accum.evict();
        };

        AggF::extract(&value_to_emit)
    }

    /// Update the aggregation state with the given inputs and return the
    /// aggregation.
    ///
    /// The `key_capacity` must be greater than all values in the
    /// `entity_indices`.
    ///
    /// # Result
    /// The result is an array containing the result of the aggregation for each
    /// input row.
    ///
    /// # Assumptions
    /// This assumes that the input data has been sorted by occurrence time.
    /// Specifically, no checking is done to ensure that elements appear in the
    /// appropriate order.
    fn aggregate(
        accum: &mut Vec<TwoStacks<AggF>>,
        key_capacity: usize,
        entity_indices: &UInt32Array,
        input: &ArrayRef,
        sliding_duration: i64,
        sliding_window: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        assert_eq!(entity_indices.len(), input.len());

        // Make sure the internal buffers are large enough for the accumulators we may
        // want to store.
        Self::ensure_entity_capacity(accum, key_capacity, sliding_duration);

        let input = downcast_primitive_array::<AggF::InArrowT>(input.as_ref())?;

        // TODO: Handle the case where the input is empty (null_count == len) and we
        // don't need to compute anything.

        let result: PrimitiveArray<AggF::OutArrowT> = match (input.nulls(), sliding_window.nulls())
        {
            (None, None) => {
                let iter = izip!(
                    entity_indices.values(),
                    input.values(),
                    sliding_window.values().iter()
                )
                .map(|(entity_index, input, sliding)| {
                    Self::update_two_stacks_accum(accum, *entity_index, true, true, input, sliding)
                });

                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }
            (Some(input_valid_bits), None) => {
                let iter = izip!(
                    entity_indices.values(),
                    input_valid_bits,
                    input.values(),
                    sliding_window.values().iter()
                )
                .map(|(entity_index, input_is_valid, input, sliding)| {
                    Self::update_two_stacks_accum(
                        accum,
                        *entity_index,
                        input_is_valid,
                        true,
                        input,
                        sliding,
                    )
                });

                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }
            (None, Some(window_valid_bits)) => {
                let iter = izip!(
                    entity_indices.values(),
                    window_valid_bits,
                    input.values(),
                    sliding_window.values().iter()
                )
                .map(|(entity_index, since_is_valid, input, since_bool)| {
                    Self::update_two_stacks_accum(
                        accum,
                        *entity_index,
                        true,
                        since_is_valid,
                        input,
                        since_bool,
                    )
                });

                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }
            (Some(input_valid_bits), Some(window_valid_bits)) => {
                let iter = izip!(
                    entity_indices.values(),
                    input_valid_bits,
                    window_valid_bits,
                    input.values(),
                    sliding_window.values().iter()
                )
                .map(
                    |(entity_index, input_is_valid, since_is_valid, input, since_bool)| {
                        Self::update_two_stacks_accum(
                            accum,
                            *entity_index,
                            input_is_valid,
                            since_is_valid,
                            input,
                            since_bool,
                        )
                    },
                );

                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }
        };

        Ok(Arc::new(result))
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{Float64Type, Int64Type};

    use super::*;
    use crate::{FirstPrimitive, LastPrimitive, Max, Mean, Min, Sum};

    #[test]
    fn test_sliding_sum_f64() {
        let entity_indices = UInt32Array::from(vec![0; 5]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![1f64, 2.5, 3.0, 4.0, 5.0]));
        let sliding = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
        ]);
        let mut accum = Vec::new();

        let output = TwoStacksArrowAggEvaluator::<Sum<Float64Type>>::aggregate(
            &mut accum,
            1,
            &entity_indices,
            &input,
            1,
            &sliding,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(output, &Float64Array::from(vec![1.0, 2.5, 5.5, 9.5, 5.0]));
    }

    #[test]
    fn test_sliding_1_sum_f64() {
        let entity_indices = UInt32Array::from(vec![0; 5]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![1f64, 2.5, 3.0, 4.0, 5.0]));
        let window = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
        ]);
        let mut accum = Vec::new();

        let output = TwoStacksArrowAggEvaluator::<Sum<Float64Type>>::aggregate(
            &mut accum,
            1,
            &entity_indices,
            &input,
            1,
            &window,
        )
        .unwrap();

        let sliding_output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();

        assert_eq!(
            sliding_output,
            &Float64Array::from(vec![1.0, 2.5, 5.5, 9.5, 5.0])
        );
    }

    #[test]
    fn test_multiple_keys_sliding_sum_f64() {
        let entity_indices = UInt32Array::from(vec![0, 0, 1, 0, 1, 2, 1, 0]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            1f64, 2.5, 3.0, 4.4, 5.0, 4.0, 2.5, 10.2,
        ]));
        let sliding = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
        ]);

        let mut accum = Vec::new();

        let output = TwoStacksArrowAggEvaluator::<Sum<Float64Type>>::aggregate(
            &mut accum,
            3,
            &entity_indices,
            &input,
            2,
            &sliding,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(
            output,
            &Float64Array::from(vec![1.0, 3.5, 3.0, 6.9, 8.0, 4.0, 7.5, 17.1])
        );
    }

    #[test]
    fn test_sliding_sum_f64_nulls() {
        let entity_indices = UInt32Array::from(vec![0; 5]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1f64),
            None,
            Some(3.0),
            None,
            None,
        ]));
        let sliding = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
        ]);
        let mut accum = Vec::new();

        let output = TwoStacksArrowAggEvaluator::<Sum<Float64Type>>::aggregate(
            &mut accum,
            1,
            &entity_indices,
            &input,
            2,
            &sliding,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(output, &Float64Array::from(vec![1.0, 1.0, 4.0, 4.0, 3.0]));
    }

    #[test]
    fn test_sliding_sum_i64() {
        let entity_indices = UInt32Array::from(vec![0; 5]);
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let sliding = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
        ]);
        let mut accum = Vec::new();

        let output = TwoStacksArrowAggEvaluator::<Sum<Int64Type>>::aggregate(
            &mut accum,
            1,
            &entity_indices,
            &input,
            2,
            &sliding,
        )
        .unwrap();

        let output = downcast_primitive_array::<Int64Type>(output.as_ref()).unwrap();
        assert_eq!(output, &Int64Array::from(vec![1, 3, 5, 9, 12]));
    }

    #[test]
    fn test_sliding_max_i64() {
        let entity_indices = UInt32Array::from(vec![0; 5]);
        let input: ArrayRef = Arc::new(Int64Array::from(vec![10, 2, 3, -3, 5]));
        let sliding = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
        ]);
        let mut accum = Vec::new();

        let output = TwoStacksArrowAggEvaluator::<Max<Int64Type>>::aggregate(
            &mut accum,
            1,
            &entity_indices,
            &input,
            2,
            &sliding,
        )
        .unwrap();

        let output = downcast_primitive_array::<Int64Type>(output.as_ref()).unwrap();
        assert_eq!(output, &Int64Array::from(vec![10, 10, 3, 3, 5]));
    }

    #[test]
    fn test_sliding_min_i64() {
        let entity_indices = UInt32Array::from(vec![0; 5]);
        let input: ArrayRef = Arc::new(Int64Array::from(vec![10, 2, 3, -3, 5]));
        let sliding = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
        ]);
        let mut accum = Vec::new();

        let output = TwoStacksArrowAggEvaluator::<Min<Int64Type>>::aggregate(
            &mut accum,
            1,
            &entity_indices,
            &input,
            2,
            &sliding,
        )
        .unwrap();

        let output = downcast_primitive_array::<Int64Type>(output.as_ref()).unwrap();
        assert_eq!(output, &Int64Array::from(vec![10, 2, 2, -3, -3]));
    }

    #[test]
    fn test_sliding_mean_f64() {
        let entity_indices = UInt32Array::from(vec![0; 5]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![1f64, 2.0, 3.0, 4.0, 5.0]));
        let sliding = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(true),
        ]);
        let mut accum = Vec::new();

        let output = TwoStacksArrowAggEvaluator::<Mean<Float64Type>>::aggregate(
            &mut accum,
            1,
            &entity_indices,
            &input,
            2,
            &sliding,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(output, &Float64Array::from(vec![1.0, 1.5, 2.5, 3.0, 4.0]));
    }

    #[test]
    fn test_sliding_first_f64() {
        let entity_indices = UInt32Array::from(vec![0; 5]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![1f64, 2.0, 3.0, 4.0, 5.0]));
        let sliding = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(true),
        ]);
        let mut accum = Vec::new();

        let output = TwoStacksArrowAggEvaluator::<FirstPrimitive<Float64Type>>::aggregate(
            &mut accum,
            1,
            &entity_indices,
            &input,
            2,
            &sliding,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(output, &Float64Array::from(vec![1.0, 1.0, 2.0, 2.0, 3.0]));
    }

    #[test]
    fn test_sliding_last_f64() {
        let entity_indices = UInt32Array::from(vec![0; 5]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![1f64, 2.0, 3.0, 4.0, 5.0]));
        let sliding = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(true),
        ]);
        let mut accum = Vec::new();

        let output = TwoStacksArrowAggEvaluator::<LastPrimitive<Float64Type>>::aggregate(
            &mut accum,
            1,
            &entity_indices,
            &input,
            2,
            &sliding,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(output, &Float64Array::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
    }
}
