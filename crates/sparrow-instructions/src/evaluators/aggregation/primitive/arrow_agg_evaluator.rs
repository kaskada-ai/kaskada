use std::sync::Arc;

use crate::ValueRef;
use arrow::array::{Array, ArrayRef, BooleanArray, PrimitiveArray, UInt32Array};
use arrow::datatypes::ArrowNativeType;
use itertools::izip;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sparrow_arrow::downcast::downcast_primitive_array;

use super::two_stacks_arrow_agg_evaluator::TwoStacksArrowAggEvaluator;
use crate::evaluators::aggregation::function::agg_fn::ArrowAggFn;
use crate::{
    AggregationArgs, Evaluator, EvaluatorFactory, PrimitiveAccumToken, RuntimeInfo, StateToken,
    StaticInfo, TwoStacksPrimitiveAccumToken,
};

/// Evaluator for arrow aggregations.
///
/// This evaluator is specialized for aggregation functions `AggF`.
///
/// This evaluator supports both non-windowed aggregations and aggregations
/// with a `since` window, as they have similar aggregation implementations.
pub struct ArrowAggEvaluator<AggF>
where
    AggF: ArrowAggFn,
    AggF::AccT: Serialize + DeserializeOwned,
{
    token: PrimitiveAccumToken<Option<AggF::AccT>>,
    args: AggregationArgs<ValueRef>,
}

impl<AggF> Evaluator for ArrowAggEvaluator<AggF>
where
    AggF: ArrowAggFn,
    AggF::InT: arrow::datatypes::ArrowNativeType,
    AggF::AccT: Serialize + DeserializeOwned + Sync,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        match &self.args {
            AggregationArgs::NoWindow { input } => {
                // Get the stored state of the accum
                let mut accum = self.token.get_primitive_accum()?;

                let grouping = info.grouping();
                let input_vals = info.value(input)?.array_ref()?;
                let result = Self::aggregate(
                    &mut accum,
                    grouping.num_groups(),
                    grouping.group_indices(),
                    &input_vals,
                );

                // Store the new state
                self.token.put_primitive_accum(accum)?;

                result
            }
            AggregationArgs::Since { ticks, input } => {
                // Get the stored state of the accum
                let mut accum = self.token.get_primitive_accum()?;

                let grouping = info.grouping();
                let input_vals = info.value(input)?.array_ref()?;
                let ticks = info.value(ticks)?.boolean_array()?;
                let result = Self::aggregate_since(
                    &mut accum,
                    grouping.num_groups(),
                    grouping.group_indices(),
                    &input_vals,
                    ticks.as_ref(),
                );

                // Store the new state
                self.token.put_primitive_accum(accum)?;

                result
            }
            AggregationArgs::Sliding { .. } => {
                unreachable!("Expected Non-windowed or Since windowed aggregation, saw Sliding.")
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

impl<AggF> EvaluatorFactory for ArrowAggEvaluator<AggF>
where
    AggF: ArrowAggFn + Send + 'static,
    AggF::InT: ArrowNativeType,
    AggF::AccT: Serialize + DeserializeOwned + Sync,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let args = AggregationArgs::from_input(info.args)?;
        match args {
            AggregationArgs::NoWindow { .. } | AggregationArgs::Since { .. } => {
                let token = PrimitiveAccumToken::default();
                Ok(Box::new(Self { token, args }))
            }
            AggregationArgs::Sliding { .. } => {
                let token = TwoStacksPrimitiveAccumToken::new();
                Ok(Box::new(TwoStacksArrowAggEvaluator::<AggF> { token, args }))
            }
        }
    }
}

impl<AggF> ArrowAggEvaluator<AggF>
where
    AggF: ArrowAggFn,
    AggF::InT: ArrowNativeType,
    AggF::AccT: Serialize + DeserializeOwned,
{
    fn ensure_entity_capacity(accum: &mut Vec<Option<AggF::AccT>>, entity_id_len: usize) {
        if entity_id_len > accum.len() {
            accum.resize(entity_id_len, None);
        }
    }

    /// Updates the accumulator based on the given flags.
    ///
    /// Accumulator behavior is to update -> emit -> reset, resulting in
    /// exclusive start bounds and inclusive end bounds.
    ///
    /// Implements a single row of the logic so that we can easily reuse it.
    /// We choose to inline this so that it can be specialized in cases where
    /// the valid bits are always true.
    #[inline]
    fn update_accum(
        accum: &mut [Option<AggF::AccT>],
        entity_index: u32,
        input_is_valid: bool,
        tick_is_valid: bool,
        input: &AggF::InT,
        tick: bool,
    ) -> Option<AggF::OutT> {
        let entity_index = entity_index as usize;
        let accum = &mut accum[entity_index];

        if input_is_valid {
            // Update the existing accumulator if it exists, or just add the single input
            if let Some(accum) = accum {
                AggF::add_one(accum, input);
            } else {
                *accum = Some(AggF::one(input));
            }
        }
        let value_to_emit = match accum.as_ref() {
            Some(accum) => AggF::extract(accum),
            None => None,
        };

        if tick_is_valid && tick {
            // If `since` is true, clear the accumulator
            *accum = None;
        }

        value_to_emit
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
        accum: &mut Vec<Option<AggF::AccT>>,
        key_capacity: usize,
        entity_indices: &UInt32Array,
        input: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        assert_eq!(entity_indices.len(), input.len());

        // Make sure the accum vec is large enough for the entity accumulators we want
        // to store.
        Self::ensure_entity_capacity(accum, key_capacity);

        let input = downcast_primitive_array::<AggF::InArrowT>(input.as_ref())?;

        // TODO: Handle the case where the input is empty (null_count == len) and we
        // don't need to compute anything.
        let result: PrimitiveArray<AggF::OutArrowT> = if let Some(is_valid) = input.nulls() {
            let iter = izip!(is_valid, entity_indices.values(), input.values()).map(
                |(is_valid, entity_index, input)| {
                    let entity_accum = &mut accum[*entity_index as usize];
                    if is_valid {
                        if let Some(entity_accum) = entity_accum {
                            AggF::add_one(entity_accum, input);
                            AggF::extract(entity_accum)
                        } else {
                            let one = AggF::one(input);
                            let result = AggF::extract(&one);
                            *entity_accum = Some(one);
                            result
                        }
                    } else {
                        match entity_accum.as_ref() {
                            Some(accum) => AggF::extract(accum),
                            None => None,
                        }
                    }
                },
            );

            // SAFETY: `izip!` and `map` are trusted length iterators.
            unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
        } else {
            // Handle the case where input contains no nulls. This allows us to
            // use `prim_input.values()` instead of `prim_input.iter()`.
            let iter =
                izip!(entity_indices.values(), input.values()).map(|(entity_index, input)| {
                    let entity_accum = &mut accum[*entity_index as usize];
                    if let Some(entity_accum) = entity_accum {
                        AggF::add_one(entity_accum, input);
                        AggF::extract(entity_accum)
                    } else {
                        let one = AggF::one(input);
                        let result = AggF::extract(&one);
                        *entity_accum = Some(one);
                        result
                    }
                });

            // SAFETY: `izip!` and `map` are trusted length iterators.
            unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
        };
        Ok(Arc::new(result))
    }

    /// Update the aggregation state with the given inputs and return the
    /// aggregation.
    ///
    /// The `key_capacity` must be greater than all values in the
    /// `entity_indices`.
    ///
    /// # Window Behavior
    /// This aggregation uses the `since` window behavior, which takes a single
    /// predicate. If the predicate evaluates to true, the accumulated value is
    /// reset. Values are not accumulated until the first time the predicate
    /// evaluates to true.
    ///
    /// # Result
    /// The result is an array containing the result of the aggregation for each
    /// input row.
    ///
    /// # Assumptions
    /// This assumes that the input data has been sorted by occurrence time.
    /// Specifically, no checking is done to ensure that elements appear in the
    /// appropriate order.
    fn aggregate_since(
        accum: &mut Vec<Option<AggF::AccT>>,
        key_capacity: usize,
        entity_indices: &UInt32Array,
        input: &ArrayRef,
        ticks: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        assert_eq!(entity_indices.len(), input.len());

        // Make sure the internal buffers are large enough for the accumulators we may
        // want to store.
        Self::ensure_entity_capacity(accum, key_capacity);

        let input = downcast_primitive_array::<AggF::InArrowT>(input.as_ref())?;

        // TODO: Handle the case where the input is empty (null_count == len) and we
        // don't need to compute anything.

        let result: PrimitiveArray<AggF::OutArrowT> = match (input.nulls(), ticks.nulls()) {
            (None, None) => {
                let iter = izip!(
                    entity_indices.values(),
                    input.values(),
                    ticks.values().iter()
                )
                .map(|(entity_index, input, tick)| {
                    Self::update_accum(accum, *entity_index, true, true, input, tick)
                });

                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }
            (Some(input_valid_bits), None) => {
                let iter = izip!(
                    entity_indices.values(),
                    input_valid_bits,
                    input.values(),
                    ticks.values().iter()
                )
                .map(|(entity_index, input_is_valid, input, since_bool)| {
                    Self::update_accum(
                        accum,
                        *entity_index,
                        input_is_valid,
                        true,
                        input,
                        since_bool,
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
                    ticks.values().iter()
                )
                .map(|(entity_index, since_is_valid, input, since_bool)| {
                    Self::update_accum(
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
                    ticks.values().iter()
                )
                .map(
                    |(entity_index, input_is_valid, since_is_valid, input, since_bool)| {
                        Self::update_accum(
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
    use crate::{FirstPrimitive, LastPrimitive, Max, Mean, Sum};

    #[test]
    fn test_sum_f64() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![1f64, 2.5, 3.0, 4.0, 5.0]));
        let mut accum = Vec::new();

        let output = ArrowAggEvaluator::<Sum<Float64Type>>::aggregate(
            &mut accum,
            3,
            &entity_indices,
            &input,
        )
        .unwrap();
        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(output, &Float64Array::from(vec![1.0, 2.5, 3.0, 6.5, 11.5]));
    }

    #[test]
    fn test_sum_f64_nulls() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1f64),
            Some(2.5),
            None,
            None,
            Some(5.0),
        ]));
        let mut accum = Vec::new();

        let output = ArrowAggEvaluator::<Sum<Float64Type>>::aggregate(
            &mut accum,
            3,
            &entity_indices,
            &input,
        )
        .unwrap();
        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(
            output,
            &Float64Array::from(vec![Some(1.0), Some(2.5), None, Some(2.5), Some(7.5)])
        );
    }

    #[test]
    fn test_sum_i64_nulls() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(5),
        ]));
        let mut accum = Vec::new();

        let output =
            ArrowAggEvaluator::<Sum<Int64Type>>::aggregate(&mut accum, 3, &entity_indices, &input)
                .unwrap();
        let output = downcast_primitive_array::<Int64Type>(output.as_ref()).unwrap();
        assert_eq!(
            output,
            &Int64Array::from(vec![Some(1), Some(2), None, Some(2), Some(7)])
        );
    }

    #[test]
    fn test_sum_i64() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]));
        let mut accum = Vec::new();

        let output =
            ArrowAggEvaluator::<Sum<Int64Type>>::aggregate(&mut accum, 3, &entity_indices, &input)
                .unwrap();

        let output = downcast_primitive_array::<Int64Type>(output.as_ref()).unwrap();
        assert_eq!(output, &Int64Array::from(vec![1, 2, 3, 6, 11]));
    }

    #[test]
    fn test_max_f64() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        // With no null values
        let input: ArrayRef = Arc::new(Float64Array::from(vec![1f64, 2.5, 3.0, 4.0, 5.0]));
        let mut accum = Vec::new();

        let output = ArrowAggEvaluator::<Max<Float64Type>>::aggregate(
            &mut accum,
            3,
            &entity_indices,
            &input,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(output, &Float64Array::from(vec![1.0, 2.5, 3.0, 4.0, 5.0]));
    }

    #[test]
    fn test_max_f64_nulls() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1f64),
            Some(2.5),
            None,
            None,
            Some(5.0),
        ]));
        let mut accum = Vec::new();

        let output = ArrowAggEvaluator::<Max<Float64Type>>::aggregate(
            &mut accum,
            3,
            &entity_indices,
            &input,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(
            output,
            &Float64Array::from(vec![Some(1.0), Some(2.5), None, Some(2.5), Some(5.0)])
        );
    }

    #[test]
    fn test_max_i64() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 3]));
        let mut accum = Vec::new();

        let output =
            ArrowAggEvaluator::<Max<Int64Type>>::aggregate(&mut accum, 3, &entity_indices, &input)
                .unwrap();
        let output = downcast_primitive_array::<Int64Type>(output.as_ref()).unwrap();
        assert_eq!(output, &Int64Array::from(vec![1, 2, 3, 4, 4]));
    }

    #[test]
    fn test_max_i64_nulls() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(5),
        ]));
        let mut accum = Vec::new();

        let output =
            ArrowAggEvaluator::<Max<Int64Type>>::aggregate(&mut accum, 3, &entity_indices, &input)
                .unwrap();

        let output = downcast_primitive_array::<Int64Type>(output.as_ref()).unwrap();
        assert_eq!(
            output,
            &Int64Array::from(vec![Some(1), Some(2), None, Some(2), Some(5)])
        );
    }

    #[test]
    fn test_mean_f64() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);

        // With no null values
        let input: ArrayRef = Arc::new(Float64Array::from(vec![1f64, 2.5, 3.0, 4.0, 5.0]));

        let mut accum = Vec::new();

        let output = ArrowAggEvaluator::<Mean<Float64Type>>::aggregate(
            &mut accum,
            3,
            &entity_indices,
            &input,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(
            output,
            &Float64Array::from(vec![
                1.0,
                2.5,
                3.0,
                (2.5 + 4.0) / 2.0,
                (2.5 + 4.0 + 5.0) / 3.0
            ])
        );
    }

    #[test]
    fn test_mean_f64_nulls() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1f64),
            Some(2.5),
            None,
            None,
            Some(5.0),
        ]));
        let mut accum = Vec::new();

        let output = ArrowAggEvaluator::<Mean<Float64Type>>::aggregate(
            &mut accum,
            3,
            &entity_indices,
            &input,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(
            output,
            &Float64Array::from(vec![
                Some(1.0),
                Some(2.5),
                None,
                Some(2.5),
                Some((2.5 + 5.0) / 2.0)
            ])
        );
    }

    #[test]
    fn test_mean_i64() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 4, 3]));
        let mut accum = Vec::new();

        let output =
            ArrowAggEvaluator::<Mean<Int64Type>>::aggregate(&mut accum, 3, &entity_indices, &input)
                .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(
            output,
            &Float64Array::from(vec![
                1f64,
                2.0,
                3.0,
                (2.0 + 4.0) / 2.0,
                (2.0 + 4.0 + 3.0) / 3.0
            ])
        );
    }

    #[test]
    fn test_mean_i64_nulls() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(1),
            Some(2),
            None,
            None,
            Some(5),
        ]));
        let mut accum = Vec::new();

        let output =
            ArrowAggEvaluator::<Mean<Int64Type>>::aggregate(&mut accum, 3, &entity_indices, &input)
                .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(
            output,
            &Float64Array::from(vec![
                Some(1f64),
                Some(2.0),
                None,
                Some(2.0),
                Some((2.0 + 5.0) / 2.0)
            ])
        );
    }

    #[test]
    fn test_last_f64() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            None,
            None,
            Some(3.0),
        ]));
        let mut accum = Vec::new();

        let output = ArrowAggEvaluator::<LastPrimitive<Float64Type>>::aggregate(
            &mut accum,
            3,
            &entity_indices,
            &input,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(
            output,
            &Float64Array::from(vec![Some(1.0), Some(2.0), None, Some(2.0), Some(3.0)])
        );

        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            None,
            Some(4.0),
            Some(5.0),
            None,
            None,
        ]));
        let output = ArrowAggEvaluator::<LastPrimitive<Float64Type>>::aggregate(
            &mut accum,
            3,
            &entity_indices,
            &input,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(
            output,
            &Float64Array::from(vec![Some(1.0), Some(4.0), Some(5.0), Some(4.0), Some(4.0)])
        );
    }

    #[test]
    fn test_first_f64() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            None,
            None,
            Some(3.0),
        ]));
        let mut accum = Vec::new();

        let output = ArrowAggEvaluator::<FirstPrimitive<Float64Type>>::aggregate(
            &mut accum,
            3,
            &entity_indices,
            &input,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();

        assert_eq!(
            output,
            &Float64Array::from(vec![Some(1.0), Some(2.0), None, Some(2.0), Some(2.0)])
        );

        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            None,
            Some(4.0),
            Some(5.0),
            None,
            None,
        ]));
        let output = ArrowAggEvaluator::<FirstPrimitive<Float64Type>>::aggregate(
            &mut accum,
            3,
            &entity_indices,
            &input,
        )
        .unwrap();

        let output = downcast_primitive_array::<Float64Type>(output.as_ref()).unwrap();
        assert_eq!(
            output,
            &Float64Array::from(vec![Some(1.0), Some(2.0), Some(5.0), Some(2.0), Some(2.0)])
        );
    }
}
