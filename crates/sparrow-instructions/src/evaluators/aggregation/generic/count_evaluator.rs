use std::sync::Arc;

use crate::ValueRef;
use arrow::array::{Array, ArrayRef, BooleanArray, PrimitiveArray, UInt32Array};
use arrow::datatypes::UInt32Type;
use itertools::izip;
use sparrow_arrow::downcast::downcast_boolean_array;

use super::two_stacks_count_evaluator::TwoStacksCountIfEvaluator;
use crate::{
    AggregationArgs, CountAccumToken, Evaluator, EvaluatorFactory, RuntimeInfo, StateToken,
    StaticInfo, TwoStacksCountAccumToken,
};

/// Evaluator for the `count_if` instruction.
pub struct CountIfEvaluator {
    args: AggregationArgs<ValueRef>,
    token: CountAccumToken,
}

impl Evaluator for CountIfEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        match &self.args {
            AggregationArgs::NoWindow { input } => {
                let grouping = info.grouping();
                let input_vals = info.value(input)?.array_ref()?;
                let result = Self::aggregate(
                    &mut self.token,
                    grouping.num_groups(),
                    grouping.group_indices(),
                    &input_vals,
                );
                result
            }
            AggregationArgs::Since { ticks, input } => {
                let grouping = info.grouping();
                let input_vals = info.value(input)?.array_ref()?;
                let ticks = info.value(ticks)?.boolean_array()?;
                let result = Self::aggregate_since(
                    &mut self.token,
                    grouping.num_groups(),
                    grouping.group_indices(),
                    &input_vals,
                    ticks.as_ref(),
                );
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

impl EvaluatorFactory for CountIfEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let args = AggregationArgs::from_input(info.args)?;
        match args {
            AggregationArgs::NoWindow { .. } | AggregationArgs::Since { .. } => {
                let token = CountAccumToken::default();
                Ok(Box::new(Self { token, args }))
            }
            AggregationArgs::Sliding { .. } => {
                let token = TwoStacksCountAccumToken::new();
                Ok(Box::new(TwoStacksCountIfEvaluator { token, args }))
            }
        }
    }
}

impl CountIfEvaluator {
    fn ensure_entity_capacity(token: &mut CountAccumToken, len: usize) {
        token.resize(len);
    }

    #[inline]
    fn update_accum(
        token: &mut CountAccumToken,
        entity_index: u32,
        input_is_valid: bool,
        input: bool,
    ) -> u32 {
        if input_is_valid && input {
            token.increment_value(entity_index);
        }
        token.get_value(entity_index)
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
    fn update_since_accum(
        token: &mut CountAccumToken,
        entity_index: u32,
        input_is_valid: bool,
        input: bool,
        since_is_valid: bool,
        since: bool,
    ) -> u32 {
        if input_is_valid && input {
            token.increment_value(entity_index);
        }
        let value_to_emit = token.get_value(entity_index);

        if since_is_valid && since {
            // If `since` is true, clear the accumulator
            token.reset_value(entity_index);
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
        token: &mut CountAccumToken,
        key_capacity: usize,
        key_indices: &UInt32Array,
        input: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        // Make sure the internal buffers are large enough for the accumulators we may
        // want to store.
        Self::ensure_entity_capacity(token, key_capacity);

        let input = downcast_boolean_array(input)?;
        let result: UInt32Array = if let Some(input_valid_bits) = input.nulls() {
            let iter = izip!(key_indices.values(), input_valid_bits, 0..).map(
                |(key_index, input_is_valid, input_index)| {
                    Some(Self::update_accum(
                        token,
                        *key_index,
                        input_is_valid,
                        input.value(input_index),
                    ))
                },
            );

            // SAFETY: The iterator produced by `izip` has trusted length.
            unsafe { PrimitiveArray::<UInt32Type>::from_trusted_len_iter(iter) }
        } else {
            let iter = izip!(key_indices.values(), 0..).map(|(entity_index, input_index)| {
                Some(Self::update_accum(
                    token,
                    *entity_index,
                    true,
                    input.value(input_index),
                ))
            });

            // SAFETY: The iterator produced by `izip` has trusted length.
            unsafe { PrimitiveArray::<UInt32Type>::from_trusted_len_iter(iter) }
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
        token: &mut CountAccumToken,
        key_capacity: usize,
        entity_indices: &UInt32Array,
        input: &ArrayRef,
        ticks: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        // Make sure the internal buffers are large enough for the accumulators we may
        // want to store.
        Self::ensure_entity_capacity(token, key_capacity);

        let input = downcast_boolean_array(input.as_ref())?;

        let result: PrimitiveArray<UInt32Type> = match (input.nulls(), ticks.nulls()) {
            (None, None) => {
                let iter = izip!(entity_indices.values(), 0.., ticks.values().iter()).map(
                    |(entity_index, input_index, tick)| {
                        Some(Self::update_since_accum(
                            token,
                            *entity_index,
                            true,
                            input.value(input_index),
                            true,
                            tick,
                        ))
                    },
                );

                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }

            (Some(input_valid_bits), None) => {
                let iter = izip!(
                    entity_indices.values(),
                    input_valid_bits,
                    0..,
                    ticks.values().iter()
                )
                .map(|(entity_index, input_valid, input_index, since_bool)| {
                    Some(Self::update_since_accum(
                        token,
                        *entity_index,
                        input_valid,
                        input.value(input_index),
                        true,
                        since_bool,
                    ))
                });

                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }
            (None, Some(window_valid_bits)) => {
                let iter = izip!(
                    entity_indices.values(),
                    window_valid_bits,
                    0..,
                    ticks.values().iter()
                )
                .map(|(entity_index, window_valid, input_index, since_bool)| {
                    Some(Self::update_since_accum(
                        token,
                        *entity_index,
                        true,
                        input.value(input_index),
                        window_valid,
                        since_bool,
                    ))
                });

                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }
            (Some(input_valid_bits), Some(window_valid_bits)) => {
                let iter = izip!(
                    entity_indices.values(),
                    input_valid_bits,
                    window_valid_bits,
                    0..,
                    ticks.values().iter()
                )
                .map(
                    |(entity_index, input_valid, window_valid, input_index, since_bool)| {
                        Some(Self::update_since_accum(
                            token,
                            *entity_index,
                            input_valid,
                            input.value(input_index),
                            window_valid,
                            since_bool,
                        ))
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
    use arrow::array::UInt32Array;
    use arrow::datatypes::UInt32Type;
    use sparrow_arrow::downcast::downcast_primitive_array;

    use super::*;

    #[test]
    fn test_count_no_nulls() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(true),
        ]));
        let mut token = CountAccumToken::default();
        let output = CountIfEvaluator::aggregate(&mut token, 3, &entity_indices, &input).unwrap();
        let output = downcast_primitive_array::<UInt32Type>(output.as_ref()).unwrap();
        assert_eq!(output, &UInt32Array::from(vec![1, 0, 0, 1, 2]));

        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(false),
            Some(false),
        ]));
        let output = CountIfEvaluator::aggregate(&mut token, 3, &entity_indices, &input).unwrap();
        let output = downcast_primitive_array::<UInt32Type>(output.as_ref()).unwrap();
        assert_eq!(output, &UInt32Array::from(vec![2, 3, 1, 3, 3]));
    }

    #[test]
    fn test_count_with_nulls() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            None,
            None,
            Some(false),
        ]));
        let mut token = CountAccumToken::default();
        let output = CountIfEvaluator::aggregate(&mut token, 3, &entity_indices, &input).unwrap();
        let output = downcast_primitive_array::<UInt32Type>(output.as_ref()).unwrap();
        assert_eq!(output, &UInt32Array::from(vec![1, 1, 0, 1, 1]));

        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![
            None,
            Some(true),
            None,
            None,
            Some(false),
        ]));
        let output = CountIfEvaluator::aggregate(&mut token, 3, &entity_indices, &input).unwrap();
        let output = downcast_primitive_array::<UInt32Type>(output.as_ref()).unwrap();
        assert_eq!(output, &UInt32Array::from(vec![1, 2, 0, 2, 2]));
    }
}
