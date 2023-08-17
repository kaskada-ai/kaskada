use std::sync::Arc;

use crate::ValueRef;
use anyhow::anyhow;
use arrow::array::{Array, ArrayRef, BooleanArray, PrimitiveArray, UInt32Array};
use arrow::datatypes::{Int64Type, UInt32Type};
use itertools::izip;
use sparrow_arrow::downcast::downcast_boolean_array;

use crate::{AggregationArgs, Count, Evaluator, RuntimeInfo, StateToken, TwoStacksCountAccumToken};

/// Evaluator for the `count` instruction.
pub(crate) struct TwoStacksCountIfEvaluator {
    pub args: AggregationArgs<ValueRef>,
    pub token: TwoStacksCountAccumToken<Count>,
}

impl Evaluator for TwoStacksCountIfEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        match &self.args {
            AggregationArgs::Sliding {
                input,
                ticks,
                duration,
            } => {
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
                    &mut self.token,
                    grouping.num_groups(),
                    grouping.group_indices(),
                    &input_vals,
                    duration,
                    ticks.as_ref(),
                );

                result
            }
            AggregationArgs::Since { .. } | AggregationArgs::NoWindow { .. } => {
                unreachable!(
                    "Expected sliding-windowed aggregation, saw non-windowed or since windowed."
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

impl TwoStacksCountIfEvaluator {
    fn ensure_entity_capacity(
        token: &mut TwoStacksCountAccumToken<Count>,
        entity_capacity: usize,
        window_parts: i64,
    ) {
        token.resize(entity_capacity, window_parts);
    }

    /// Updates the windowed accumulator based on the given flags.
    ///
    /// Accumulator behavior is to update -> emit -> reset, resulting in
    /// exclusive start bounds and inclusive end bounds.
    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn update_two_stacks_accum(
        token: &mut TwoStacksCountAccumToken<Count>,
        entity_index: u32,
        input_is_valid: bool,
        input: bool,
        sliding_is_valid: bool,
        sliding: bool,
    ) -> u32 {
        let evict_window = sliding_is_valid && sliding;

        if input_is_valid && input {
            token.add_value(entity_index, 1);
        }

        let value_to_emit = token.get_value(entity_index);

        if evict_window {
            token.evict_window(entity_index);
        }

        value_to_emit
    }

    /// Update the aggregation state with the given inputs and return the
    /// aggregation.
    ///
    /// The `key_capacity` must be greater than all values in the
    /// `entity_indices`.
    ///
    /// # Window Behavior
    /// This aggregation uses the `sliding` window behavior.
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
        token: &mut TwoStacksCountAccumToken<Count>,
        key_capacity: usize,
        key_indices: &UInt32Array,
        input: &ArrayRef,
        sliding_duration: i64,
        sliding_window: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        assert_eq!(key_indices.len(), input.len());

        Self::ensure_entity_capacity(token, key_capacity, sliding_duration);

        let input = downcast_boolean_array(input)?;

        let result: PrimitiveArray<UInt32Type> = match (input.nulls(), sliding_window.nulls()) {
            (None, None) => {
                let iter = izip!(key_indices.values(), 0.., sliding_window.values().iter()).map(
                    |(entity_index, input_index, since_bool)| {
                        Some(Self::update_two_stacks_accum(
                            token,
                            *entity_index,
                            true,
                            input.value(input_index),
                            true,
                            since_bool,
                        ))
                    },
                );

                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }

            (Some(input_valid_bits), None) => {
                let iter = izip!(
                    key_indices.values(),
                    input_valid_bits,
                    0..,
                    sliding_window.values().iter()
                )
                .map(|(entity_index, input_is_valid, input_index, since_bool)| {
                    Some(Self::update_two_stacks_accum(
                        token,
                        *entity_index,
                        input_is_valid,
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
                    key_indices.values(),
                    0..,
                    window_valid_bits,
                    sliding_window.values().iter()
                )
                .map(|(entity_index, input_index, since_is_valid, since_bool)| {
                    Some(Self::update_two_stacks_accum(
                        token,
                        *entity_index,
                        true,
                        input.value(input_index),
                        since_is_valid,
                        since_bool,
                    ))
                });

                // SAFETY: `izip!` and `map` are trusted length iterators.
                unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
            }

            (Some(input_valid_bits), Some(window_valid_bits)) => {
                let iter = izip!(
                    key_indices.values(),
                    input_valid_bits,
                    0..,
                    window_valid_bits,
                    sliding_window.values().iter()
                )
                .map(
                    |(entity_index, input_is_valid, input_index, since_is_valid, since_bool)| {
                        Some(Self::update_two_stacks_accum(
                            token,
                            *entity_index,
                            input_is_valid,
                            input.value(input_index),
                            since_is_valid,
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
    use sparrow_arrow::downcast::downcast_primitive_array;

    use super::*;

    #[test]
    fn test_sliding_count_no_nulls() {
        let entity_indices = UInt32Array::from(vec![0, 0, 0, 0, 0]);
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
        ]));
        let sliding = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
        ]);
        let mut token = TwoStacksCountAccumToken::new();

        let output = TwoStacksCountIfEvaluator::aggregate(
            &mut token,
            1,
            &entity_indices,
            &input,
            2,
            &sliding,
        )
        .unwrap();
        let output = downcast_primitive_array::<UInt32Type>(output.as_ref()).unwrap();
        assert_eq!(output, &UInt32Array::from(vec![1, 2, 3, 4, 4]));

        let entity_indices = UInt32Array::from(vec![0, 0, 0, 0, 0]);
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(true),
            Some(true),
            Some(true),
        ]));
        let sliding = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(true),
            Some(true),
            Some(true),
        ]);

        let output = TwoStacksCountIfEvaluator::aggregate(
            &mut token,
            1,
            &entity_indices,
            &input,
            2,
            &sliding,
        )
        .unwrap();
        let output = downcast_primitive_array::<UInt32Type>(output.as_ref()).unwrap();
        assert_eq!(output, &UInt32Array::from(vec![5, 6, 7, 5, 2]));
    }
}
