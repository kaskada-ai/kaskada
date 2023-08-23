use std::sync::Arc;

use crate::ValueRef;
use anyhow::anyhow;
use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow::datatypes::Int64Type;
use itertools::izip;
use sparrow_arrow::downcast::downcast_boolean_array;

use crate::{
    AggregationArgs, Evaluator, LastBoolean, RuntimeInfo, StateToken, TwoStacks,
    TwoStacksBooleanAccumToken,
};

/// Evaluator for the `last` instruction on booleans.
pub(crate) struct TwoStacksLastBooleanEvaluator {
    pub args: AggregationArgs<ValueRef>,
    pub token: TwoStacksBooleanAccumToken<LastBoolean>,
}

impl Evaluator for TwoStacksLastBooleanEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        match &self.args {
            AggregationArgs::Sliding {
                input,
                ticks,
                duration,
            } => {
                // Get the stored state of the accum
                let mut accum = self.token.get_boolean_accum()?;

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
                self.token.put_boolean_accum(accum)?;

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

impl TwoStacksLastBooleanEvaluator {
    fn ensure_entity_capacity(
        accum: &mut Vec<TwoStacks<LastBoolean>>,
        entity_capacity: usize,
        window_parts: i64,
    ) {
        if entity_capacity > accum.len() {
            accum.resize(entity_capacity, TwoStacks::new(window_parts));
        }
    }

    /// Updates the windowed accumulator based on the given flags.
    ///
    /// Accumulator behavior is to update -> emit -> reset, resulting in
    /// exclusive start bounds and inclusive end bounds.
    #[inline]
    fn update_two_stacks_accum(
        accum: &mut [TwoStacks<LastBoolean>],
        entity_index: u32,
        input_is_valid: bool,
        sliding_is_valid: bool,
        input: bool,
        sliding: bool,
    ) -> anyhow::Result<Option<bool>> {
        if input_is_valid {
            accum[entity_index as usize].add_input(&input);
        }

        let value_to_emit = accum[entity_index as usize].accum_value();

        if sliding_is_valid && sliding {
            accum[entity_index as usize].evict();
        }

        Ok(value_to_emit)
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
        accum: &mut Vec<TwoStacks<LastBoolean>>,
        key_capacity: usize,
        key_indices: &UInt32Array,
        input: &ArrayRef,
        sliding_duration: i64,
        sliding_window: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        assert_eq!(key_indices.len(), input.len());
        let input: &BooleanArray = downcast_boolean_array(input.as_ref())?;

        Self::ensure_entity_capacity(accum, key_capacity, sliding_duration);

        let result: BooleanArray = match (input.nulls(), sliding_window.nulls()) {
            (None, None) => izip!(key_indices.values(), 0.., sliding_window.values().iter())
                .map(|(entity_index, input_index, since_bool)| {
                    Self::update_two_stacks_accum(
                        accum,
                        *entity_index,
                        true,
                        true,
                        input.value(input_index),
                        since_bool,
                    )
                })
                .collect::<anyhow::Result<BooleanArray>>()?,

            (Some(input_valid_bits), None) => izip!(
                key_indices.values(),
                input_valid_bits,
                0..,
                sliding_window.values().iter()
            )
            .map(|(entity_index, input_is_valid, input_index, since_bool)| {
                Self::update_two_stacks_accum(
                    accum,
                    *entity_index,
                    input_is_valid,
                    true,
                    input.value(input_index),
                    since_bool,
                )
            })
            .collect::<anyhow::Result<BooleanArray>>()?,

            (None, Some(window_valid_bits)) => izip!(
                key_indices.values(),
                window_valid_bits,
                0..,
                sliding_window.values().iter()
            )
            .map(|(entity_index, since_is_valid, input_index, since_bool)| {
                Self::update_two_stacks_accum(
                    accum,
                    *entity_index,
                    true,
                    since_is_valid,
                    input.value(input_index),
                    since_bool,
                )
            })
            .collect::<anyhow::Result<BooleanArray>>()?,

            (Some(input_valid_bits), Some(window_valid_bits)) => izip!(
                key_indices.values(),
                input_valid_bits,
                window_valid_bits,
                0..,
                sliding_window.values().iter()
            )
            .map(
                |(entity_index, input_is_valid, since_is_valid, input_index, since_bool)| {
                    Self::update_two_stacks_accum(
                        accum,
                        *entity_index,
                        input_is_valid,
                        since_is_valid,
                        input.value(input_index),
                        since_bool,
                    )
                },
            )
            .collect::<anyhow::Result<BooleanArray>>()?,
        };

        Ok(Arc::new(result))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_sliding_boolean_last_with_no_null() {
        let entity_indices = UInt32Array::from(vec![0, 0, 0, 0, 0]);
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
        ]));
        let sliding = BooleanArray::from(vec![
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
        ]);
        let mut token = TwoStacksBooleanAccumToken::new();
        let mut accum = token.get_boolean_accum().unwrap();

        let output = TwoStacksLastBooleanEvaluator::aggregate(
            &mut accum,
            1,
            &entity_indices,
            &input,
            2,
            &sliding,
        )
        .unwrap();
        assert_eq!(
            downcast_boolean_array(output.as_ref()).unwrap(),
            &BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                Some(false)
            ])
        );
    }
}
