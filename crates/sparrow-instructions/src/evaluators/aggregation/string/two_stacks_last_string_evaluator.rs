use std::sync::Arc;

use crate::ValueRef;
use anyhow::anyhow;
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray, UInt32Array};
use arrow::datatypes::Int64Type;
use itertools::izip;
use sparrow_arrow::downcast::downcast_string_array;

use crate::{
    AggregationArgs, Evaluator, LastString, RuntimeInfo, StateToken, TwoStacks,
    TwoStacksStringAccumToken,
};

/// Evaluator for the `last` instruction on strings.
pub(crate) struct TwoStacksLastStringEvaluator {
    pub args: AggregationArgs<ValueRef>,
    pub token: TwoStacksStringAccumToken<LastString>,
}

impl Evaluator for TwoStacksLastStringEvaluator {
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

impl TwoStacksLastStringEvaluator {
    fn ensure_entity_capacity(
        token: &mut TwoStacksStringAccumToken<LastString>,
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
        token: &mut TwoStacksStringAccumToken<LastString>,
        entity_index: u32,
        input_is_valid: bool,
        sliding_is_valid: bool,
        input: &str,
        sliding: bool,
        initial_windows: i64,
    ) -> anyhow::Result<Option<String>> {
        let evict_window = sliding_is_valid && sliding;

        let mut accum = match token.get_value(entity_index)? {
            Some(accum) => accum,
            None => TwoStacks::new(initial_windows),
        };

        if input_is_valid {
            accum.add_input(&input.to_string());
        };

        let value_to_emit = accum.accum_value();

        if evict_window {
            accum.evict();
        }

        token.put_value(entity_index, accum)?;
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
        token: &mut TwoStacksStringAccumToken<LastString>,
        key_capacity: usize,
        key_indices: &UInt32Array,
        input: &ArrayRef,
        sliding_duration: i64,
        sliding_window: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        assert_eq!(key_indices.len(), input.len());
        let input: &StringArray = downcast_string_array(input.as_ref())?;

        Self::ensure_entity_capacity(token, key_capacity, sliding_duration);

        let result: StringArray = match (input.nulls(), sliding_window.nulls()) {
            (None, None) => izip!(key_indices.values(), 0.., sliding_window.values().iter())
                .map(|(entity_index, input_index, since_bool)| {
                    Self::update_two_stacks_accum(
                        token,
                        *entity_index,
                        true,
                        true,
                        input.value(input_index),
                        since_bool,
                        sliding_duration,
                    )
                })
                .collect::<anyhow::Result<StringArray>>()?,

            (Some(input_valid_bits), None) => izip!(
                key_indices.values(),
                input_valid_bits,
                0..,
                sliding_window.values().iter()
            )
            .map(|(entity_index, input_is_valid, input_index, since_bool)| {
                Self::update_two_stacks_accum(
                    token,
                    *entity_index,
                    input_is_valid,
                    true,
                    input.value(input_index),
                    since_bool,
                    sliding_duration,
                )
            })
            .collect::<anyhow::Result<StringArray>>()?,

            (None, Some(window_valid_bits)) => izip!(
                key_indices.values(),
                window_valid_bits,
                0..,
                sliding_window.values().iter()
            )
            .map(|(entity_index, since_is_valid, input_index, since_bool)| {
                Self::update_two_stacks_accum(
                    token,
                    *entity_index,
                    true,
                    since_is_valid,
                    input.value(input_index),
                    since_bool,
                    sliding_duration,
                )
            })
            .collect::<anyhow::Result<StringArray>>()?,

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
                        token,
                        *entity_index,
                        input_is_valid,
                        since_is_valid,
                        input.value(input_index),
                        since_bool,
                        sliding_duration,
                    )
                },
            )
            .collect::<anyhow::Result<StringArray>>()?,
        };

        Ok(Arc::new(result))
    }
}
#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn test_sliding_string_last_with_no_null() {
        let entity_indices = UInt32Array::from(vec![0, 0, 0, 0, 0]);
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some("phone"),
            Some("hello"),
            Some("world"),
            Some("monday"),
            Some("dog"),
        ]));
        let sliding = BooleanArray::from(vec![
            Some(true),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
        ]);

        let mut token = TwoStacksStringAccumToken::new();
        let output = TwoStacksLastStringEvaluator::aggregate(
            &mut token,
            1,
            &entity_indices,
            &input,
            2,
            &sliding,
        )
        .unwrap();

        assert_eq!(
            downcast_string_array(output.as_ref()).unwrap(),
            &StringArray::from(vec![
                Some("phone"),
                Some("hello"),
                Some("world"),
                Some("monday"),
                Some("dog")
            ])
        );
    }
}
