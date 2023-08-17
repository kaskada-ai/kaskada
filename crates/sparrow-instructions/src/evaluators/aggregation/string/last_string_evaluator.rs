use std::sync::Arc;

use crate::ValueRef;
use arrow::array::{Array, ArrayRef, BooleanArray, StringArray, UInt32Array};
use itertools::izip;
use sparrow_arrow::downcast::downcast_string_array;

use super::two_stacks_last_string_evaluator::TwoStacksLastStringEvaluator;
use crate::{
    AggregationArgs, Evaluator, EvaluatorFactory, RuntimeInfo, StateToken, StaticInfo,
    StringAccumToken, TwoStacksStringAccumToken,
};

/// Evaluator for the `last` instruction on strings.
pub struct LastStringEvaluator {
    args: AggregationArgs<ValueRef>,
    token: StringAccumToken,
}

impl Evaluator for LastStringEvaluator {
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

impl EvaluatorFactory for LastStringEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let args = AggregationArgs::from_input(info.args)?;
        match args {
            AggregationArgs::NoWindow { .. } | AggregationArgs::Since { .. } => {
                let token = StringAccumToken::default();
                Ok(Box::new(Self { token, args }))
            }
            AggregationArgs::Sliding { .. } => {
                let token = TwoStacksStringAccumToken::new();
                Ok(Box::new(TwoStacksLastStringEvaluator { token, args }))
            }
        }
    }
}

impl LastStringEvaluator {
    fn ensure_entity_capacity(token: &mut StringAccumToken, entity_id_len: usize) {
        token.resize(entity_id_len);
    }

    /// Updates the non-windowed accumulator based on the given flags.
    ///
    /// Implements a single row of the logic so that we can easily reuse it.
    /// We choose to inline this so that it can be specialized in cases where
    /// the valid bits are always true.
    #[inline]
    fn update_accum(
        token: &mut StringAccumToken,
        entity_index: u32,
        input_is_valid: bool,
        input: &str,
    ) -> anyhow::Result<Option<String>> {
        let value_to_emit = if input_is_valid {
            token.put_value(entity_index, Some(input.to_string()))?;
            Some(input.to_string())
        } else {
            token.get_value(entity_index)?
        };

        Ok(value_to_emit)
    }

    /// Updates the since-windowed accumulator based on the given flags.
    ///
    /// Accumulator behavior is to update -> emit -> reset, resulting in
    /// exclusive start bounds and inclusive end bounds.
    ///
    /// Implements a single row of the logic so that we can easily reuse it.
    /// We choose to inline this so that it can be specialized in cases where
    /// the valid bits are always true.
    #[inline]
    fn update_since_accum(
        token: &mut StringAccumToken,
        entity_index: u32,
        input_is_valid: bool,
        since_is_valid: bool,
        input: &str,
        since_bool: bool,
    ) -> anyhow::Result<Option<String>> {
        let reset_window = since_is_valid && since_bool;
        let value_to_emit = if input_is_valid && reset_window {
            // Here we know we're going to reset the window, so don't bother attempting to
            // put a new value, since it'll be set to `None` afterwards.
            Some(input.to_string())
        } else if input_is_valid {
            // Input is valid and no reset, so put the new value.
            token.put_value(entity_index, Some(input.to_string()))?;
            Some(input.to_string())
        } else {
            // Input is not valid, so grab the current latest value.
            token.get_value(entity_index)?
        };

        if reset_window {
            // Reset the value for this entity
            token.put_value(entity_index, None)?;
        };

        Ok(value_to_emit)
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
        token: &mut StringAccumToken,
        key_capacity: usize,
        key_indices: &UInt32Array,
        input: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        assert_eq!(key_indices.len(), input.len());
        let input: &StringArray = downcast_string_array(input.as_ref())?;

        // Make sure the internal buffers are large enough for the accumulators we may
        // want to store.
        Self::ensure_entity_capacity(token, key_capacity);

        let result: StringArray = if let Some(input_valid_bits) = input.nulls() {
            izip!(key_indices.values(), input_valid_bits, 0..)
                .map(|(entity_index, input_is_valid, input_index)| {
                    Self::update_accum(
                        token,
                        *entity_index,
                        input_is_valid,
                        input.value(input_index),
                    )
                })
                .collect::<anyhow::Result<StringArray>>()?
        } else {
            izip!(key_indices.values(), 0..)
                .map(|(entity_index, input_index)| {
                    Self::update_accum(token, *entity_index, true, input.value(input_index))
                })
                .collect::<anyhow::Result<StringArray>>()?
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
    /// reset.
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
        token: &mut StringAccumToken,
        key_capacity: usize,
        key_indices: &UInt32Array,
        input: &ArrayRef,
        window_since: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        assert_eq!(key_indices.len(), input.len());
        let input: &StringArray = downcast_string_array(input.as_ref())?;

        // Make sure the internal buffers are large enough for the accumulators we may
        // want to store.
        Self::ensure_entity_capacity(token, key_capacity);

        let result: StringArray = match (input.nulls(), window_since.nulls()) {
            (None, None) => izip!(key_indices.values(), 0.., window_since.values().iter())
                .map(|(entity_index, input_index, since_bool)| {
                    Self::update_since_accum(
                        token,
                        *entity_index,
                        true,
                        true,
                        input.value(input_index),
                        since_bool,
                    )
                })
                .collect::<anyhow::Result<StringArray>>()?,

            (Some(input_valid_bits), None) => izip!(
                key_indices.values(),
                input_valid_bits,
                0..,
                window_since.values().iter()
            )
            .map(|(entity_index, input_is_valid, input_index, since_bool)| {
                Self::update_since_accum(
                    token,
                    *entity_index,
                    input_is_valid,
                    true,
                    input.value(input_index),
                    since_bool,
                )
            })
            .collect::<anyhow::Result<StringArray>>()?,

            (None, Some(window_valid_bits)) => izip!(
                key_indices.values(),
                window_valid_bits,
                0..,
                window_since.values().iter()
            )
            .map(|(entity_index, since_is_valid, input_index, since_bool)| {
                Self::update_since_accum(
                    token,
                    *entity_index,
                    true,
                    since_is_valid,
                    input.value(input_index),
                    since_bool,
                )
            })
            .collect::<anyhow::Result<StringArray>>()?,

            (Some(input_valid_bits), Some(window_valid_bits)) => izip!(
                key_indices.values(),
                input_valid_bits,
                window_valid_bits,
                0..,
                window_since.values().iter()
            )
            .map(
                |(entity_index, input_is_valid, since_is_valid, input_index, since_bool)| {
                    Self::update_since_accum(
                        token,
                        *entity_index,
                        input_is_valid,
                        since_is_valid,
                        input.value(input_index),
                        since_bool,
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

    use arrow::array::{ArrayRef, BooleanArray};

    use super::*;

    /// Size of chunks used during aggregation. 64 aligns with the size of
    /// chunks in arrow's null-bit vector.
    const CHUNK_SIZE: usize = 64;

    #[test]
    fn test_string_last_with_multiple_chunks() {
        let entity_indices = UInt32Array::from(vec![0; CHUNK_SIZE + 1]);
        let input: Vec<String> = (0..CHUNK_SIZE + 1).map(|i| i.to_string()).collect();
        let input: Vec<&str> = input.iter().map(|i| i.as_str()).collect();
        let input: ArrayRef = Arc::new(StringArray::from(input));

        let mut token = StringAccumToken::default();
        let output =
            LastStringEvaluator::aggregate(&mut token, 1, &entity_indices, &input).unwrap();

        assert_eq!(
            downcast_string_array::<i32>(output.as_ref()).unwrap(),
            downcast_string_array::<i32>(input.as_ref()).unwrap()
        );
    }

    #[test]
    fn test_string_last_with_no_null() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some("phone"),
            Some("hello"),
            Some("world"),
            Some("monday"),
            Some("dog"),
        ]));
        let mut token = StringAccumToken::default();
        let output =
            LastStringEvaluator::aggregate(&mut token, 3, &entity_indices, &input).unwrap();

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

    #[test]
    fn test_string_last_with_null() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some("phone"),
            Some("hello"),
            None,
            None,
            Some("dog"),
        ]));
        let mut token = StringAccumToken::default();
        let output =
            LastStringEvaluator::aggregate(&mut token, 3, &entity_indices, &input).unwrap();

        assert_eq!(
            downcast_string_array(output.as_ref()).unwrap(),
            &StringArray::from(vec![
                Some("phone"),
                Some("hello"),
                None,
                Some("hello"),
                Some("dog")
            ])
        );

        // And another round (to make sure values carry over)
        let entity_indices = UInt32Array::from(vec![0, 1, 1, 2, 3]);
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            None,
            Some("second"),
            None,
            None,
        ]));
        let output =
            LastStringEvaluator::aggregate(&mut token, 4, &entity_indices, &input).unwrap();

        assert_eq!(
            downcast_string_array(output.as_ref()).unwrap(),
            &StringArray::from(vec![Some("phone"), Some("dog"), Some("second"), None, None,])
        );
    }

    #[test]
    fn test_string_last_since_with_null() {
        let entity_indices = UInt32Array::from(vec![0, 0, 0, 0, 0]);
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            Some("phone"),
            Some("hello"),
            None,
            None,
            Some("dog"),
        ]));

        let since = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
        ]);

        let mut token = StringAccumToken::default();
        let output =
            LastStringEvaluator::aggregate_since(&mut token, 3, &entity_indices, &input, &since)
                .unwrap();

        assert_eq!(
            downcast_string_array(output.as_ref()).unwrap(),
            &StringArray::from(vec![
                Some("phone"),
                Some("hello"),
                Some("hello"),
                Some("hello"),
                Some("dog")
            ])
        );

        // And another round (to make sure values carry over)
        let entity_indices = UInt32Array::from(vec![0, 0, 0, 0, 0]);
        let input: ArrayRef = Arc::new(StringArray::from(vec![
            None,
            None,
            Some("second"),
            None,
            None,
        ]));
        let since = BooleanArray::from(vec![
            Some(false),
            Some(false),
            Some(false),
            Some(true),
            Some(false),
        ]);
        let output =
            LastStringEvaluator::aggregate_since(&mut token, 4, &entity_indices, &input, &since)
                .unwrap();

        assert_eq!(
            downcast_string_array(output.as_ref()).unwrap(),
            &StringArray::from(vec![
                Some("dog"),
                Some("dog"),
                Some("second"),
                Some("second"),
                None,
            ])
        );
    }
}
