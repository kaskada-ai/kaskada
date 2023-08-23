use std::sync::Arc;

use crate::ValueRef;
use arrow::array::{Array, ArrayRef, BooleanArray, UInt32Array};
use itertools::izip;
use sparrow_arrow::downcast::downcast_boolean_array;

use super::two_stacks_first_boolean_evaluator::TwoStacksFirstBooleanEvaluator;
use crate::{
    AggregationArgs, BooleanAccumToken, Evaluator, EvaluatorFactory, RuntimeInfo, StateToken,
    StaticInfo, TwoStacksBooleanAccumToken,
};

/// Evaluator for the `First` instruction on booleans.
pub struct FirstBooleanEvaluator {
    args: AggregationArgs<ValueRef>,
    token: BooleanAccumToken,
}

impl Evaluator for FirstBooleanEvaluator {
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

impl EvaluatorFactory for FirstBooleanEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let args = AggregationArgs::from_input(info.args)?;
        match args {
            AggregationArgs::NoWindow { .. } | AggregationArgs::Since { .. } => {
                let token = BooleanAccumToken::default();
                Ok(Box::new(Self { token, args }))
            }
            AggregationArgs::Sliding { .. } => {
                let token = TwoStacksBooleanAccumToken::new();
                Ok(Box::new(TwoStacksFirstBooleanEvaluator { token, args }))
            }
        }
    }
}

impl FirstBooleanEvaluator {
    fn ensure_entity_capacity(token: &mut BooleanAccumToken, len: usize) {
        token.resize(len);
    }

    /// Updates the non-windowed accumulator based on the given flags.
    ///
    /// Implements a single row of the logic so that we can easily reuse it.
    /// We choose to inline this so that it can be specialized in cases where
    /// the valid bits are always true.
    #[inline]
    fn update_accum(
        token: &mut BooleanAccumToken,
        entity_index: u32,
        input_is_valid: bool,
        input: bool,
    ) -> anyhow::Result<Option<bool>> {
        let value_to_emit = if let Some(value) = token.get_optional_value(entity_index)? {
            Some(value)
        } else if input_is_valid {
            token.put_optional_value(entity_index, Some(input))?;
            Some(input)
        } else {
            None
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
        token: &mut BooleanAccumToken,
        entity_index: u32,
        input_is_valid: bool,
        since_is_valid: bool,
        input: bool,
        since_bool: bool,
    ) -> anyhow::Result<Option<bool>> {
        let value_to_emit = if let Some(value) = token.get_optional_value(entity_index)? {
            Some(value)
        } else if input_is_valid {
            token.put_optional_value(entity_index, Some(input))?;
            Some(input)
        } else {
            None
        };

        if since_is_valid && since_bool {
            token.put_optional_value(entity_index, None)?;
        }

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
        token: &mut BooleanAccumToken,
        key_capacity: usize,
        key_indices: &UInt32Array,
        input: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        assert_eq!(key_indices.len(), input.len());
        let input: &BooleanArray = downcast_boolean_array(input.as_ref())?;

        // Make sure the internal buffers are large enough for the accumulators we may
        // want to store.
        Self::ensure_entity_capacity(token, key_capacity);

        let result: BooleanArray = if let Some(input_valid_bits) = input.nulls() {
            izip!(key_indices.values(), input_valid_bits, 0..)
                .map(|(entity_index, input_is_valid, input_index)| {
                    Self::update_accum(
                        token,
                        *entity_index,
                        input_is_valid,
                        input.value(input_index),
                    )
                })
                .collect::<anyhow::Result<BooleanArray>>()?
        } else {
            izip!(key_indices.values(), 0..)
                .map(|(entity_index, input_index)| {
                    Self::update_accum(token, *entity_index, true, input.value(input_index))
                })
                .collect::<anyhow::Result<BooleanArray>>()?
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
        token: &mut BooleanAccumToken,
        key_capacity: usize,
        key_indices: &UInt32Array,
        input: &ArrayRef,
        window_since: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        assert_eq!(key_indices.len(), input.len());
        let input: &BooleanArray = downcast_boolean_array(input.as_ref())?;

        // Make sure the internal buffers are large enough for the accumulators we may
        // want to store.
        Self::ensure_entity_capacity(token, key_capacity);

        let result: BooleanArray = match (input.nulls(), window_since.nulls()) {
            (None, None) => izip!(key_indices.values(), 0.., window_since.values().iter(),)
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
                .collect::<anyhow::Result<BooleanArray>>()?,

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
            .collect::<anyhow::Result<BooleanArray>>()?,

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
            .collect::<anyhow::Result<BooleanArray>>()?,

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
            .collect::<anyhow::Result<BooleanArray>>()?,
        };

        Ok(Arc::new(result))
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_boolean_first_with_no_null() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1, 0]);
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            Some(true),
        ]));
        let mut token = BooleanAccumToken::default();

        let output =
            FirstBooleanEvaluator::aggregate(&mut token, 3, &entity_indices, &input).unwrap();
        assert_eq!(
            downcast_boolean_array(output.as_ref()).unwrap(),
            &BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(false),
                Some(true),
                Some(true),
                Some(false)
            ])
        );
    }

    #[test]
    fn test_boolean_first_with_null() {
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1, 0]);
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            None,
            Some(false),
            None,
        ]));
        let mut token = BooleanAccumToken::default();

        let output =
            FirstBooleanEvaluator::aggregate(&mut token, 3, &entity_indices, &input).unwrap();
        assert_eq!(
            downcast_boolean_array(output.as_ref()).unwrap(),
            &BooleanArray::from(vec![
                Some(false),
                Some(true),
                None,
                Some(true),
                Some(true),
                Some(false)
            ])
        );

        // And another round (to make sure values carry over)
        let entity_indices = UInt32Array::from(vec![0, 1, 1, 2, 3, 0]);
        let input: ArrayRef = Arc::new(BooleanArray::from(vec![
            None,
            None,
            Some(true),
            Some(true),
            None,
            Some(true),
        ]));

        let output =
            FirstBooleanEvaluator::aggregate(&mut token, 4, &entity_indices, &input).unwrap();
        assert_eq!(
            downcast_boolean_array(output.as_ref()).unwrap(),
            &BooleanArray::from(vec![
                Some(false),
                Some(true),
                Some(true),
                Some(true),
                None,
                Some(false)
            ])
        );
    }
}
