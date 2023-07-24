use std::sync::Arc;

use arrow::{
    array::{
        as_map_array, new_empty_array, Array, ArrayRef, BooleanArray, Int32Array, MapBuilder,
        PrimitiveArray, StringArray, UInt32Array,
    },
    datatypes::{ArrowPrimitiveType, DataType},
};
use itertools::izip;
use sparrow_arrow::downcast::downcast_string_array;
use sparrow_plan::ValueRef;

use crate::{
    AggregationArgs, Evaluator, EvaluatorFactory, MapAccumToken, RuntimeInfo, StateToken,
    StaticInfo, StringAccumToken, TwoStacksStringAccumToken,
};

/// Evaluator for the `First` instruction on maps
pub struct FirstMapEvaluator {
    args: AggregationArgs<ValueRef>,
    token: MapAccumToken,
}

impl Evaluator for FirstMapEvaluator {
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
                unimplemented!("windowed aggregation over maps")
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

impl EvaluatorFactory for FirstMapEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let args = AggregationArgs::from_input(info.args)?;
        match args {
            AggregationArgs::NoWindow { .. } | AggregationArgs::Since { .. } => {
                let map_type = info.result_type;
                let accum = as_map_array(new_empty_array(map_type).as_ref()).to_owned();
                let token = MapAccumToken::new(accum);
                Ok(Box::new(Self { token, args }))
            }
            AggregationArgs::Sliding { .. } => {
                todo!()
            }
        }
    }
}

impl FirstMapEvaluator {
    /// Resizes the accumulator to the new size.
    fn ensure_entity_capacity(token: &mut MapAccumToken, len: usize) -> anyhow::Result<()> {
        Ok(token.resize(len)?)
    }

    fn concat_take(
        array1: &ArrayRef,
        array2: &ArrayRef,
        indices: &UInt32Array,
    ) -> anyhow::Result<ArrayRef> {
        let combined = arrow::compute::concat(&[array1, array2])?;
        Ok(arrow::compute::take(&combined, indices, None)?)
    }

    /// Returns the existing value for an entity if it exists, or a new value from the
    /// input if it exists, or null if neither.
    ///
    /// Takes advantage of the `take` and `concat` kernels to avoid having to type the
    /// evaluator, keeping everything as ArrayRefs.
    ///
    /// The output is taken from the concatenated batch of the old state and the new input.
    /// If the old state's value is null, then the take index for that entity is the length
    /// of the old state plus the current index (i.e. the index into the new input).
    /// If not, then we keep the take index as the old state's index.
    fn aggregate(
        token: &mut MapAccumToken,
        key_capacity: usize,
        key_indices: &UInt32Array,
        input: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        Self::ensure_entity_capacity(token, key_capacity)?;
        let map_input = as_map_array(input);

        let mut take_new_state: Vec<u32> = (0..token.accum.len() as u32).collect();
        let mut take_output_builder = UInt32Array::builder(input.len());
        for input_index in 0..map_input.len() {
            let entity_index = key_indices.value(input_index);
            if token.value_is_null(entity_index) && map_input.is_valid(input_index) {
                // If the `take_new_state[entity_index]` is greater than the length, that
                // means it has been set already, so we should not overwrite it.
                let not_taken = take_new_state[entity_index as usize] < take_new_state.len() as u32;
                if not_taken {
                    take_new_state[entity_index as usize] =
                        (input_index + take_new_state.len()) as u32;
                }
            };

            take_output_builder.append_value(take_new_state[entity_index as usize])
        }

        let take_new_state = PrimitiveArray::from_iter_values(take_new_state);
        let new_state = Self::concat_take(&token.accum, input, &take_new_state)?;
        token.set_state(new_state);

        let output = Self::concat_take(&token.accum, input, &take_output_builder.finish())?;
        Ok(output)
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
        todo!()
    }
}
