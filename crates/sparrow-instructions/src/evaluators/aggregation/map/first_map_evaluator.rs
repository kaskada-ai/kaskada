use std::sync::Arc;

use crate::ValueRef;
use crate::{
    AggregationArgs, Evaluator, EvaluatorFactory, MapAccumToken, RuntimeInfo, StateToken,
    StaticInfo,
};
use arrow::array::{
    as_map_array, new_empty_array, Array, ArrayRef, AsArray, PrimitiveArray, UInt32Array,
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
            AggregationArgs::Since { ticks: _, input: _ } => {
                unimplemented!("windowed aggregation over maps")
            }
            AggregationArgs::Sliding { .. } => {
                panic!("expected non-windowed or since-windowed aggregation, saw sliding.")
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
                let accum = new_empty_array(map_type).as_map().to_owned();
                let token = MapAccumToken::new(Arc::new(accum));
                Ok(Box::new(Self { token, args }))
            }
            AggregationArgs::Sliding { .. } => {
                unimplemented!("sliding window aggregation over maps unsupported")
            }
        }
    }
}

impl FirstMapEvaluator {
    /// Resizes the accumulator to the new size.
    fn ensure_entity_capacity(token: &mut MapAccumToken, len: usize) -> anyhow::Result<()> {
        token.resize(len)
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

        // Gather the output, using the previous state and the new input
        let output =
            sparrow_arrow::concat_take(&token.accum, input, &take_output_builder.finish())?;

        // Update the state token with the new state
        let take_new_state = PrimitiveArray::from_iter_values(take_new_state);
        let new_state = sparrow_arrow::concat_take(&token.accum, input, &take_new_state)?;
        token.set_state(new_state);

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Int64Builder, MapBuilder};
    use arrow_schema::{DataType, Field, Fields};
    use std::sync::Arc;

    fn default_token() -> MapAccumToken {
        let k = Field::new("keys", DataType::Int64, false);
        let v = Field::new("values", DataType::Int64, true);
        let fields = Fields::from(vec![k, v]);
        let s = Arc::new(Field::new("entries", DataType::Struct(fields), false));
        let map = DataType::Map(s, false);
        let accum = new_empty_array(&map);
        MapAccumToken { accum }
    }

    #[test]
    fn test_first_map_multiple_batches() {
        let mut token = default_token();
        let key_indices = UInt32Array::from(vec![0, 0, 0, 0, 0]);
        let key_capacity = 1;

        // Batch 1
        let mut builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        builder.keys().append_value(1);
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.keys().append_value(1);
        builder.values().append_value(2);
        builder.keys().append_value(2);
        builder.values().append_value(4);
        builder.append(true).unwrap();

        builder.append(true).unwrap();

        builder.keys().append_value(2);
        builder.values().append_value(99);
        builder.append(true).unwrap();

        builder.keys().append_value(1);
        builder.values().append_value(10);
        builder.keys().append_value(3);
        builder.values().append_value(7);
        builder.append(true).unwrap();
        let array = builder.finish();

        let input: ArrayRef = Arc::new(array);
        let result =
            FirstMapEvaluator::aggregate(&mut token, key_capacity, &key_indices, &input).unwrap();
        let result = result.as_map();

        let mut builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        for _ in 0..5 {
            builder.keys().append_value(1);
            builder.values().append_value(1);
            builder.append(true).unwrap();
        }
        let expected = builder.finish();

        assert_eq!(&expected, result);

        // Batch 2
        let mut builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        builder.keys().append_value(1);
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.keys().append_value(1);
        builder.values().append_value(2);
        builder.keys().append_value(2);
        builder.values().append_value(4);
        builder.append(true).unwrap();

        builder.append(true).unwrap();

        builder.keys().append_value(2);
        builder.values().append_value(99);
        builder.append(true).unwrap();

        builder.keys().append_value(1);
        builder.values().append_value(10);
        builder.keys().append_value(3);
        builder.values().append_value(7);
        builder.append(true).unwrap();

        let array = builder.finish();
        let input: ArrayRef = Arc::new(array);

        // Introduce second entity key
        let key_indices = UInt32Array::from(vec![0, 1, 0, 1, 0]);
        let key_capacity = 2;
        let result =
            FirstMapEvaluator::aggregate(&mut token, key_capacity, &key_indices, &input).unwrap();
        let result = result.as_map();

        let mut builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());
        builder.keys().append_value(1);
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.keys().append_value(1);
        builder.values().append_value(2);
        builder.keys().append_value(2);
        builder.values().append_value(4);
        builder.append(true).unwrap();

        builder.keys().append_value(1);
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.keys().append_value(1);
        builder.values().append_value(2);
        builder.keys().append_value(2);
        builder.values().append_value(4);
        builder.append(true).unwrap();

        builder.keys().append_value(1);
        builder.values().append_value(1);
        builder.append(true).unwrap();
        let expected = builder.finish();

        assert_eq!(&expected, result);
    }
}
