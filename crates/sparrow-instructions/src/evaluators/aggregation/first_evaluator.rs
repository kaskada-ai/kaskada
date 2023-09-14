use crate::ValueRef;
use crate::{
    AggregationArgs, ArrayRefAccumToken, Evaluator, EvaluatorFactory, RuntimeInfo, StateToken,
    StaticInfo,
};
use arrow::array::{Array, ArrayRef, BooleanArray, PrimitiveArray, UInt32Array};

/// Evaluator for the `First` instruction.
pub struct FirstEvaluator {
    args: AggregationArgs<ValueRef>,
    token: ArrayRefAccumToken,
}

impl Evaluator for FirstEvaluator {
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

impl EvaluatorFactory for FirstEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let args = AggregationArgs::from_input(info.args)?;
        match args {
            AggregationArgs::NoWindow { .. } | AggregationArgs::Since { .. } => {
                let token = ArrayRefAccumToken::empty(info.result_type);
                Ok(Box::new(Self { token, args }))
            }
            AggregationArgs::Sliding { .. } => {
                unimplemented!("sliding window aggregation over list unsupported")
            }
        }
    }
}

impl FirstEvaluator {
    /// Resizes the accumulator to the new size.
    fn ensure_entity_capacity(token: &mut ArrayRefAccumToken, len: usize) -> anyhow::Result<()> {
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
        token: &mut ArrayRefAccumToken,
        key_capacity: usize,
        key_indices: &UInt32Array,
        input: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        Self::ensure_entity_capacity(token, key_capacity)?;

        let mut take_new_state: Vec<u32> = (0..token.accum.len() as u32).collect();
        let mut take_output_builder = UInt32Array::builder(input.len());
        for input_index in 0..input.len() {
            let entity_index = key_indices.value(input_index) as usize;
            if token.value_is_null(entity_index) && input.is_valid(input_index) {
                // If the `take_new_state[entity_index]` is greater than the length, that
                // means it has been set already, so we should not overwrite it.
                //
                // Note that we only check if the token was previously null.
                let not_taken = take_new_state[entity_index] < take_new_state.len() as u32;
                if not_taken {
                    take_new_state[entity_index] = (input_index + take_new_state.len()) as u32;
                }
            };

            take_output_builder.append_value(take_new_state[entity_index])
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
    fn aggregate_since(
        token: &mut ArrayRefAccumToken,
        key_capacity: usize,
        key_indices: &UInt32Array,
        input: &ArrayRef,
        ticks: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        Self::ensure_entity_capacity(token, key_capacity)?;

        let mut take_new_state: Vec<Option<u32>> = (0..token.accum.len())
            .map(|index| {
                if token.accum.is_valid(index) {
                    Some(index as u32)
                } else {
                    None
                }
            })
            .collect();

        let mut take_output_builder = UInt32Array::builder(input.len());
        for input_index in 0..input.len() {
            let entity_index = key_indices.value(input_index) as usize;
            if input.is_valid(input_index) && take_new_state[entity_index].is_none() {
                take_new_state[entity_index] = Some((input_index + take_new_state.len()) as u32);
            };

            take_output_builder.append_option(take_new_state[entity_index]);

            if ticks.value(input_index) && ticks.is_valid(input_index) {
                take_new_state[entity_index] = None;
            }
        }

        // Gather the output, using the previous state and the new input
        let output =
            sparrow_arrow::concat_take(&token.accum, input, &take_output_builder.finish())?;

        // Update the state token with the new state
        let take_new_state = PrimitiveArray::from_iter(take_new_state);
        let new_state = sparrow_arrow::concat_take(&token.accum, input, &take_new_state)?;
        token.set_state(new_state);

        Ok(output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Float64Array, Int64Builder, ListBuilder, MapBuilder};
    use arrow_schema::{DataType, Field, Fields};
    use std::sync::Arc;

    fn empty_list_i64_token() -> ArrayRefAccumToken {
        let f = Arc::new(Field::new("item", DataType::Int64, true));
        let list = DataType::List(f);
        ArrayRefAccumToken::empty(&list)
    }

    #[test]
    fn test_first_list_multiple_batches() {
        let mut token = empty_list_i64_token();
        let key_indices = UInt32Array::from(vec![0, 0, 0, 0, 0, 0]);
        let key_capacity = 1;

        // Batch 1
        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.append_value([Some(1), Some(2), Some(3)]);
        builder.append_value([Some(4), None, Some(5)]);
        builder.append_value([None, None]);
        builder.append(false);
        builder.append_value([]);
        builder.append_value([Some(7), Some(8), Some(9)]);

        let array = builder.finish();

        let input: ArrayRef = Arc::new(array);
        let result =
            FirstEvaluator::aggregate(&mut token, key_capacity, &key_indices, &input).unwrap();
        let result = result.as_list();

        let mut builder = ListBuilder::new(Int64Builder::new());
        for _ in 0..6 {
            builder.append_value([Some(1), Some(2), Some(3)]);
        }
        let expected = builder.finish();

        assert_eq!(&expected, result);

        // Batch 2
        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.append_value([Some(10), Some(11)]);
        builder.append(true);
        builder.append_value([Some(13), None]);
        builder.append(false);
        builder.append(false);
        builder.append_value([Some(14)]);

        let array = builder.finish();
        let input: ArrayRef = Arc::new(array);

        // Introduce more entities
        let key_indices = UInt32Array::from(vec![0, 1, 2, 1, 0, 1]);
        let key_capacity = 3;
        let result =
            FirstEvaluator::aggregate(&mut token, key_capacity, &key_indices, &input).unwrap();
        let result = result.as_list();

        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.append_value([Some(1), Some(2), Some(3)]);
        builder.append(true);
        builder.append_value([Some(13), None]);
        builder.append(true);
        builder.append_value([Some(1), Some(2), Some(3)]);
        builder.append(true);
        let expected = builder.finish();

        assert_eq!(&expected, result);
    }

    fn empty_map_i64_token() -> ArrayRefAccumToken {
        let k = Field::new("keys", DataType::Int64, false);
        let v = Field::new("values", DataType::Int64, true);
        let fields = Fields::from(vec![k, v]);
        let s = Arc::new(Field::new("entries", DataType::Struct(fields), false));
        let map = DataType::Map(s, false);
        ArrayRefAccumToken::empty(&map)
    }

    #[test]
    fn test_first_map_multiple_batches() {
        let mut token = empty_map_i64_token();
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
            FirstEvaluator::aggregate(&mut token, key_capacity, &key_indices, &input).unwrap();
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
            FirstEvaluator::aggregate(&mut token, key_capacity, &key_indices, &input).unwrap();
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

    #[test]
    fn test_first_f64() {
        let mut token = ArrayRefAccumToken::empty(&DataType::Float64);
        let entity_indices = UInt32Array::from(vec![0, 1, 2, 1, 1]);
        let input: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(1.0),
            Some(2.0),
            None,
            None,
            Some(3.0),
        ]));

        let output = FirstEvaluator::aggregate(&mut token, 3, &entity_indices, &input).unwrap();

        let output: &Float64Array = output.as_primitive();
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
        let output = FirstEvaluator::aggregate(&mut token, 3, &entity_indices, &input).unwrap();
        let output: &Float64Array = output.as_primitive();
        assert_eq!(
            output,
            &Float64Array::from(vec![Some(1.0), Some(2.0), Some(5.0), Some(2.0), Some(2.0)])
        );
    }
}
