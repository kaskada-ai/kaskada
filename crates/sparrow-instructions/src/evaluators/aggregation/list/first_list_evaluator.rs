use std::sync::Arc;

use crate::ValueRef;
use crate::{
    AggregationArgs, Evaluator, EvaluatorFactory, ListAccumToken, RuntimeInfo, StateToken,
    StaticInfo,
};
use arrow::array::{
    as_list_array, new_empty_array, Array, ArrayRef, AsArray, PrimitiveArray, UInt32Array,
};

/// Evaluator for the `First` instruction on lists
pub struct FirstListEvaluator {
    args: AggregationArgs<ValueRef>,
    token: ListAccumToken,
}

impl Evaluator for FirstListEvaluator {
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
                unimplemented!("windowed aggregation over lists")
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

impl EvaluatorFactory for FirstListEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let args = AggregationArgs::from_input(info.args)?;
        match args {
            AggregationArgs::NoWindow { .. } | AggregationArgs::Since { .. } => {
                let list_type = info.result_type;
                let accum = new_empty_array(list_type).as_list::<i32>().to_owned();
                let token = ListAccumToken::new(Arc::new(accum));
                Ok(Box::new(Self { token, args }))
            }
            AggregationArgs::Sliding { .. } => {
                unimplemented!("sliding window aggregation over list unsupported")
            }
        }
    }
}

impl FirstListEvaluator {
    /// Resizes the accumulator to the new size.
    fn ensure_entity_capacity(token: &mut ListAccumToken, len: usize) -> anyhow::Result<()> {
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
        token: &mut ListAccumToken,
        key_capacity: usize,
        key_indices: &UInt32Array,
        input: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        Self::ensure_entity_capacity(token, key_capacity)?;
        let list_input = as_list_array(input);

        let mut take_new_state: Vec<u32> = (0..token.accum.len() as u32).collect();
        let mut take_output_builder = UInt32Array::builder(input.len());
        for input_index in 0..list_input.len() {
            let entity_index = key_indices.value(input_index);
            if token.value_is_null(entity_index) && list_input.is_valid(input_index) {
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
    use arrow::array::{AsArray, Int64Builder, ListBuilder};
    use arrow_schema::{DataType, Field};
    use std::sync::Arc;

    fn default_token() -> ListAccumToken {
        let f = Arc::new(Field::new("item", DataType::Int64, true));
        let list = DataType::List(f);
        let accum = new_empty_array(&list);
        ListAccumToken { accum }
    }

    #[test]
    fn test_first_list_multiple_batches() {
        let mut token = default_token();
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
            FirstListEvaluator::aggregate(&mut token, key_capacity, &key_indices, &input).unwrap();
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
            FirstListEvaluator::aggregate(&mut token, key_capacity, &key_indices, &input).unwrap();
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
}
