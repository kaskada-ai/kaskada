use crate::{
    CollectStructToken, CollectToken, Evaluator, EvaluatorFactory, RuntimeInfo, StateToken,
    StaticInfo,
};
use arrow::array::{
    new_empty_array, Array, ArrayRef, AsArray, BufferBuilder, ListArray, ListBuilder,
    PrimitiveArray, StringBuilder, StructBuilder, UInt32Array, UInt32Builder,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, UInt32Type};
use arrow_schema::Field;
use hashbrown::HashMap;
use itertools::izip;
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_plan::ValueRef;
use std::collections::VecDeque;
use std::sync::Arc;

/// Evaluator for the `collect` instruction.
///
/// Collects a stream of values into a List. A list is produced
/// for each input value received, growing up to a maximum size.
///
/// If the list is empty, an empty list is returned (rather than `null`).
#[derive(Debug)]
pub struct CollectStructEvaluator {
    /// The max size of the buffer.
    ///
    /// Once the max size is reached, the front will be popped and the new
    /// value pushed to the back.
    max: usize,
    input: ValueRef,
    tick: ValueRef,
    duration: ValueRef,
    /// Contains the buffer of values for each entity
    token: CollectStructToken,
}

impl EvaluatorFactory for CollectStructEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input_type = info.args[1].data_type();
        let result_type = info.result_type;
        match result_type {
            DataType::List(t) => {
                anyhow::ensure!(matches!(input_type, DataType::Struct(..)));
                anyhow::ensure!(t.data_type() == input_type);
            }
            other => anyhow::bail!("expected list result type, saw {:?}", other),
        };

        let max = match info.args[0].value_ref.literal_value() {
            Some(ScalarValue::Int64(Some(v))) if *v <= 0 => {
                anyhow::bail!("unexpected value of `max` -- must be > 0")
            }
            Some(ScalarValue::Int64(Some(v))) => *v as usize,
            // If a user specifies `max = null`, we use usize::MAX value as a way
            // to have an "unlimited" buffer.
            Some(ScalarValue::Int64(None)) => usize::MAX,
            Some(other) => anyhow::bail!("expected i64 for max parameter, saw {:?}", other),
            None => anyhow::bail!("expected literal value for max parameter"),
        };

        let accum = new_empty_array(result_type).as_list::<i32>().to_owned();
        let token = CollectStructToken::new(Arc::new(accum));

        let (_, input, tick, duration) = info.unpack_arguments()?;
        Ok(Box::new(Self {
            max,
            input,
            tick,
            duration,
            token,
        }))
    }
}

impl Evaluator for CollectStructEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        match (self.tick.is_literal_null(), self.duration.is_literal_null()) {
            (true, true) => self.evaluate_non_windowed(info),
            (false, true) => panic!("sdf"),
            (false, false) => panic!("sliding window aggregation should use other evaluator"),
            (_, _) => anyhow::bail!("saw invalid combination of tick and duration"),
        }
    }

    fn state_token(&self) -> Option<&dyn StateToken> {
        Some(&self.token)
    }

    fn state_token_mut(&mut self) -> Option<&mut dyn StateToken> {
        Some(&mut self.token)
    }
}

impl CollectStructEvaluator {
    fn ensure_entity_capacity(&mut self, len: usize) -> anyhow::Result<()> {
        self.token.resize(len)
    }

    fn evaluate_non_windowed(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let input_structs = input.as_struct();
        let key_capacity = info.grouping().num_groups();
        let entity_indices = info.grouping().group_indices();
        assert_eq!(entity_indices.len(), input_structs.len());

        self.ensure_entity_capacity(key_capacity)?;

        let old_state = self.token.state.as_list::<i32>();
        // This is a bunch of structs
        let old_state_flat = old_state.values();
        // These offsets indicate the entity index boundaries.
        // let old_state_offsets = old_state.offsets();

        // let mut entity_take_indices: &HashMap<u32, VecDeque<u32>> = &self.token.entity_take_indices;

        // I want each "input" iteration to correspond to multiple indices, because
        // we want to grab multiple structs to make that single list (they're flat).
        // hence the index -> Vec<take index>

        // 1. Initialize the old take indices (or keep them stored, whatever)
        // 2. For each input (which is a struct), add the current index + the length of
        // old state (flattened) to that entities vecdeque. (if we exceed max vecdeque, pop).

        // TODO: size hint?
        let mut take_output_builder = UInt32Builder::new();
        let mut output_offset_builder = BufferBuilder::new(input.len());

        let mut cur_offset = 0;
        output_offset_builder.append(0);
        for (index, entity_index) in entity_indices.values().iter().enumerate() {
            let take_index = (old_state_flat.len() + index) as u32;
            println!("Take index: {:?}", take_index);
            // TODO: Pop?
            self.token
                .entity_take_indices
                .entry(*entity_index)
                .and_modify(|v| v.push_back(take_index))
                .or_insert(vec![take_index].into());

            // already verified key exists, or created entry if not, in previous step
            let entity_take = self.token.entity_take_indices.get(entity_index).unwrap();
            println!("Entity: {}, take: {:?}", entity_index, entity_take);

            // Append this entity's take indices to the take output builder
            entity_take.iter().for_each(|i| {
                take_output_builder.append_value(*i as u32);
            });

            // Append this entity's current number of take indices to the output offset builder
            cur_offset += entity_take.len();
            output_offset_builder.append(cur_offset as u32);
            println!("Appended offset: {:}", cur_offset);
        }
        let output_values =
            sparrow_arrow::concat_take(&old_state_flat, &input, &take_output_builder.finish())?;

        let fields = input_structs.fields().clone();
        let field = Arc::new(Field::new("item", DataType::Struct(fields.clone()), true));

        let result = ListArray::new(
            field.clone(),
            OffsetBuffer::new(output_offset_builder.finish().into()),
            output_values,
            None,
        );

        println!("Output: {:?}", result);

        let mut new_state_offset_builder = BufferBuilder::new(self.token.entity_take_indices.len());
        new_state_offset_builder.append(0);
        let mut cur_state_offset = 0;
        let take_new_state = self.token.entity_take_indices.values().flat_map(|v| {
            cur_state_offset += v.len() as u32;
            new_state_offset_builder.append(cur_state_offset);
            v.iter().copied().map(Some)
        });
        let take_new_state = UInt32Array::from_iter(take_new_state);

        let new_state_values =
            sparrow_arrow::concat_take(&old_state_flat, &input, &take_new_state)?;

        let new_state = ListArray::new(
            Arc::new(Field::new("item", DataType::Struct(fields), true)),
            OffsetBuffer::new(new_state_offset_builder.finish().into()),
            new_state_values,
            None,
        );

        println!("New state: {:?}", new_state);
        self.token.set_state(Arc::new(new_state));

        Ok(Arc::new(result))
    }

    // /// Since windows follow the pattern "update -> emit -> reset".
    // ///
    // /// i.e. if an input appears in the same row as a tick, then that value will
    // /// be included in the output before the tick causes the state to be cleared.
    // /// However, note that ticks are generated with a maximum subsort value, so it is
    // /// unlikely an input naturally appears in the same row as a tick. It is more likely
    // /// that an input may appear at the same time, but an earlier subsort value.
    // fn evaluate_since_windowed(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
    //     let input = info.value(&self.input)?.array_ref()?;
    //     let key_capacity = info.grouping().num_groups();
    //     let entity_indices = info.grouping().group_indices();
    //     assert_eq!(entity_indices.len(), input.len());

    //     self.ensure_entity_capacity(key_capacity);

    //     let input = input.as_string::<i32>();
    //     let ticks = info.value(&self.tick)?.array_ref()?;
    //     let ticks = ticks.as_boolean();

    //     let builder = StringBuilder::new();
    //     let mut list_builder = ListBuilder::new(builder);

    //     izip!(entity_indices.values(), ticks, input).for_each(|(entity_index, tick, input)| {
    //         let entity_index = *entity_index as usize;

    //         self.token
    //             .add_value(self.max, entity_index, input.map(|s| s.to_owned()));
    //         let cur_list = self.token.state(entity_index);

    //         list_builder.append_value(cur_list.clone());

    //         match tick {
    //             Some(t) if t => {
    //                 self.token.reset(entity_index);
    //             }
    //             _ => (), // Tick is false or null, so do nothing.
    //         }
    //     });

    //     Ok(Arc::new(list_builder.finish()))
    // }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{AsArray, Int64Builder, MapBuilder};
    use arrow_schema::{DataType, Field, Fields};
    use std::sync::Arc;

    fn default_token() -> CollectStructToken {
        //
        todo!()
    }

    #[test]
    fn test_() {
        // Batch 1
        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.append_value([Some(1), Some(2), Some(3)]);
        builder.append_value([Some(4), None, Some(5)]);
        builder.append_value([None, None]);
        builder.append(false);
        builder.append_value([]);
        builder.append_value([Some(7), Some(8), Some(9)]);

        let array = builder.finish();

        println!("Lenght: {:?}", array.len());
    }
}
