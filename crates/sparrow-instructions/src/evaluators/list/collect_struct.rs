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
            (true, true) => {
                let token = &mut self.token;
                let input = info.value(&self.input)?.array_ref()?;
                let key_capacity = info.grouping().num_groups();
                let entity_indices = info.grouping().group_indices();
                Self::evaluate_non_windowed(token, key_capacity, entity_indices, input, self.max)
            }
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
    fn ensure_entity_capacity(token: &mut CollectStructToken, len: usize) -> anyhow::Result<()> {
        token.resize(len)
    }

    fn evaluate_non_windowed(
        token: &mut CollectStructToken,
        key_capacity: usize,
        entity_indices: &UInt32Array,
        input: ArrayRef,
        max: usize,
    ) -> anyhow::Result<ArrayRef> {
        let input_structs = input.as_struct();
        assert_eq!(entity_indices.len(), input_structs.len());

        Self::ensure_entity_capacity(token, key_capacity)?;

        let old_state = token.state.as_list::<i32>();
        let old_state_flat = old_state.values();

        // TODO: size hint?
        let mut take_output_builder = UInt32Builder::new();
        let mut output_offset_builder = BufferBuilder::new(input.len());

        let mut cur_offset = 0;
        output_offset_builder.append(0);
        for (index, entity_index) in entity_indices.values().iter().enumerate() {
            let take_index = (old_state_flat.len() + index) as u32;
            token
                .entity_take_indices
                .entry(*entity_index)
                .and_modify(|v| {
                    v.push_back(take_index);
                    if v.len() > max {
                        v.pop_front();
                    }
                })
                .or_insert(vec![take_index].into());

            // already verified key exists, or created entry if not, in previous step
            let entity_take = token.entity_take_indices.get(entity_index).unwrap();
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

        let mut new_state_offset_builder = BufferBuilder::new(token.entity_take_indices.len());
        new_state_offset_builder.append(0);
        let mut cur_state_offset = 0;

        // TODO: These are referencing indices in the `input`
        // They need to be only referencing items in "new_state"
        // Recreate entity state each time
        let take_new_state = token.entity_take_indices.values().flat_map(|v| {
            // TODO: suspect non-deterministic mapping here
            // when I was using hashmap vs btree
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
        token.set_state(Arc::new(new_state));

        Ok(Arc::new(result))
    }
}

#[cfg(test)]
mod tests {
    use crate::StaticArg;

    use super::*;
    use arrow::array::{ArrayBuilder, AsArray, Int64Builder, MapBuilder};
    use arrow_schema::{DataType, Field, Fields};
    use sparrow_plan::{InstKind, InstOp};
    use std::sync::Arc;

    fn fields() -> Fields {
        Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ])
    }

    fn field_builders() -> Vec<Box<dyn ArrayBuilder>> {
        let field_builders: Vec<Box<dyn ArrayBuilder>> = vec![
            Box::new(Int64Builder::new()),
            Box::new(StringBuilder::new()),
        ];
        field_builders
    }

    fn default_token() -> CollectStructToken {
        let f = Arc::new(Field::new("item", DataType::Struct(fields()), true));
        let result_type = DataType::List(f);
        let accum = new_empty_array(&result_type).as_list::<i32>().to_owned();
        CollectStructToken::new(Arc::new(accum))
    }

    #[test]
    fn test_basic_collect_multiple_batches() {
        let mut token = default_token();
        // Batch 1
        let mut builder = StructBuilder::new(fields(), field_builders());
        builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(0);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("a");
        builder.append(true);

        builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(1);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("b");
        builder.append(true);

        builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(2);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("c");
        builder.append(true);
        let input = builder.finish();
        let input = Arc::new(input);

        let key_indices = UInt32Array::from(vec![0, 0, 0]);
        let key_capacity = 1;

        let result = CollectStructEvaluator::evaluate_non_windowed(
            &mut token,
            key_capacity,
            &key_indices,
            input,
            usize::MAX,
        )
        .unwrap();
        let result = result.as_list::<i32>();

        // build expected result 1
        let mut builder = ListBuilder::new(StructBuilder::new(fields(), field_builders()));
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(0);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("a");
        builder.values().append(true);
        builder.append(true);

        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(0);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("a");
        builder.values().append(true);
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(1);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("b");
        builder.values().append(true);
        builder.append(true);

        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(0);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("a");
        builder.values().append(true);
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(1);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("b");
        builder.values().append(true);
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(2);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("c");
        builder.values().append(true);
        builder.append(true);
        let expected = builder.finish();
        let expected = Arc::new(expected);

        assert_eq!(expected.as_ref(), result);

        // Batch 2
        let mut builder = StructBuilder::new(fields(), field_builders());
        builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(3);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("d");
        builder.append(true);

        builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(4);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("e");
        builder.append(true);

        builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(5);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("f");
        builder.append(true);
        let input = builder.finish();
        let input = Arc::new(input);

        // New entity!
        let key_indices = UInt32Array::from(vec![1, 0, 1]);
        let key_capacity = 2;

        let result = CollectStructEvaluator::evaluate_non_windowed(
            &mut token,
            key_capacity,
            &key_indices,
            input,
            usize::MAX,
        )
        .unwrap();
        let result = result.as_list::<i32>();

        // build expected result 2
        let mut builder = ListBuilder::new(StructBuilder::new(fields(), field_builders()));
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(3);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("d");
        builder.values().append(true);
        builder.append(true);

        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(0);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("a");
        builder.values().append(true);
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(1);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("b");
        builder.values().append(true);
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(2);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("c");
        builder.values().append(true);
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(4);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("e");
        builder.values().append(true);
        builder.append(true);

        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(3);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("d");
        builder.values().append(true);
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(5);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("f");
        builder.values().append(true);
        builder.append(true);

        let expected = builder.finish();
        let expected = Arc::new(expected);

        assert_eq!(expected.as_ref(), result);
    }

    #[test]
    fn test_basic_collect_multiple_batches_with_max() {
        let max = 2;
        let mut token = default_token();
        // Batch 1
        let mut builder = StructBuilder::new(fields(), field_builders());
        builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(0);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("a");
        builder.append(true);

        builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(1);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("b");
        builder.append(true);

        builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(2);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("c");
        builder.append(true);
        let input = builder.finish();
        let input = Arc::new(input);

        let key_indices = UInt32Array::from(vec![0, 0, 0]);
        let key_capacity = 1;

        let result = CollectStructEvaluator::evaluate_non_windowed(
            &mut token,
            key_capacity,
            &key_indices,
            input,
            max,
        )
        .unwrap();
        let result = result.as_list::<i32>();

        // build expected result 1
        let mut builder = ListBuilder::new(StructBuilder::new(fields(), field_builders()));
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(0);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("a");
        builder.values().append(true);
        builder.append(true);

        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(0);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("a");
        builder.values().append(true);
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(1);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("b");
        builder.values().append(true);
        builder.append(true);

        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(1);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("b");
        builder.values().append(true);
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(2);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("c");
        builder.values().append(true);
        builder.append(true);
        let expected = builder.finish();
        let expected = Arc::new(expected);

        assert_eq!(expected.as_ref(), result);

        // Batch 2
        let mut builder = StructBuilder::new(fields(), field_builders());
        builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(3);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("d");
        builder.append(true);

        builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(4);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("e");
        builder.append(true);

        builder
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(5);
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("f");
        builder.append(true);
        let input = builder.finish();
        let input = Arc::new(input);

        // New entity!
        let key_indices = UInt32Array::from(vec![1, 0, 1]);
        let key_capacity = 2;

        let result = CollectStructEvaluator::evaluate_non_windowed(
            &mut token,
            key_capacity,
            &key_indices,
            input,
            max,
        )
        .unwrap();
        let result = result.as_list::<i32>();

        // build expected result 2
        let mut builder = ListBuilder::new(StructBuilder::new(fields(), field_builders()));
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(3);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("d");
        builder.values().append(true);
        builder.append(true);

        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(2);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("c");
        builder.values().append(true);
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(4);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("e");
        builder.values().append(true);
        builder.append(true);

        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(3);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("d");
        builder.values().append(true);
        builder
            .values()
            .field_builder::<Int64Builder>(0)
            .unwrap()
            .append_value(5);
        builder
            .values()
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_value("f");
        builder.values().append(true);
        builder.append(true);

        let expected = builder.finish();
        let expected = Arc::new(expected);

        assert_eq!(expected.as_ref(), result);
    }
}
