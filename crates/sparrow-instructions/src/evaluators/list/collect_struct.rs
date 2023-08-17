use crate::ValueRef;
use crate::{CollectStructToken, Evaluator, EvaluatorFactory, RuntimeInfo, StateToken, StaticInfo};
use arrow::array::{
    new_empty_array, Array, ArrayRef, AsArray, ListArray, TimestampNanosecondArray, UInt32Array,
    UInt32Builder,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Int64Type};
use arrow_schema::{Field, TimeUnit};
use itertools::{izip, Itertools};
use sparrow_arrow::scalar_value::ScalarValue;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

/// Evaluator for the `collect` instruction.
///
/// Collects a stream of struct values into a List. A list is produced
/// for each input value received, growing up to a maximum size.
///
/// If the list is empty, an empty list is returned (rather than `null`).
#[derive(Debug)]
pub struct CollectStructEvaluator {
    /// The min size of the buffer.
    ///
    /// If the buffer is smaller than this, a null value
    /// will be produced.
    min: usize,
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
        let input_type = info.args[0].data_type();
        let result_type = info.result_type;
        match result_type {
            DataType::List(t) => {
                anyhow::ensure!(matches!(input_type, DataType::Struct(..)));
                anyhow::ensure!(t.data_type() == input_type);
            }
            other => anyhow::bail!("expected list result type, saw {:?}", other),
        };

        let max = match info.args[1].value_ref.literal_value() {
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
        let min = match info.args[2].value_ref.literal_value() {
            Some(ScalarValue::Int64(Some(v))) if *v < 0 => {
                anyhow::bail!("unexpected value of `min` -- must be >= 0")
            }
            Some(ScalarValue::Int64(Some(v))) => *v as usize,
            // If a user specifies `min = null`, default to 0.
            Some(ScalarValue::Int64(None)) => 0,
            Some(other) => anyhow::bail!("expected i64 for min parameter, saw {:?}", other),
            None => anyhow::bail!("expected literal value for min parameter"),
        };
        debug_assert!(min <= max, "min must be less than max");

        let accum = new_empty_array(result_type).as_list::<i32>().to_owned();
        let token = CollectStructToken::new(Arc::new(accum));
        let (input, _, _, tick, duration) = info.unpack_arguments()?;
        Ok(Box::new(Self {
            min,
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
                Self::evaluate_non_windowed(
                    token,
                    key_capacity,
                    entity_indices,
                    input,
                    self.min,
                    self.max,
                )
            }
            (false, true) => {
                let token = &mut self.token;
                let input = info.value(&self.input)?.array_ref()?;
                let tick = info.value(&self.tick)?.array_ref()?;
                let key_capacity = info.grouping().num_groups();
                let entity_indices = info.grouping().group_indices();
                Self::evaluate_since_windowed(
                    token,
                    key_capacity,
                    entity_indices,
                    input,
                    tick,
                    self.min,
                    self.max,
                )
            }
            (true, false) => {
                let token = &mut self.token;
                let input = info.value(&self.input)?.array_ref()?;
                let input_times = info.time_column().array_ref()?;

                // The duration is the nanosecond time to trail the window by.
                let duration = info
                    .value(&self.duration)?
                    .try_primitive_literal::<Int64Type>()?
                    .ok_or_else(|| anyhow::anyhow!("Expected non-null literal duration"))?;

                let key_capacity = info.grouping().num_groups();
                let entity_indices = info.grouping().group_indices();
                Self::evaluate_trailing_windowed(
                    token,
                    key_capacity,
                    entity_indices,
                    input,
                    input_times,
                    duration,
                    self.min,
                    self.max,
                )
            }
            (false, false) => panic!("sliding window aggregation should use other evaluator"),
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

    /// Construct the entity take indices for the current state.
    fn construct_entity_take_indices(
        token: &mut CollectStructToken,
    ) -> BTreeMap<u32, VecDeque<u32>> {
        let mut entity_take_indices = BTreeMap::<u32, VecDeque<u32>>::new();
        let state = token.state.as_list::<i32>();
        for (index, (start, end)) in state.offsets().iter().tuple_windows().enumerate() {
            // The index of enumeration is the entity index
            entity_take_indices.insert(index as u32, (*start as u32..*end as u32).collect());
        }
        entity_take_indices
    }

    /// Evaluate the collect instruction for non-windowed aggregation.
    ///
    /// This algorithm takes advantage of the fact that [ListArray]s are
    /// represented as a single flattened list of values and a list of offsets.
    /// By constructing the take indices for each entity, we can then use
    /// [sparrow_arrow::concat_take] to efficiently take the values at the indices
    /// we need to construct the output list.
    ///
    /// See the following example:
    ///
    /// Current state: [Ben = [A, B], Jordan = [C, D]]
    /// state.flattened: [A, B, C, D]
    /// state.offsets: [0, 2, 4]
    ///
    /// New Input: [E, F, G]
    /// Entity Indices: [Ben, Jordan, Ben]
    ///
    /// Concat the flattened state with the new input:
    /// concat_state_input: [A, B, C, D, E, F, G]
    ///
    /// Create the current entity take indices:
    /// { Ben: [0, 1], Jordan: [2, 3] }
    ///
    /// For each entity, we need to take the values at the indices
    /// from the new input, append all indices for that entity to the
    /// Output Take Indices, then append the number of indices to the
    /// Output Offset builder:
    ///
    /// Entity: Ben, Input: [E]
    /// Entity Take Indices: { Ben: [0, 1, 4], Jordan: [2, 3] }
    /// Output Take Indices: [0, 1, 4]
    /// Output Offset: [0, 3]
    ///
    /// Entity: Jordan, Input: [F]
    /// Entity Take Indices: { Ben: [0, 1, 4], Jordan: [2, 3, 5] }
    /// Output Take Indices: [0, 1, 4, 2, 3, 5]
    /// Output Offset: [0, 3, 6]
    ///
    /// Entity: Ben, Input: [G]
    /// Entity Take Indices: { Ben: [0, 1, 4, 6], Jordan: [2, 3, 5] }
    /// Output Take Indices: [0, 1, 4, 2, 3, 5, 0, 1, 4, 6]
    /// Output Offset: [0, 3, 6, 10]
    ///
    /// Then, we use [sparrow_arrow::concat_take] to concat the old flattened state
    /// and the input together, then take the Output Take Indices. This constructs an
    /// output:
    ///
    /// concat_state_input: [A, B, C, D, E, F, G]
    /// Output Take Indices: [0, 1, 4, 2, 3, 5, 0, 1, 4, 6]
    /// Output Values: [A, B, E, C, D, F, A, B, E, G]
    ///
    /// Then, with the offsets, we can construct the output lists:
    /// [[A, B, E], [C, D, F], [A, B, E, G]]
    ///
    /// Lastly, the new state must be set.
    /// The Entity Take Indices are flattened and the offsets are constructed.
    ///
    /// Entity Take Indices: { Ben: [0, 1, 4, 6], Jordan: [2, 3, 5] }
    /// Flattened: [0, 1, 4, 6, 2, 3, 5]
    /// Offsets: [0, 4, 7]
    ///
    /// New State: [Ben = [A, B, E, G], Jordan = [C, D, F]]
    fn evaluate_non_windowed(
        token: &mut CollectStructToken,
        key_capacity: usize,
        entity_indices: &UInt32Array,
        input: ArrayRef,
        min: usize,
        max: usize,
    ) -> anyhow::Result<ArrayRef> {
        let input_structs = input.as_struct();
        assert_eq!(entity_indices.len(), input_structs.len());

        Self::ensure_entity_capacity(token, key_capacity)?;

        // Recreate the take indices for the current state
        let mut entity_take_indices = Self::construct_entity_take_indices(token);

        let old_state = token.state.as_list::<i32>();
        let old_state_flat = old_state.values();

        let mut take_output_builder = UInt32Builder::new();
        let mut output_offset_builder = vec![0];

        // Tracks the result's null values
        let mut null_buffer = vec![];

        let mut cur_offset = 0;
        // For each entity, append the take indices for the new input to the existing
        // entity take indices
        for (index, entity_index) in entity_indices.values().iter().enumerate() {
            if input.is_valid(index) {
                let take_index = (old_state_flat.len() + index) as u32;
                entity_take_indices
                    .entry(*entity_index)
                    .and_modify(|v| {
                        v.push_back(take_index);
                        if v.len() > max {
                            v.pop_front();
                        }
                    })
                    .or_insert(vec![take_index].into());
            }

            // safety: map was resized to handle entity_index size
            let entity_take = entity_take_indices.get(entity_index).unwrap();

            if entity_take.len() >= min {
                // Append this entity's take indices to the take output builder
                entity_take.iter().for_each(|i| {
                    take_output_builder.append_value(*i);
                });

                // Append this entity's current number of take indices to the output offset builder
                cur_offset += entity_take.len();

                output_offset_builder.push(cur_offset as i32);
                null_buffer.push(true);
            } else {
                // Append null if there are not enough values
                take_output_builder.append_null();
                null_buffer.push(false);

                // Cur offset increases by 1 to account for the null value
                cur_offset += 1;
                output_offset_builder.push(cur_offset as i32);
            }
        }
        let output_values =
            sparrow_arrow::concat_take(old_state_flat, &input, &take_output_builder.finish())?;

        let fields = input_structs.fields().clone();
        let field = Arc::new(Field::new("item", DataType::Struct(fields.clone()), true));

        let result = ListArray::new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(output_offset_builder)),
            output_values,
            Some(NullBuffer::from(BooleanBuffer::from(null_buffer))),
        );

        // Now update the new state using the last entity take indices
        let new_state = update_token_state(&entity_take_indices, old_state_flat, input, fields)?;
        token.set_state(Arc::new(new_state));

        Ok(Arc::new(result))
    }

    /// Evaluates the collect function for structs with a `since` window.
    ///
    /// State is handled in order of "update -> emit -> reset".
    ///
    /// Follows the same implementation as above, but resets the state of
    /// an entity when a `tick` is seen.
    fn evaluate_since_windowed(
        token: &mut CollectStructToken,
        key_capacity: usize,
        entity_indices: &UInt32Array,
        input: ArrayRef,
        ticks: ArrayRef,
        min: usize,
        max: usize,
    ) -> anyhow::Result<ArrayRef> {
        let ticks = ticks.as_boolean();
        let input_structs = input.as_struct();
        assert_eq!(entity_indices.len(), input_structs.len());

        Self::ensure_entity_capacity(token, key_capacity)?;

        // Recreate the take indices for the current state
        let mut entity_take_indices = Self::construct_entity_take_indices(token);

        let old_state = token.state.as_list::<i32>();
        let old_state_flat = old_state.values();

        let mut take_output_builder = UInt32Builder::new();
        let mut output_offset_builder = vec![0];

        // Tracks the result's null values
        let mut null_buffer = vec![];

        let mut cur_offset = 0;
        // For each entity, append the take indices for the new input to the existing
        // entity take indices
        for (index, (tick, entity_index)) in
            izip!(ticks.values().iter(), entity_indices.values().iter()).enumerate()
        {
            // Update state
            if input.is_valid(index) {
                let take_index = (old_state_flat.len() + index) as u32;
                entity_take_indices
                    .entry(*entity_index)
                    .and_modify(|v| {
                        v.push_back(take_index);
                        if v.len() > max {
                            v.pop_front();
                        }
                    })
                    .or_insert(vec![take_index].into());
            }

            // safety: map was resized to handle entity_index size
            let entity_take = entity_take_indices.get(entity_index).unwrap();

            // Emit state
            if entity_take.len() >= min {
                // Append this entity's take indices to the take output builder
                entity_take.iter().for_each(|i| {
                    take_output_builder.append_value(*i);
                });

                // Append this entity's current number of take indices to the output offset builder
                cur_offset += entity_take.len();

                output_offset_builder.push(cur_offset as i32);
                null_buffer.push(true);
            } else {
                // Append null if there are not enough values
                take_output_builder.append_null();
                null_buffer.push(false);

                // Cur offset increases by 1 to account for the null value
                cur_offset += 1;
                output_offset_builder.push(cur_offset as i32);
            }

            // Reset state
            if ticks.is_valid(index) && tick {
                entity_take_indices.insert(*entity_index, vec![].into());
            }
        }
        let output_values =
            sparrow_arrow::concat_take(old_state_flat, &input, &take_output_builder.finish())?;

        let fields = input_structs.fields().clone();
        let field = Arc::new(Field::new("item", DataType::Struct(fields.clone()), true));

        let result = ListArray::new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(output_offset_builder)),
            output_values,
            Some(NullBuffer::from(BooleanBuffer::from(null_buffer))),
        );

        // Now update the new state using the last entity take indices
        let new_state = update_token_state(&entity_take_indices, old_state_flat, input, fields)?;
        token.set_state(Arc::new(new_state));

        Ok(Arc::new(result))
    }

    /// Evaluates the collect function for structs with a `trailing` window.
    ///
    /// State is handled in order of "update -> emit -> reset".
    ///
    /// Follows the same implementation as above, but includes values in [current time - duration].
    #[allow(clippy::too_many_arguments)]
    fn evaluate_trailing_windowed(
        token: &mut CollectStructToken,
        key_capacity: usize,
        entity_indices: &UInt32Array,
        input: ArrayRef,
        input_times: ArrayRef,
        duration: i64,
        min: usize,
        max: usize,
    ) -> anyhow::Result<ArrayRef> {
        let input_structs = input.as_struct();
        assert_eq!(entity_indices.len(), input_structs.len());

        Self::ensure_entity_capacity(token, key_capacity)?;

        // Recreate the take indices for the current state
        let mut entity_take_indices = Self::construct_entity_take_indices(token);

        let old_state = token.state.as_list::<i32>();
        let old_state_flat = old_state.values();

        let mut take_output_builder = UInt32Builder::new();
        let mut output_offset_builder = vec![0];

        // Tracks the result's null values
        let mut null_buffer = vec![];

        // Concat the state's times and the input's times
        let old_times = token.times.as_list::<i32>();
        let old_times_flat = old_times.values();
        let combined_times =
            arrow::compute::concat(&[old_times_flat.as_ref(), input_times.as_ref()])?;
        let combined_times: &TimestampNanosecondArray = combined_times.as_primitive();
        assert_eq!(
            old_state_flat.len(),
            old_times_flat.len(),
            "time and state length mismatch"
        );

        let mut cur_offset = 0;
        // For each entity, append the take indices for the new input to the existing
        // entity take indices
        for (index, entity_index) in entity_indices.values().iter().enumerate() {
            // Update state
            let take_index = (old_state_flat.len() + index) as u32;
            if input.is_valid(index) {
                entity_take_indices
                    .entry(*entity_index)
                    .and_modify(|v| {
                        v.push_back(take_index);
                        if v.len() > max {
                            v.pop_front();
                        }
                    })
                    .or_insert(vec![take_index].into());

                pop_trailing_window_if_needed(
                    take_index as usize,
                    *entity_index,
                    &mut entity_take_indices,
                    combined_times,
                    duration,
                );
            } else {
                pop_trailing_window_if_needed(
                    take_index as usize,
                    *entity_index,
                    &mut entity_take_indices,
                    combined_times,
                    duration,
                );
            }

            // safety: map was resized to handle entity_index size
            let entity_take = entity_take_indices.get(entity_index).unwrap();

            // Emit state
            if entity_take.len() >= min {
                // Append this entity's take indices to the take output builder
                entity_take.iter().for_each(|i| {
                    take_output_builder.append_value(*i);
                });

                // Append this entity's current number of take indices to the output offset builder
                cur_offset += entity_take.len();

                output_offset_builder.push(cur_offset as i32);
                null_buffer.push(true);
            } else {
                // Append null if there are not enough values
                take_output_builder.append_null();
                null_buffer.push(false);

                // Cur offset increases by 1 to account for the null value
                cur_offset += 1;
                output_offset_builder.push(cur_offset as i32);
            }
        }
        let output_values =
            sparrow_arrow::concat_take(old_state_flat, &input, &take_output_builder.finish())?;

        let fields = input_structs.fields().clone();
        let field = Arc::new(Field::new("item", DataType::Struct(fields.clone()), true));

        let result = ListArray::new(
            field,
            OffsetBuffer::new(ScalarBuffer::from(output_offset_builder)),
            output_values,
            Some(NullBuffer::from(BooleanBuffer::from(null_buffer))),
        );

        // Now update the new state using the last entity take indices
        let new_state = update_token_state(&entity_take_indices, old_state_flat, input, fields)?;
        let new_times = update_token_times(&entity_take_indices, old_times_flat, input_times)?;
        token.set_state_and_time(Arc::new(new_state), Arc::new(new_times));

        Ok(Arc::new(result))
    }
}

/// Pops the front element(s) from the window if time has progressed
/// past the window's duration.
fn pop_trailing_window_if_needed(
    take_index: usize,
    entity_index: u32,
    entity_take_indices: &mut BTreeMap<u32, VecDeque<u32>>,
    combined_times: &arrow::array::PrimitiveArray<arrow::datatypes::TimestampNanosecondType>,
    duration: i64,
) {
    let v = entity_take_indices
        .entry(entity_index)
        .or_insert(vec![].into());

    if let Some(front_i) = v.front() {
        let mut oldest_time = combined_times.value(*front_i as usize);
        // Note this uses the `combined_times` and `take_index`
        // because it's possible we need to pop off new input
        let min_window_start = combined_times.value(take_index) - duration;
        while oldest_time <= min_window_start {
            v.pop_front();
            if let Some(f_i) = v.front() {
                oldest_time = combined_times.value(*f_i as usize);
            } else {
                return;
            }
        }
    }
}

/// Uses the final entity take indices to get the new state
fn update_token_state(
    entity_take_indices: &BTreeMap<u32, VecDeque<u32>>,
    old_state_flat: &Arc<dyn Array>,
    input: Arc<dyn Array>,
    fields: arrow_schema::Fields,
) -> anyhow::Result<ListArray> {
    let mut new_state_offset_builder = Vec::with_capacity(entity_take_indices.len());
    new_state_offset_builder.push(0);

    let mut cur_state_offset = 0;
    let take_new_state = entity_take_indices.values().flat_map(|v| {
        cur_state_offset += v.len() as i32;
        new_state_offset_builder.push(cur_state_offset);
        v.iter().copied().map(Some)
    });
    let take_new_state = UInt32Array::from_iter(take_new_state);

    let new_state_values = sparrow_arrow::concat_take(old_state_flat, &input, &take_new_state)?;
    let new_state = ListArray::new(
        Arc::new(Field::new("item", DataType::Struct(fields), true)),
        OffsetBuffer::new(ScalarBuffer::from(new_state_offset_builder)),
        new_state_values,
        None,
    );
    Ok(new_state)
}

/// Uses the final entity take indices to get the new times
fn update_token_times(
    entity_take_indices: &BTreeMap<u32, VecDeque<u32>>,
    old_times_flat: &Arc<dyn Array>,
    input_times: Arc<dyn Array>,
) -> anyhow::Result<ListArray> {
    let mut new_times_offset_builder = Vec::with_capacity(entity_take_indices.len());
    new_times_offset_builder.push(0);

    let mut cur_times_offset = 0;
    let take_new_times = entity_take_indices.values().flat_map(|v| {
        cur_times_offset += v.len() as i32;
        new_times_offset_builder.push(cur_times_offset);
        v.iter().copied().map(Some)
    });
    let take_new_times = UInt32Array::from_iter(take_new_times);

    let new_times_values =
        sparrow_arrow::concat_take(old_times_flat, &input_times, &take_new_times)?;
    let new_state = ListArray::new(
        Arc::new(Field::new(
            "item",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        )),
        OffsetBuffer::new(ScalarBuffer::from(new_times_offset_builder)),
        new_times_values,
        None,
    );
    Ok(new_state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{
            new_null_array, ArrayBuilder, AsArray, Int64Array, Int64Builder, StringArray,
            StringBuilder, StructArray, StructBuilder,
        },
        buffer::ScalarBuffer,
    };
    use arrow_schema::{DataType, Field, Fields};
    use std::sync::Arc;

    fn default_token() -> CollectStructToken {
        let f = Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("n", DataType::Int64, true),
                Field::new("s", DataType::Utf8, true),
            ])),
            true,
        ));
        let result_type = DataType::List(f);
        let accum = new_empty_array(&result_type).as_list::<i32>().to_owned();
        CollectStructToken::new(Arc::new(accum))
    }

    #[test]
    fn test_basic_collect_multiple_batches() {
        let mut token = default_token();
        // Batch 1
        let n_array = Int64Array::from(vec![Some(0), Some(1), Some(2)]);
        let s_array = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);
        let input = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let input = Arc::new(input);

        let key_indices = UInt32Array::from(vec![0, 0, 0]);
        let key_capacity = 1;

        let result = CollectStructEvaluator::evaluate_non_windowed(
            &mut token,
            key_capacity,
            &key_indices,
            input,
            0,
            usize::MAX,
        )
        .unwrap();
        let result = result.as_list::<i32>();

        // build expected result 1
        let n_array = Int64Array::from(vec![Some(0), Some(0), Some(1), Some(0), Some(1), Some(2)]);
        let s_array = StringArray::from(vec![
            Some("a"),
            Some("a"),
            Some("b"),
            Some("a"),
            Some("b"),
            Some("c"),
        ]);
        let expected = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let offsets = ScalarBuffer::from(vec![0, 1, 3, 6]);
        let expected = ListArray::new(
            Arc::new(Field::new("item", expected.data_type().clone(), true)),
            OffsetBuffer::new(offsets),
            Arc::new(expected),
            None,
        );
        let expected = Arc::new(expected);
        assert_eq!(expected.as_ref(), result);

        // Batch 2
        let n_array = Int64Array::from(vec![Some(3), Some(4), Some(5)]);
        let s_array = StringArray::from(vec![Some("d"), Some("e"), Some("f")]);
        let input = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let input = Arc::new(input);

        // New entity!
        let key_indices = UInt32Array::from(vec![1, 0, 1]);
        let key_capacity = 2;

        let result = CollectStructEvaluator::evaluate_non_windowed(
            &mut token,
            key_capacity,
            &key_indices,
            input,
            0,
            usize::MAX,
        )
        .unwrap();
        let result = result.as_list::<i32>();

        // build expected result 2
        let n_array = Int64Array::from(vec![
            Some(3),
            Some(0),
            Some(1),
            Some(2),
            Some(4),
            Some(3),
            Some(5),
        ]);
        let s_array = StringArray::from(vec![
            Some("d"),
            Some("a"),
            Some("b"),
            Some("c"),
            Some("e"),
            Some("d"),
            Some("f"),
        ]);
        let expected = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let offsets = ScalarBuffer::from(vec![0, 1, 5, 7]);
        let expected = ListArray::new(
            Arc::new(Field::new("item", expected.data_type().clone(), true)),
            OffsetBuffer::new(offsets),
            Arc::new(expected),
            None,
        );
        let expected = Arc::new(expected);
        assert_eq!(expected.as_ref(), result);
    }

    #[test]
    fn test_basic_collect_multiple_batches_with_max() {
        let max = 2;
        let mut token = default_token();
        // Batch 1
        let n_array = Int64Array::from(vec![Some(0), Some(1), Some(2)]);
        let s_array = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);
        let input = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let input = Arc::new(input);

        let key_indices = UInt32Array::from(vec![0, 0, 0]);
        let key_capacity = 1;

        let result = CollectStructEvaluator::evaluate_non_windowed(
            &mut token,
            key_capacity,
            &key_indices,
            input,
            0,
            max,
        )
        .unwrap();
        let result = result.as_list::<i32>();

        // build expected result 1
        let n_array = Int64Array::from(vec![Some(0), Some(0), Some(1), Some(1), Some(2)]);
        let s_array =
            StringArray::from(vec![Some("a"), Some("a"), Some("b"), Some("b"), Some("c")]);
        let expected = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let offsets = ScalarBuffer::from(vec![0, 1, 3, 5]);
        let expected = ListArray::new(
            Arc::new(Field::new("item", expected.data_type().clone(), true)),
            OffsetBuffer::new(offsets),
            Arc::new(expected),
            None,
        );
        let expected = Arc::new(expected);
        assert_eq!(expected.as_ref(), result);

        // Batch 2
        let n_array = Int64Array::from(vec![Some(3), Some(4), Some(5)]);
        let s_array = StringArray::from(vec![Some("d"), Some("e"), Some("f")]);
        let input = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let input = Arc::new(input);

        // New entity!
        let key_indices = UInt32Array::from(vec![1, 0, 1]);
        let key_capacity = 2;

        let result = CollectStructEvaluator::evaluate_non_windowed(
            &mut token,
            key_capacity,
            &key_indices,
            input,
            0,
            max,
        )
        .unwrap();
        let result = result.as_list::<i32>();

        // build expected result 2
        let n_array = Int64Array::from(vec![Some(3), Some(2), Some(4), Some(3), Some(5)]);
        let s_array =
            StringArray::from(vec![Some("d"), Some("c"), Some("e"), Some("d"), Some("f")]);
        let expected = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let offsets = ScalarBuffer::from(vec![0, 1, 3, 5]);
        let expected = ListArray::new(
            Arc::new(Field::new("item", expected.data_type().clone(), true)),
            OffsetBuffer::new(offsets),
            Arc::new(expected),
            None,
        );
        let expected = Arc::new(expected);

        assert_eq!(expected.as_ref(), result);
    }

    #[test]
    fn test_basic_collect_multiple_batches_with_min() {
        let min = 3;
        let mut token = default_token();
        // Batch 1
        let n_array = Int64Array::from(vec![Some(0), Some(1), Some(2)]);
        let s_array = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);
        let input = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let input = Arc::new(input);

        let key_indices = UInt32Array::from(vec![0, 0, 0]);
        let key_capacity = 1;

        let result = CollectStructEvaluator::evaluate_non_windowed(
            &mut token,
            key_capacity,
            &key_indices,
            input,
            min,
            usize::MAX,
        )
        .unwrap();
        let result = result.as_list::<i32>();

        // build expected result 1
        let n_array = Int64Array::from(vec![Some(0), Some(1), Some(2)]);
        let s_array = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);
        let expected = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let offsets = ScalarBuffer::from(vec![0, 3]);
        let expected = ListArray::new(
            Arc::new(Field::new("item", expected.data_type().clone(), true)),
            OffsetBuffer::new(offsets),
            Arc::new(expected),
            None,
        );
        let null_structs = new_null_array(expected.data_type(), 2);
        let expected = Arc::new(expected);
        let expected = arrow::compute::concat(&[null_structs.as_ref(), expected.as_ref()]).unwrap();
        assert_eq!(expected.as_ref(), result);

        // Batch 2
        let n_array = Int64Array::from(vec![Some(3), Some(4), Some(5)]);
        let s_array = StringArray::from(vec![Some("d"), Some("e"), Some("f")]);
        let input = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let input = Arc::new(input);

        // New entity!
        let key_indices = UInt32Array::from(vec![1, 0, 1]);
        let key_capacity = 2;

        let result = CollectStructEvaluator::evaluate_non_windowed(
            &mut token,
            key_capacity,
            &key_indices,
            input,
            min,
            usize::MAX,
        )
        .unwrap();
        let result = result.as_list::<i32>();

        // build expected result 2
        let n_array = Int64Array::from(vec![Some(0), Some(1), Some(2), Some(4)]);
        let s_array = StringArray::from(vec![Some("a"), Some("b"), Some("c"), Some("e")]);
        let expected = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let offsets = ScalarBuffer::from(vec![0, 4]);
        let expected = ListArray::new(
            Arc::new(Field::new("item", expected.data_type().clone(), true)),
            OffsetBuffer::new(offsets),
            Arc::new(expected),
            None,
        );
        let null_structs = new_null_array(expected.data_type(), 1);
        let expected = Arc::new(expected);
        let expected = arrow::compute::concat(&[
            null_structs.as_ref(),
            expected.as_ref(),
            null_structs.as_ref(),
        ])
        .unwrap();
        assert_eq!(expected.as_ref(), result);
    }

    #[test]
    fn test_trailing_collect() {
        let min = 0;
        let max = 10;
        let duration = 6;

        let mut token = default_token();
        // Batch 1
        let n_array = Int64Array::from(vec![Some(0), Some(1), Some(2)]);
        let s_array = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);
        let input = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let input = Arc::new(input);

        let key_indices = UInt32Array::from(vec![0, 0, 0]);
        let key_capacity = 1;

        let input_times = TimestampNanosecondArray::from(vec![0, 5, 10]);
        let input_times = Arc::new(input_times);

        let result = CollectStructEvaluator::evaluate_trailing_windowed(
            &mut token,
            key_capacity,
            &key_indices,
            input,
            input_times,
            duration,
            min,
            max,
        )
        .unwrap();
        let result = result.as_list::<i32>();

        // build expected result 1
        let n_array = Int64Array::from(vec![Some(0), Some(0), Some(1), Some(1), Some(2)]);
        let s_array =
            StringArray::from(vec![Some("a"), Some("a"), Some("b"), Some("b"), Some("c")]);
        let expected = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let offsets = ScalarBuffer::from(vec![0, 1, 3, 5]);
        let expected = ListArray::new(
            Arc::new(Field::new("item", expected.data_type().clone(), true)),
            OffsetBuffer::new(offsets),
            Arc::new(expected),
            None,
        );
        let expected = Arc::new(expected);
        assert_eq!(expected.as_ref(), result);

        // Batch 2
        let n_array = Int64Array::from(vec![Some(3), Some(4), Some(5)]);
        let s_array = StringArray::from(vec![Some("d"), Some("e"), Some("f")]);
        let input = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let input = Arc::new(input);

        // New entity!
        let key_indices = UInt32Array::from(vec![1, 0, 1]);
        let key_capacity = 2;

        let input_times = TimestampNanosecondArray::from(vec![15, 20, 25]);
        let input_times = Arc::new(input_times);

        let result = CollectStructEvaluator::evaluate_trailing_windowed(
            &mut token,
            key_capacity,
            &key_indices,
            input,
            input_times,
            duration,
            min,
            max,
        )
        .unwrap();
        let result = result.as_list::<i32>();

        // build expected result 2
        let n_array = Int64Array::from(vec![Some(3), Some(4), Some(5)]);
        let s_array = StringArray::from(vec![Some("d"), Some("e"), Some("f")]);
        let expected = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let offsets = ScalarBuffer::from(vec![0, 1, 2, 3]);
        let expected = ListArray::new(
            Arc::new(Field::new("item", expected.data_type().clone(), true)),
            OffsetBuffer::new(offsets),
            Arc::new(expected),
            None,
        );
        let expected = Arc::new(expected);
        assert_eq!(expected.as_ref(), result);
    }

    #[test]
    fn test_ignores_null_inputs() {
        let mut token = default_token();
        let fields = Fields::from(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]);
        let field_builders: Vec<Box<dyn ArrayBuilder>> = vec![
            Box::new(Int64Builder::new()),
            Box::new(StringBuilder::new()),
        ];

        // batch
        let mut builder = StructBuilder::new(fields.clone(), field_builders);
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
            .append_null();
        builder
            .field_builder::<StringBuilder>(1)
            .unwrap()
            .append_null();
        builder.append(false);

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

        let key_indices = UInt32Array::from(vec![0, 0, 0, 0]);
        let key_capacity = 1;

        let result = CollectStructEvaluator::evaluate_non_windowed(
            &mut token,
            key_capacity,
            &key_indices,
            input,
            0,
            usize::MAX,
        )
        .unwrap();
        let result = result.as_list::<i32>();

        // build expected result
        let n_array = Int64Array::from(vec![
            Some(0),
            Some(0),
            Some(0),
            Some(1),
            Some(0),
            Some(1),
            Some(2),
        ]);
        let s_array = StringArray::from(vec![
            Some("a"),
            Some("a"),
            Some("a"),
            Some("b"),
            Some("a"),
            Some("b"),
            Some("c"),
        ]);
        let expected = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let offsets = ScalarBuffer::from(vec![0, 1, 2, 4, 7]);
        let expected = ListArray::new(
            Arc::new(Field::new("item", expected.data_type().clone(), true)),
            OffsetBuffer::new(offsets),
            Arc::new(expected),
            None,
        );
        let expected = Arc::new(expected);
        assert_eq!(expected.as_ref(), result);
    }
}
