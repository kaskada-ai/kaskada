use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use crate::{
    CollectStructToken, Evaluator, EvaluatorFactory, LagStructToken, RuntimeInfo, StateToken,
    StaticInfo,
};
use anyhow::anyhow;
use arrow::array::{
    new_empty_array, Array, ArrayRef, AsArray, ListArray, UInt32Array, UInt32Builder,
};
use arrow::array::{
    Int32Array, IntervalDayTimeArray, IntervalYearMonthArray, TimestampNanosecondArray,
};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Int64Type, IntervalDayTimeType, IntervalUnit,
    IntervalYearMonthType, TimeUnit, TimestampNanosecondType,
};
use arrow::temporal_conversions::timestamp_ns_to_datetime;
use arrow_schema::Field;
use chrono::Datelike;
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::Serialize;
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_kernels::lag::LagPrimitive;
use sparrow_plan::ValueRef;

/// Evaluator for the `Lag` instruction.
pub(super) struct StructLagEvaluator {
    input: ValueRef,
    lag: usize,
    token: LagStructToken,
}

impl Evaluator for StructLagEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let token = &mut self.token;
        let grouping = info.grouping();
        let input = info.value(&self.input)?.array_ref()?;
        let key_capacity = info.grouping().num_groups();
        let entity_indices = info.grouping().group_indices();
        Self::evaluate(token, key_capacity, entity_indices, input, self.lag)
    }

    fn state_token(&self) -> Option<&dyn StateToken> {
        Some(&self.token)
    }

    fn state_token_mut(&mut self) -> Option<&mut dyn StateToken> {
        Some(&mut self.token)
    }
}

impl StructLagEvaluator {
    fn ensure_entity_capacity(token: &mut LagStructToken, len: usize) -> anyhow::Result<()> {
        token.resize(len)
    }

    /// Construct the entity take indices for the current state.
    fn construct_entity_take_indices(token: &mut LagStructToken) -> BTreeMap<u32, VecDeque<u32>> {
        let mut entity_take_indices = BTreeMap::<u32, VecDeque<u32>>::new();
        let state = token.state.as_list::<i32>();
        for (index, (start, end)) in state.offsets().iter().tuple_windows().enumerate() {
            // The index of enumeration is the entity index
            entity_take_indices.insert(index as u32, (*start as u32..*end as u32).collect());
        }
        entity_take_indices
    }

    fn evaluate(
        token: &mut LagStructToken,
        key_capacity: usize,
        entity_indices: &UInt32Array,
        input: ArrayRef,
        lag: usize,
    ) -> anyhow::Result<ArrayRef> {
        let input_structs = input.as_struct();
        assert_eq!(entity_indices.len(), input_structs.len());

        Self::ensure_entity_capacity(token, key_capacity)?;

        // Recreate the take indices for the current state
        let mut entity_take_indices = Self::construct_entity_take_indices(token);

        let old_state = token.state.as_list::<i32>();
        let old_state_flat = old_state.values();

        let mut take_output_builder = UInt32Builder::new();

        // For each entity, append the take indices for the new input to the existing
        // entity take indices
        for (index, entity_index) in entity_indices.values().iter().enumerate() {
            if input.is_valid(index) {
                let take_index = (old_state_flat.len() + index) as u32;
                entity_take_indices
                    .entry(*entity_index)
                    .and_modify(|v| {
                        v.push_back(take_index);
                        if v.len() > lag {
                            v.pop_front();
                        }
                    })
                    .or_insert(vec![take_index].into());
            }

            // safety: map was resized to handle entity_index size
            let entity_take = entity_take_indices.get(entity_index).unwrap();

            if entity_take.len() == lag {
                take_output_builder.append_value(entity_take[0]);
            } else {
                // Append null if there are not enough values
                take_output_builder.append_null();
            }
        }
        let result =
            sparrow_arrow::concat_take(old_state_flat, &input, &take_output_builder.finish())?;

        // Construct the new state offset and values using the current entity take indices
        let mut new_state_offset_builder = Vec::with_capacity(entity_take_indices.len());
        new_state_offset_builder.push(0);
        let mut cur_state_offset = 0;

        let take_new_state = entity_take_indices.values().flat_map(|v| {
            cur_state_offset += v.len() as i32;
            new_state_offset_builder.push(cur_state_offset);
            v.iter().copied().map(Some)
        });
        let take_new_state = UInt32Array::from_iter(take_new_state);

        let fields = input_structs.fields().clone();
        let new_state_values = sparrow_arrow::concat_take(old_state_flat, &input, &take_new_state)?;
        let new_state = ListArray::new(
            Arc::new(Field::new("item", DataType::Struct(fields), true)),
            OffsetBuffer::new(ScalarBuffer::from(new_state_offset_builder)),
            new_state_values,
            None,
        );

        token.set_state(Arc::new(new_state));
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for StructLagEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let lag = info.args[0].value_ref.literal_value().ok_or_else(|| {
            anyhow!(
                "Expected value of lag to be a literal, was {:?}",
                info.args[0]
            )
        })?;
        match lag {
            ScalarValue::Int64(None) => Err(anyhow!("Unexpected `null` size for lag")),
            ScalarValue::Int64(Some(lag)) if *lag <= 0 => {
                Err(anyhow!("Unexpected value of lag ({}) -- must be > 0", lag))
            }
            ScalarValue::Int64(Some(lag)) => {
                let lag = *lag as usize;
                let result_type = info.result_type;
                let f = Arc::new(Field::new("item", result_type.clone(), true));
                let accum_type = DataType::List(f);
                let accum = new_empty_array(&accum_type).as_list::<i32>().to_owned();
                let token = LagStructToken::new(Arc::new(accum));
                // TODO: Pass the value of `lag` to the state.
                let (_, input) = info.unpack_arguments()?;
                Ok(Box::new(Self { input, lag, token }))
            }
            unexpected => anyhow::bail!("Unexpected literal {:?} for lag", unexpected),
        }
    }
}

/// Evaluator for the `Lag` instruction.
pub(super) struct PrimitiveLagEvaluator<T: ArrowPrimitiveType> {
    input: ValueRef,
    lag: usize,
    state: LagPrimitive<T>,
}

impl<T> Evaluator for PrimitiveLagEvaluator<T>
where
    T: ArrowPrimitiveType,
    T::Native: Serialize + DeserializeOwned + Copy,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let grouping = info.grouping();
        self.state.execute(
            grouping.num_groups(),
            grouping.group_indices(),
            &info.value(&self.input)?.array_ref()?,
            self.lag,
        )
    }

    fn state_token(&self) -> Option<&dyn StateToken> {
        Some(&self.state)
    }

    fn state_token_mut(&mut self) -> Option<&mut dyn StateToken> {
        Some(&mut self.state)
    }
}

impl<T> EvaluatorFactory for PrimitiveLagEvaluator<T>
where
    T: ArrowPrimitiveType,
    T::Native: Serialize + DeserializeOwned + Copy,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (lag, input) = info.unpack_arguments()?;
        let lag = lag
            .literal_value()
            .ok_or_else(|| anyhow!("Expected value of lag to be a literal, was {:?}", lag))?;
        match lag {
            ScalarValue::Int64(None) => Err(anyhow!("Unexpected `null` size for lag")),
            ScalarValue::Int64(Some(lag)) if *lag <= 0 => {
                Err(anyhow!("Unexpected value of lag ({}) -- must be > 0", lag))
            }
            ScalarValue::Int64(Some(lag)) => {
                let lag = *lag as usize;
                // TODO: Pass the value of `lag` to the state.
                let state = LagPrimitive::new();
                Ok(Box::new(Self { input, lag, state }))
            }
            unexpected => anyhow::bail!("Unexpected literal {:?} for lag", unexpected),
        }
    }
}
