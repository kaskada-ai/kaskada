use crate::{
    CollectToken, Evaluator, EvaluatorFactory, RuntimeInfo, SlidingCollectToken, StateToken,
    StaticInfo,
};
use anyhow::anyhow;
use arrow::array::{ArrayRef, AsArray, ListBuilder, StringBuilder};
use arrow::datatypes::{DataType, Int64Type};
use itertools::izip;
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_plan::ValueRef;
use std::sync::Arc;

/// Evaluator for the `collect` instruction.
///
/// Collects a stream of values into a List. A list is produced
/// for each input value received, growing up to a maximum size.
///
/// If the list is empty, an empty list is returned (rather than `null`).
#[derive(Debug)]
pub struct SlidingCollectStringEvaluator {
    /// The max size of the buffer.
    ///
    /// Once the max size is reached, the front will be popped and the new
    /// value pushed to the back.
    max: usize,
    input: ValueRef,
    tick: ValueRef,
    duration: ValueRef,
    /// Contains the buffer of values for each entity
    token: SlidingCollectToken<String>,
}

impl EvaluatorFactory for SlidingCollectStringEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input_type = info.args[1].data_type();
        let result_type = info.result_type;
        match result_type {
            DataType::List(t) => anyhow::ensure!(t.data_type() == input_type),
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

        let (_, input, tick, duration) = info.unpack_arguments()?;
        Ok(Box::new(Self {
            max,
            input,
            tick,
            duration,
            token: SlidingCollectToken::default(),
        }))
    }
}

impl Evaluator for SlidingCollectStringEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        match (self.tick.is_literal_null(), self.duration.is_literal_null()) {
            (true, true) => panic!("non-windowed aggregation should use other evaluator"),
            (false, true) => panic!("since-window aggregation should use other evaluator"),
            (false, false) => self.evaluate(info),
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

impl SlidingCollectStringEvaluator {
    fn ensure_entity_capacity(&mut self, len: usize) {
        self.token.resize(len)
    }

    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let key_capacity = info.grouping().num_groups();
        let entity_indices = info.grouping().group_indices();
        assert_eq!(entity_indices.len(), input.len());

        self.ensure_entity_capacity(key_capacity);

        let input = input.as_string::<i32>();
        let ticks = info.value(&self.tick)?.array_ref()?;
        let ticks = ticks.as_boolean();

        // The number of overlapping windows at any given time
        let num_windows = info
            .value(&self.duration)?
            .try_primitive_literal::<Int64Type>()?
            .ok_or_else(|| anyhow!("Expected non-null literal duration"))?;
        if num_windows <= 0 {
            anyhow::bail!(
                "Expected positive duration for sliding window, saw {:?}",
                num_windows
            );
        }

        let builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(builder);

        izip!(entity_indices.values(), ticks, input).for_each(|(entity_index, tick, input)| {
            let entity_index = *entity_index as usize;

            self.token
                .add_value(self.max, entity_index, input.map(|s| s.to_owned()));
            let cur_list = self.token.state(entity_index);

            list_builder.append_value(cur_list.clone());

            match tick {
                Some(t) if t => {
                    self.token.reset(entity_index);
                }
                _ => (), // Tick is false or null, so do nothing.
            }
        });

        Ok(Arc::new(list_builder.finish()))
    }
}
