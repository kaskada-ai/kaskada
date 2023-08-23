use crate::ValueRef;
use crate::{CollectToken, Evaluator, EvaluatorFactory, RuntimeInfo, StateToken, StaticInfo};
use arrow::array::{ArrayRef, AsArray, ListBuilder, StringBuilder, TimestampNanosecondArray};
use arrow::datatypes::{DataType, Int64Type};
use itertools::izip;
use sparrow_arrow::scalar_value::ScalarValue;
use std::sync::Arc;

/// Evaluator for the `collect` instruction.
///
/// Collects a stream of values into a List. A list is produced
/// for each input value received, growing up to a maximum size.
///
/// If the list is empty, an empty list is returned (rather than `null`).
#[derive(Debug)]
pub struct CollectStringEvaluator {
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
    token: CollectToken<String>,
}

impl EvaluatorFactory for CollectStringEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input_type = info.args[0].data_type();
        let result_type = info.result_type;
        match result_type {
            DataType::List(t) => anyhow::ensure!(t.data_type() == input_type),
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
        assert!(min < max, "min must be less than max");

        let (input, _, _, tick, duration) = info.unpack_arguments()?;
        Ok(Box::new(Self {
            min,
            max,
            input,
            tick,
            duration,
            token: CollectToken::default(),
        }))
    }
}

impl Evaluator for CollectStringEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        match (self.tick.is_literal_null(), self.duration.is_literal_null()) {
            (true, true) => self.evaluate_non_windowed(info),
            (false, true) => self.evaluate_since_windowed(info),
            (true, false) => self.evaluate_trailing_windowed(info),
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

impl CollectStringEvaluator {
    fn ensure_entity_capacity(&mut self, len: usize) {
        self.token.resize(len)
    }

    fn evaluate_non_windowed(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let key_capacity = info.grouping().num_groups();
        let entity_indices = info.grouping().group_indices();
        assert_eq!(entity_indices.len(), input.len());

        self.ensure_entity_capacity(key_capacity);

        let input = input.as_string::<i32>();
        let builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(builder);

        izip!(entity_indices.values(), input).for_each(|(entity_index, input)| {
            let entity_index = *entity_index as usize;

            // Do not collect null values
            if let Some(input) = input {
                self.token
                    .add_value(self.max, entity_index, input.to_owned());
            }

            let cur_list = self.token.state(entity_index);
            if cur_list.len() >= self.min {
                list_builder.append_value(cur_list.iter().map(Some));
            } else {
                list_builder.append_null();
            }
        });

        Ok(Arc::new(list_builder.finish()))
    }

    /// Since windows follow the pattern "update -> emit -> reset".
    ///
    /// i.e. if an input appears in the same row as a tick, then that value will
    /// be included in the output before the tick causes the state to be cleared.
    /// However, note that ticks are generated with a maximum subsort value, so it is
    /// unlikely an input naturally appears in the same row as a tick. It is more likely
    /// that an input may appear at the same time, but an earlier subsort value.
    fn evaluate_since_windowed(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let key_capacity = info.grouping().num_groups();
        let entity_indices = info.grouping().group_indices();
        assert_eq!(entity_indices.len(), input.len());

        self.ensure_entity_capacity(key_capacity);

        let input = input.as_string::<i32>();
        let ticks = info.value(&self.tick)?.array_ref()?;
        let ticks = ticks.as_boolean();

        let builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(builder);

        izip!(entity_indices.values(), ticks, input).for_each(|(entity_index, tick, input)| {
            let entity_index = *entity_index as usize;

            // Update state
            // Do not collect null values
            if let Some(input) = input {
                self.token
                    .add_value(self.max, entity_index, input.to_owned());
            }

            // Emit state
            let cur_list = self.token.state(entity_index);
            if cur_list.len() >= self.min {
                list_builder.append_value(cur_list.iter().map(Some));
            } else {
                list_builder.append_null();
            }

            // Reset state
            if let Some(true) = tick {
                self.token.reset(entity_index);
            }
        });

        Ok(Arc::new(list_builder.finish()))
    }

    /// Trailing windows emit values from the window of the current point to the
    /// current time minus the given duration.
    fn evaluate_trailing_windowed(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let duration = info
            .value(&self.duration)?
            .try_primitive_literal::<Int64Type>()?
            .ok_or_else(|| anyhow::anyhow!("Expected non-null literal duration"))?;
        debug_assert!(duration > 0);

        let input = info.value(&self.input)?.array_ref()?;
        let key_capacity = info.grouping().num_groups();
        let entity_indices = info.grouping().group_indices();
        assert_eq!(entity_indices.len(), input.len());

        self.ensure_entity_capacity(key_capacity);

        let input = input.as_string::<i32>();
        let input_times = info.time_column().array_ref()?;
        let input_times: &TimestampNanosecondArray = input_times.as_primitive();

        let builder = StringBuilder::new();
        let mut list_builder = ListBuilder::new(builder);

        izip!(entity_indices.values(), input, input_times.values()).for_each(
            |(entity_index, input, input_time)| {
                let entity_index = *entity_index as usize;

                // Update state
                // Do not collect null values
                if let Some(input) = input {
                    self.token.add_value_with_time(
                        self.max,
                        entity_index,
                        input.to_owned(),
                        *input_time,
                        duration,
                    );
                } else {
                    self.token.check_time(entity_index, *input_time, duration);
                }

                // Emit state
                let cur_list = self.token.state(entity_index);
                if cur_list.len() >= self.min {
                    list_builder.append_value(cur_list.iter().map(Some));
                } else {
                    list_builder.append_null();
                }
            },
        );

        Ok(Arc::new(list_builder.finish()))
    }
}
