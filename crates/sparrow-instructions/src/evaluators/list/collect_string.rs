use crate::{CollectToken, Evaluator, EvaluatorFactory, RuntimeInfo, StateToken, StaticInfo};
use arrow::array::{ArrayRef, AsArray, ListBuilder, StringBuilder};
use arrow::datatypes::DataType;
use itertools::izip;
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_plan::ValueRef;
use std::sync::Arc;

/// Evaluator for the `collect` instruction.
///
/// Collect collects a stream of values into a List<O>. A list is produced
/// for each input value received, growing up to a maximum size.
///
/// If the list is empty, an empty list is returned (rather than `null`).
#[derive(Debug)]
pub struct CollectStringEvaluator {
    /// The max size of the buffer.
    ///
    /// Once the max size is reached, the front will be popped and the new
    /// value pushed to the back.
    max: i64,
    input: ValueRef,
    tick: ValueRef,
    duration: ValueRef,
    /// Contains the buffer of values for each entity
    token: CollectToken<String>,
}

impl EvaluatorFactory for CollectStringEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input_type = info.args[1].data_type();
        let result_type = info.result_type;
        match result_type {
            DataType::List(t) => anyhow::ensure!(t.data_type() == input_type),
            other => anyhow::bail!("expected list result type, saw {:?}", other),
        };

        let max = match &info.args[0].value_ref.literal_value() {
            Some(ScalarValue::Int64(Some(v))) => *v,
            Some(other) => anyhow::bail!("expected i64 for max parameter, saw {:?}", other),
            None => anyhow::bail!("expected literal value for max parameter"),
        };

        let (_, input, tick, duration) = info.unpack_arguments()?;
        Ok(Box::new(Self {
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
            (true, false) => unimplemented!("since window aggregation unsupported"),
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

            self.token
                .add_value(self.max as usize, entity_index, input.map(|s| s.to_owned()));
            let cur_list = self.token.state(entity_index);

            list_builder.append_value(cur_list.clone());
        });

        Ok(Arc::new(list_builder.finish()))
    }
}
