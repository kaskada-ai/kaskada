//! The cast instruction isn't a "normal" instruction since it doesn't have a
//! a single, fixed signature. Specifically, the input and output types depend
//! on the input to the instruction and the requested output type.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{ArrayRef, ListBuilder, PrimitiveBuilder};
use arrow::datatypes::{ArrowPrimitiveType, DataType};

use itertools::izip;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_arrow::scalar_value::ScalarValue;

use sparrow_plan::ValueRef;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for the `collect` instruction.
///
/// Collect collects a stream of values into a List<T>. A list is produced
/// for each input value received, growing up to a maximum size.
///
/// If the list is empty, an empty list is returned (rather than `null`).
#[derive(Debug)]
pub struct CollectPrimitiveEvaluator<T>
where
    T: ArrowPrimitiveType,
{
    /// The max size of the buffer.
    ///
    /// Once the max size is reached, the front will be popped and the new
    /// value pushed to the back.
    max: i64,
    input: ValueRef,
    tick: ValueRef,
    duration: ValueRef,
    /// Contains the buffer of values for each entity
    buffers: Vec<VecDeque<Option<T::Native>>>,
}

impl<T> EvaluatorFactory for CollectPrimitiveEvaluator<T>
where
    T: ArrowPrimitiveType + Send + Sync,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input_type = info.args[1].data_type();
        let result_type = info.result_type;
        match result_type {
            DataType::List(t) => anyhow::ensure!(t.data_type() == input_type),
            other => anyhow::bail!("expected list result type, saw {:?}", other),
        };

        let max = match info.args[0].value_ref.literal_value() {
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
            buffers: vec![],
        }))
    }
}

impl<T> Evaluator for CollectPrimitiveEvaluator<T>
where
    T: ArrowPrimitiveType + Send + Sync,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        match (self.tick.is_literal_null(), self.duration.is_literal_null()) {
            (true, true) => self.evaluate_non_windowed(info),
            (true, false) => unimplemented!("since window aggregation unsupported"),
            (false, false) => panic!("sliding window aggregation should use other evaluator"),
            (_, _) => anyhow::bail!("saw invalid combination of tick and duration"),
        }
    }
}

impl<T> CollectPrimitiveEvaluator<T>
where
    T: ArrowPrimitiveType + Send + Sync,
{
    fn ensure_entity_capacity(&mut self, len: usize) {
        if len >= self.buffers.len() {
            self.buffers.resize(len + 1, VecDeque::new());
        }
    }

    fn evaluate_non_windowed(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let key_capacity = info.grouping().num_groups();
        let entity_indices = info.grouping().group_indices();
        assert_eq!(entity_indices.len(), input.len());

        self.ensure_entity_capacity(key_capacity);

        let input = downcast_primitive_array::<T>(input.as_ref())?;
        let builder = PrimitiveBuilder::<T>::new();
        let mut list_builder = ListBuilder::new(builder);

        izip!(entity_indices.values(), input).for_each(|(entity_index, input)| {
            let entity_index = *entity_index as usize;

            self.buffers[entity_index].push_back(input);
            if self.buffers[entity_index].len() > self.max as usize {
                self.buffers[entity_index].pop_front();
            }

            list_builder.append_value(self.buffers[entity_index].clone());
        });

        Ok(Arc::new(list_builder.finish()))
    }
}
