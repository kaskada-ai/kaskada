//! The cast instruction isn't a "normal" instruction since it doesn't have a
//! a single, fixed signature. Specifically, the input and output types depend
//! on the input to the instruction and the requested output type.

use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::anyhow;
use arrow::array::{
    ArrayRef, AsArray, BooleanBuilder, Int32Array, IntervalDayTimeArray, IntervalYearMonthArray,
    ListBuilder, PrimitiveBuilder,
};
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use arrow::downcast_primitive_array;
use itertools::izip;
use sparrow_arrow::downcast::{downcast_boolean_array, downcast_primitive_array};
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_kernels::time::i64_to_two_i32;
use sparrow_plan::ValueRef;
use sparrow_syntax::FenlType;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for the `collect` instruction.
///
/// Collect collects a stream of values into a List. A list is produced
/// for each input value received, growing up to a maximum size.
///
/// If the list is empty, an empty list is returned (rather than `null`).
#[derive(Debug)]
pub struct CollectBooleanEvaluator {
    /// The max size of the buffer.
    ///
    /// Once the max size is reached, the front will be popped and the new
    /// value pushed to the back.
    max: i64,
    input: ValueRef,
    tick: ValueRef,
    duration: ValueRef,
    /// Contains the buffer of values for each entity
    buffers: Vec<VecDeque<Option<bool>>>,
}

impl EvaluatorFactory for CollectBooleanEvaluator {
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
            buffers: vec![].into(),
        }))
    }
}

impl Evaluator for CollectBooleanEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        match (self.tick.is_literal_null(), self.duration.is_literal_null()) {
            (true, true) => self.evaluate_non_windowed(info),
            (true, false) => unimplemented!("since window aggregation unsupported"),
            (false, false) => panic!("sliding window aggregation should use other evaluator"),
            (_, _) => anyhow::bail!("saw invalid combination of tick and duration"),
        }
    }
}

impl CollectBooleanEvaluator {
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

        let input = input.as_boolean();
        let builder = BooleanBuilder::new();
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
