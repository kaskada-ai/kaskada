//! The cast instruction isn't a "normal" instruction since it doesn't have a
//! a single, fixed signature. Specifically, the input and output types depend
//! on the input to the instruction and the requested output type.

use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::anyhow;
use arrow::array::{
    ArrayRef, GenericStringBuilder, Int32Array, IntervalDayTimeArray, IntervalYearMonthArray,
    ListBuilder, OffsetSizeTrait, PrimitiveBuilder,
};
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use arrow::downcast_primitive_array;
use sparrow_arrow::downcast::{downcast_primitive_array, downcast_string_array};
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_plan::ValueRef;
use sparrow_syntax::FenlType;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for the `collect` instruction.
///
/// Collect collects a stream of values into a List<O>. A list is produced
/// for each input value received, growing up to a maximum size.
#[derive(Debug)]
pub struct CollectStringEvaluator<'a, O>
where
    O: OffsetSizeTrait,
{
    /// The max size of the buffer.
    ///
    /// Once the max size is reached, the front will be popped and the new
    /// value pushed to the back.
    max: i64,
    input: ValueRef,
    tick: ValueRef,
    duration: ValueRef,
    buffer: VecDeque<Option<&'a str>>,

    _phantom: std::marker::PhantomData<O>,
}

impl<'a, O> EvaluatorFactory for CollectStringEvaluator<'a, O>
where
    O: OffsetSizeTrait,
{
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input_type = info.args[1].data_type();
        let result_type = info.result_type;
        match result_type {
            DataType::List(t) => anyhow::ensure!(t.data_type() == input_type),
            other => anyhow::bail!("expected list result type, saw {:?}", other),
        };

        let max = match info.args[0].value_ref.literal_value() {
            Some(ScalarValue::Int64(Some(v))) => v,
            Some(other) => anyhow::bail!("expected i64 for max parameter, saw {:?}", other),
            None => anyhow::bail!("expected literal value for max parameter"),
        };

        let (_, input, tick, duration) = info.unpack_arguments()?;
        Ok(Box::new(Self {
            max: *max,
            input,
            tick,
            duration,
            buffer: vec![].into(),
            _phantom: std::marker::PhantomData,
        }))
    }
}

impl<'a, O> Evaluator for CollectStringEvaluator<'a, O>
where
    O: OffsetSizeTrait,
{
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;

        match (self.tick.is_literal_null(), self.duration.is_literal_null()) {
            (true, true) => self.evaluate_non_windowed(info),
            (true, false) => unimplemented!("since window aggregation unsupported"),
            (false, false) => panic!("sliding window aggregation should use other evaluator"),
            (_, _) => anyhow::bail!("saw invalid combination of tick and duration"),
        }
    }
}

impl<'a, O> CollectStringEvaluator<'a, O>
where
    O: OffsetSizeTrait,
{
    fn evaluate_non_windowed(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let input = downcast_string_array::<O>(input.as_ref())?;
        let mut builder = GenericStringBuilder::<O>::new();
        let mut list_builder = ListBuilder::new(builder);

        input.into_iter().for_each(|i| {
            self.buffer.push_back(i);
            if self.buffer.len() > self.max as usize {
                self.buffer.pop_front();
            }

            // TODO: Empty is null or empty?
            list_builder.append_value(self.buffer.clone());
            list_builder.append(true);
        });

        Ok(Arc::new(list_builder.finish()))
    }
}
