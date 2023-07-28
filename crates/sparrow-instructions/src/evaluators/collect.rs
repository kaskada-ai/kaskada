//! The cast instruction isn't a "normal" instruction since it doesn't have a
//! a single, fixed signature. Specifically, the input and output types depend
//! on the input to the instruction and the requested output type.

use std::sync::Arc;

use anyhow::anyhow;
use arrow::array::{ArrayRef, Int32Array, IntervalDayTimeArray, IntervalYearMonthArray};
use arrow::datatypes::DataType;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_kernels::time::i64_to_two_i32;
use sparrow_plan::ValueRef;
use sparrow_syntax::FenlType;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for the `collect` instruction.
///
/// Collect collects a stream of values into a List<T>. A list is produced
/// for each input value received, growing up to a maximum size.
#[derive(Debug)]
pub struct CollectEvaluator {
    max: ValueRef,
    input: ValueRef,
    tick: ValueRef,
    duration: ValueRef,
}

impl EvaluatorFactory for CollectEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input_type = info.args[1].data_type();
        let result_type = info.result_type;
        match result_type {
            DataType::List(t) => anyhow::ensure!(t.data_type() == input_type),
            other => anyhow::bail!("expected list result type, saw {:?}", other),
        };

        let (max, input, tick, duration) = info.unpack_arguments()?;
        Ok(Box::new(Self {
            max,
            input,
            tick,
            duration,
        }))
    }
}

impl CollectEvaluator {
    fn execute(input: &ArrayRef) -> anyhow::Result<ArrayRef> {
        todo!()
    }
}

impl Evaluator for CollectEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let result = CollectEvaluator::execute(&input)?;
        Ok(result)
    }
}
