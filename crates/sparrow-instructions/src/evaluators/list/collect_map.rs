//! The cast instruction isn't a "normal" instruction since it doesn't have a
//! a single, fixed signature. Specifically, the input and output types depend
//! on the input to the instruction and the requested output type.

use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::anyhow;
use arrow::array::{
    ArrayRef, Int32Array, IntervalDayTimeArray, IntervalYearMonthArray, ListBuilder,
    PrimitiveBuilder,
};
use arrow::datatypes::{ArrowPrimitiveType, DataType};
use arrow::downcast_primitive_array;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_kernels::time::i64_to_two_i32;
use sparrow_plan::ValueRef;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

/// Evaluator for the `collect` instruction.
///
/// Collect collects a stream of values into a List<T>. A list is produced
/// for each input value received, growing up to a maximum size.
#[derive(Debug)]
pub struct CollectMapEvaluator {
    /// The max size of the buffer.
    ///
    /// Once the max size is reached, the front will be popped and the new
    /// value pushed to the back.
    _max: i64,
    _input: ValueRef,
    _tick: ValueRef,
    _duration: ValueRef,
}

impl EvaluatorFactory for CollectMapEvaluator {
    fn try_new(_info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        unimplemented!("map collect evaluator is unsupported")
    }
}

impl Evaluator for CollectMapEvaluator {
    fn evaluate(&mut self, _info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        unimplemented!("map collect evaluator is unsupported")
    }
}
