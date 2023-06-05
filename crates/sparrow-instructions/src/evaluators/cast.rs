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

/// Evaluator for the `cast` instruction.
#[derive(Debug)]
pub struct CastEvaluator {
    input: ValueRef,
    data_type: DataType,
}

impl CastEvaluator {
    /// Indicates whether a fenl type can be cast to another.
    ///
    /// For now, this method returns false if the given types are not
    /// concrete `DataTypes` and are not castable. Future work may be done
    /// to support custom fenl types that need to be cast, such as window
    /// behaviors.
    pub fn is_supported_fenl(from: &FenlType, to: &FenlType) -> bool {
        match (from, to) {
            (_, FenlType::Error) => true,
            (FenlType::Concrete(from), FenlType::Concrete(to)) => Self::is_supported(from, to),
            (_, _) => false,
        }
    }

    pub fn is_supported(from: &DataType, to: &DataType) -> bool {
        match (from, to) {
            _ if arrow::compute::can_cast_types(from, to) => true,
            (
                DataType::Interval(
                    arrow::datatypes::IntervalUnit::DayTime
                    | arrow::datatypes::IntervalUnit::YearMonth,
                ),
                ty,
            ) if CastEvaluator::is_supported_interval_cast(ty) => true,
            _ => false,
        }
    }

    // [Interval::DayTime] is represented as an i64, with 32 bits representing
    // the day, and 32 bits representing the milliseconds. We support custom casts
    // that use just the bits representing the day, then cast them to the requested
    // type. However, as the interval can be negative, uints are not supported.
    fn is_supported_interval_cast(data_type: &DataType) -> bool {
        matches!(
            data_type,
            DataType::Int32 | DataType::Int64 | DataType::Float32 | DataType::Float64
        )
    }

    fn execute(input: &ArrayRef, to: &DataType) -> anyhow::Result<ArrayRef> {
        match (input.data_type(), to) {
            (DataType::Interval(interval_unit), to_type) => match interval_unit {
                arrow::datatypes::IntervalUnit::DayTime => {
                    let input: &IntervalDayTimeArray = downcast_primitive_array(input.as_ref())?;
                    let input: Int32Array =
                        arrow::compute::kernels::arity::unary(input, |n| i64_to_two_i32(n).0);
                    let input: ArrayRef = Arc::new(input);

                    // Int32 can cast to all supported int, uint, and float types
                    arrow::compute::cast(&input, to_type).map_err(|e| e.into())
                }
                arrow::datatypes::IntervalUnit::YearMonth => {
                    let input: &IntervalYearMonthArray = downcast_primitive_array(input.as_ref())?;
                    let result: Int32Array = arrow::compute::kernels::arity::unary(input, |n| n);
                    Ok(Arc::new(result))
                }
                arrow::datatypes::IntervalUnit::MonthDayNano => Err(anyhow!(
                    "Unsupported input type for 'cast': {:?}",
                    input.data_type()
                )),
            },
            _ => arrow::compute::cast(input, to).map_err(|e| e.into()),
        }
    }
}

impl Evaluator for CastEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let result = CastEvaluator::execute(&input, &self.data_type)?;
        Ok(result)
    }
}

impl EvaluatorFactory for CastEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let data_type = info.result_type.clone();
        let input = info.unpack_argument()?;

        Ok(Box::new(CastEvaluator { input, data_type }))
    }
}
