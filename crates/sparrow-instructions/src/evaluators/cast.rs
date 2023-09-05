//! The cast instruction isn't a "normal" instruction since it doesn't have a
//! a single, fixed signature. Specifically, the input and output types depend
//! on the input to the instruction and the requested output type.

use std::sync::Arc;

use crate::ValueRef;
use anyhow::anyhow;
use arrow::array::{
    Array, ArrayRef, AsArray, Int32Array, Int64Array, IntervalDayTimeArray, IntervalYearMonthArray,
};
use arrow::datatypes::DataType;
use arrow_schema::TimeUnit;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_kernels::time::i64_to_two_i32;
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
            // Support explicit casting from a duration to a duration. Note that this means
            // that casting to a less granular unit will lose precision.
            //
            // Arrow doesn't currently support this directly, so this hacks it a bit.
            (DataType::Duration(_), DataType::Duration(_)) => true,
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
            (DataType::Null, _) => {
                // TODO: https://github.com/apache/arrow-rs/issues/684 is fixed,
                // use existing arrow cast kernels.
                Ok(sparrow_arrow::scalar_value::ScalarValue::try_new_null(to)?
                    .to_array(input.len()))
            }
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
            // Arrow does not support casting directly from duration to duration types, but it does
            // support casting to the underlying data type (int64) then back to any duration type.
            // This hacks that cast by doing the multiplication manually then casting back to the
            // desired type.
            (DataType::Duration(tu1), DataType::Duration(tu2)) => {
                cast_duration(input.as_ref(), tu1, tu2)
            }
            _ => arrow::compute::cast(input, to).map_err(|e| e.into()),
        }
    }
}

fn time_unit_nanos(unit: &TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    }
}

fn cast_duration(input: &dyn Array, from: &TimeUnit, to: &TimeUnit) -> anyhow::Result<ArrayRef> {
    let input: ArrayRef = arrow::compute::cast(input, &DataType::Int64)?;
    let input: &Int64Array = input.as_primitive();

    let from_nanos = time_unit_nanos(from);
    let to_nanos = time_unit_nanos(to);

    let result = if from_nanos < to_nanos {
        arrow_arith::arithmetic::multiply_scalar(input, to_nanos / from_nanos)?
    } else {
        arrow_arith::arithmetic::divide_scalar(input, from_nanos / to_nanos)?
    };

    let result = arrow::compute::cast(&result, &DataType::Duration(to.clone()))?;
    Ok(result)
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
