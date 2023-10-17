use std::borrow::Cow;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int32Array, IntervalDayTimeArray, IntervalYearMonthArray};
use arrow_schema::DataType;
use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_arrow::utils::make_null_array;

use crate::time::i64_to_two_i32;
use sparrow_interfaces::expression::{
    ArrayRefValue, Error, Evaluator, EvaluatorFactory, StaticInfo, WorkArea,
};

inventory::submit!(EvaluatorFactory {
    name: "cast",
    create: &create,
});

/// Evaluator for the `cast` instructino.
struct CastEvaluator {
    input: ArrayRefValue,
    to: DataType,
}

impl Evaluator for CastEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let result = cast(input, &self.to)?;
        Ok(Arc::new(result))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let to = info.result_type.clone();
    let input = info.unpack_argument()?;
    error_stack::ensure!(
        is_supported(&to, input.data_type),
        Error::Unsupported(Cow::Owned(format!(
            "Unsupported cast from {:?} to {to:?}",
            input.data_type
        )))
    );

    let input = input.array_ref();
    Ok(Box::new(CastEvaluator { input, to }))
}

fn is_supported(from: &DataType, to: &DataType) -> bool {
    match (from, to) {
        _ if arrow_cast::can_cast_types(from, to) => true,
        (
            DataType::Interval(
                arrow_schema::IntervalUnit::DayTime | arrow_schema::IntervalUnit::YearMonth,
            ),
            ty,
        ) if is_supported_interval_cast(ty) => true,
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

fn cast(input: &ArrayRef, to: &DataType) -> error_stack::Result<ArrayRef, Error> {
    match (input.data_type(), to) {
        (DataType::Null, _) => {
            // TODO: https://github.com/apache/arrow-rs/issues/684 is fixed,
            // use existing arrow cast kernels.
            Ok(make_null_array(to, input.len()))
        }
        (DataType::Interval(interval_unit), to_type) => match interval_unit {
            arrow_schema::IntervalUnit::DayTime => {
                let input: &IntervalDayTimeArray = downcast_primitive_array(input.as_ref())
                    .into_report()
                    .change_context(Error::ExprEvaluation)?;
                let input: Int32Array = input.unary(|n| i64_to_two_i32(n).0);
                let input: ArrayRef = Arc::new(input);

                // Int32 can cast to all supported int, uint, and float types
                arrow_cast::cast(&input, to_type)
                    .into_report()
                    .change_context(Error::ExprEvaluation)
            }
            arrow_schema::IntervalUnit::YearMonth => {
                let input: &IntervalYearMonthArray = downcast_primitive_array(input.as_ref())
                    .into_report()
                    .change_context(Error::ExprEvaluation)?;
                let result: Int32Array = input.unary(|n| n);
                Ok(Arc::new(result))
            }
            arrow_schema::IntervalUnit::MonthDayNano => {
                error_stack::bail!(Error::Unsupported(Cow::Borrowed(
                    "casting from MonthDayNano intervals not yet supported"
                )))
            }
        },
        _ => arrow_cast::cast(input, to)
            .into_report()
            .change_context(Error::ExprEvaluation),
    }
}
