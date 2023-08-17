use std::sync::Arc;

use crate::ValueRef;
use arrow::array::{
    ArrayRef, Int32Array, IntervalDayTimeArray, IntervalYearMonthArray, TimestampNanosecondArray,
    UInt32Array,
};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Int64Type, IntervalDayTimeType, IntervalUnit,
    IntervalYearMonthType, TimeUnit, TimestampNanosecondType,
};
use arrow::temporal_conversions::timestamp_ns_to_datetime;
use chrono::Datelike;

use crate::evaluators::{Evaluator, RuntimeInfo};
use crate::{EvaluatorFactory, StaticInfo};

/// Evaluator for the `TimeOf` instruction.
pub(super) struct TimeOfEvaluator {}

impl Evaluator for TimeOfEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let times = info.time_column().array_ref()?;
        let result_array = sparrow_kernels::time::time_of(&times)?;
        Ok(result_array)
    }
}

impl EvaluatorFactory for TimeOfEvaluator {
    fn try_new(_info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        Ok(Box::new(Self {}))
    }
}

/// Evaluator for the `Days` instruction.
pub(super) struct DaysEvaluator {
    input: ValueRef,
}

impl Evaluator for DaysEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let days = info.value(&self.input)?.primitive_array::<Int64Type>()?;

        // The cast kernel doesn't support conversion from numbers to intervals.
        // This may go away in Arrow2.
        let intervals: IntervalDayTimeArray = days
            .iter()
            .map(|v| match v {
                None => None,
                Some(days) if days < 0 => None,
                Some(days) if days > (i32::MAX as i64) => None,
                Some(days) => Some(days << 32),
            })
            .collect();

        Ok(Arc::new(intervals))
    }
}

impl EvaluatorFactory for DaysEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `DayOfMonth` instruction.
pub(super) struct DayOfMonthEvaluator {
    input: ValueRef,
}

#[allow(clippy::redundant_closure)]
fn evaluate_time_accessor<O, F>(
    time: &TimestampNanosecondArray,
    f: F,
) -> arrow::array::PrimitiveArray<O>
where
    O: ArrowPrimitiveType,
    F: Fn(chrono::NaiveDateTime) -> O::Native,
{
    time.unary_opt(|t| timestamp_ns_to_datetime(t).map(|t| f(t)))
}

impl Evaluator for DayOfMonthEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let time = info.value(&self.input)?.primitive_array()?;
        let result: UInt32Array = evaluate_time_accessor(time.as_ref(), |t| t.day());

        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for DayOfMonthEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `DayOfMonth0` instruction.
pub(super) struct DayOfMonth0Evaluator {
    input: ValueRef,
}

impl Evaluator for DayOfMonth0Evaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let time = info.value(&self.input)?.primitive_array()?;
        let result: UInt32Array = evaluate_time_accessor(time.as_ref(), |t| t.day0());

        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for DayOfMonth0Evaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `DayOfYear` instruction.
pub(super) struct DayOfYearEvaluator {
    input: ValueRef,
}

impl Evaluator for DayOfYearEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let time = info.value(&self.input)?.primitive_array()?;
        let result: UInt32Array = evaluate_time_accessor(time.as_ref(), |t| t.ordinal());
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for DayOfYearEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `DayOfYear0` instruction.
pub(super) struct DayOfYear0Evaluator {
    input: ValueRef,
}

impl Evaluator for DayOfYear0Evaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let time = info.value(&self.input)?.primitive_array()?;
        let result: UInt32Array = evaluate_time_accessor(time.as_ref(), |t| t.ordinal0());
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for DayOfYear0Evaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `Months` instruction.
pub(super) struct MonthsEvaluator {
    input: ValueRef,
}

impl Evaluator for MonthsEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let months = info.value(&self.input)?.primitive_array::<Int64Type>()?;

        // The cast kernel doesn't support conversion from numbers to intervals.
        // This should be no longer true in Arrow2.
        let intervals: IntervalYearMonthArray = months
            .iter()
            .map(|v| match v {
                None => None,
                Some(months) if months < (i32::MIN as i64) => None,
                Some(months) if months > (i32::MAX as i64) => None,
                Some(months) => Some(months as i32),
            })
            .collect();

        Ok(Arc::new(intervals))
    }
}

impl EvaluatorFactory for MonthsEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `MonthOfYear` instruction.
pub(super) struct MonthOfYearEvaluator {
    input: ValueRef,
}

impl Evaluator for MonthOfYearEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let time = info.value(&self.input)?.primitive_array()?;
        let result: UInt32Array = evaluate_time_accessor(time.as_ref(), |t| t.month());
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for MonthOfYearEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `MonthOfYear0` instruction.
pub(super) struct MonthOfYear0Evaluator {
    input: ValueRef,
}

impl Evaluator for MonthOfYear0Evaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let time = info.value(&self.input)?.primitive_array()?;
        let result: UInt32Array = evaluate_time_accessor(time.as_ref(), |t| t.month0());
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for MonthOfYear0Evaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `Year` instruction.
pub(super) struct YearEvaluator {
    input: ValueRef,
}

impl Evaluator for YearEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let time = info.value(&self.input)?.primitive_array()?;
        let result: Int32Array = evaluate_time_accessor(time.as_ref(), |t| t.year());
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for YearEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `Seconds` instruction.
pub(super) struct SecondsEvaluator {
    input: ValueRef,
}

impl Evaluator for SecondsEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;
        let seconds = arrow::compute::cast(&input, &DataType::Duration(TimeUnit::Second))?;
        Ok(seconds)
    }
}

impl EvaluatorFactory for SecondsEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input }))
    }
}

/// Evaluator for the `AddTime` instruction.
pub(super) struct AddTimeEvaluator {
    delta: ValueRef,
    time: ValueRef,
}

impl Evaluator for AddTimeEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        // TODO: Support other types of timestamp.
        // TODO: Specialize the scalar case (eg., adding a fixed duration).
        let time = info
            .value(&self.time)?
            .primitive_array::<TimestampNanosecondType>()?;

        let delta = info.value(&self.delta)?;
        let result = match delta.data_type() {
            DataType::Interval(IntervalUnit::DayTime) => {
                let interval = delta.primitive_array::<IntervalDayTimeType>()?;
                sparrow_kernels::time::add_time(&time, &interval)?
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                let interval = delta.primitive_array::<IntervalYearMonthType>()?;
                sparrow_kernels::time::add_time(&time, &interval)?
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                let duration = delta.primitive_array::<DurationNanosecondType>()?;
                sparrow_kernels::time::add_time(&time, &duration)?
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                let duration = delta.primitive_array::<DurationMicrosecondType>()?;
                sparrow_kernels::time::add_time(&time, &duration)?
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                let duration = delta.primitive_array::<DurationMillisecondType>()?;
                sparrow_kernels::time::add_time(&time, &duration)?
            }
            DataType::Duration(TimeUnit::Second) => {
                let duration = delta.primitive_array::<DurationSecondType>()?;
                sparrow_kernels::time::add_time(&time, &duration)?
            }
            timedelta_type => {
                anyhow::bail!("Unsupported time delta type {:?}", timedelta_type)
            }
        };

        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for AddTimeEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (delta, time) = info.unpack_arguments()?;
        Ok(Box::new(Self { delta, time }))
    }
}

/// Evaluator for the `SecondsBetween` instruction.
pub(super) struct SecondsBetweenEvaluator {
    time1: ValueRef,
    time2: ValueRef,
}

impl Evaluator for SecondsBetweenEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let t1 = info.value(&self.time1)?.primitive_array()?;
        let t2 = info.value(&self.time2)?.primitive_array()?;
        let result =
            sparrow_kernels::time::time_between::<DurationSecondType>(t1.as_ref(), t2.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for SecondsBetweenEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (time1, time2) = info.unpack_arguments()?;
        Ok(Box::new(Self { time1, time2 }))
    }
}

/// Evaluator for the `DaysBetween` instruction.
pub(super) struct DaysBetweenEvaluator {
    time1: ValueRef,
    time2: ValueRef,
}

impl Evaluator for DaysBetweenEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let t1 = info.value(&self.time1)?.primitive_array()?;
        let t2 = info.value(&self.time2)?.primitive_array()?;
        let result =
            sparrow_kernels::time::time_between::<IntervalDayTimeType>(t1.as_ref(), t2.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for DaysBetweenEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (time1, time2) = info.unpack_arguments()?;
        Ok(Box::new(Self { time1, time2 }))
    }
}

/// Evaluator for the `MonthsBetween` instruction.
pub(super) struct MonthsBetweenEvaluator {
    time1: ValueRef,
    time2: ValueRef,
}

impl Evaluator for MonthsBetweenEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let t1 = info.value(&self.time1)?.primitive_array()?;
        let t2 = info.value(&self.time2)?.primitive_array()?;
        let result =
            sparrow_kernels::time::time_between::<IntervalYearMonthType>(t1.as_ref(), t2.as_ref())?;
        Ok(Arc::new(result))
    }
}

impl EvaluatorFactory for MonthsBetweenEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let (time1, time2) = info.unpack_arguments()?;
        Ok(Box::new(Self { time1, time2 }))
    }
}
