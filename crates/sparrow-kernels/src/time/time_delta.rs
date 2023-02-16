use arrow::array::{PrimitiveArray, TimestampNanosecondArray, TimestampNanosecondBuilder};
use arrow::datatypes::{
    ArrowPrimitiveType, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, IntervalDayTimeType, IntervalYearMonthType,
};
use arrow::temporal_conversions::timestamp_ns_to_datetime;
use chrono::{Datelike, Duration, NaiveDateTime};
use chronoutil::RelativeDuration;
use itertools::izip;

/// Splits an `i64` into two `i32` parts.
/// This is useful for splitting the native representation of IntervalDayTime.
#[inline]
pub fn i64_to_two_i32(v: i64) -> (i32, i32) {
    // The low part
    let low: i32 = (v & 0xFFFFFFFF) as i32;

    // The high
    let high: i32 = (v >> 32) as i32;
    (high, low)
}

pub trait TimeDeltaType: ArrowPrimitiveType {
    fn add_to(time: NaiveDateTime, value: Self::Native) -> NaiveDateTime;
    fn between(start: NaiveDateTime, end: NaiveDateTime) -> Option<Self::Native>;
}

impl TimeDeltaType for IntervalDayTimeType {
    fn add_to(time: NaiveDateTime, value: Self::Native) -> NaiveDateTime {
        // DayTime is represented as a 64 bit value -- 32 bit day and 32 bit
        // milliseconds.
        let (days, milliseconds) = i64_to_two_i32(value);
        let days = RelativeDuration::days(days as i64);
        let milliseconds = Duration::milliseconds(milliseconds as i64);

        time + days + milliseconds
    }

    fn between(start: NaiveDateTime, end: NaiveDateTime) -> Option<Self::Native> {
        let duration = end - start;
        let days_part = duration.num_days() as i32;
        let millis_part = duration.num_milliseconds() as i32;

        let combined = ((days_part as i64) << 32) | ((millis_part as i64) & 0xFFFFFFFF);
        debug_assert_eq!(i64_to_two_i32(combined), (days_part, millis_part));
        Some(combined)
    }
}

impl TimeDeltaType for IntervalYearMonthType {
    fn add_to(time: NaiveDateTime, value: Self::Native) -> NaiveDateTime {
        // YearMonth is represented as a 32 bit value containing the number of months.
        time + RelativeDuration::months(value)
    }

    fn between(start: NaiveDateTime, end: NaiveDateTime) -> Option<Self::Native> {
        let years = end.year() - start.year();
        let months = years * 12 + (end.month0() as i32) - (start.month0() as i32);
        Some(months)
    }
}

impl TimeDeltaType for DurationSecondType {
    fn add_to(time: NaiveDateTime, value: Self::Native) -> NaiveDateTime {
        time + Duration::seconds(value)
    }

    fn between(start: NaiveDateTime, end: NaiveDateTime) -> Option<Self::Native> {
        Some((end - start).num_seconds())
    }
}

impl TimeDeltaType for DurationMillisecondType {
    fn add_to(time: NaiveDateTime, value: Self::Native) -> NaiveDateTime {
        time + Duration::milliseconds(value)
    }

    fn between(start: NaiveDateTime, end: NaiveDateTime) -> Option<Self::Native> {
        Some((end - start).num_milliseconds())
    }
}

impl TimeDeltaType for DurationMicrosecondType {
    fn add_to(time: NaiveDateTime, value: Self::Native) -> NaiveDateTime {
        time + Duration::microseconds(value)
    }

    fn between(start: NaiveDateTime, end: NaiveDateTime) -> Option<Self::Native> {
        (end - start).num_microseconds()
    }
}

impl TimeDeltaType for DurationNanosecondType {
    fn add_to(time: NaiveDateTime, value: Self::Native) -> NaiveDateTime {
        time + Duration::nanoseconds(value)
    }

    fn between(start: NaiveDateTime, end: NaiveDateTime) -> Option<Self::Native> {
        (end - start).num_nanoseconds()
    }
}

/// Add the given time delta to each time in the `time` array.
///
/// # TODO
/// Support other representations of time. See Arrow schema info
/// for some details. We probably want to handle the various timestamp,
/// date time, etc. We'll also need to figure out how to handle timezones,
/// etc.
///
/// <https://github.com/apache/arrow/blob/master/format/Schema.fbs#L217>
pub fn add_time<Delta>(
    time: &TimestampNanosecondArray,
    delta: &PrimitiveArray<Delta>,
) -> anyhow::Result<TimestampNanosecondArray>
where
    Delta: TimeDeltaType,
{
    anyhow::ensure!(
        time.len() == delta.len(),
        "add_time arguments must be same length, got {:?} and {:?}",
        time.len(),
        delta.len()
    );

    // TODO: Iterate over things in chunks?
    let mut result = TimestampNanosecondBuilder::with_capacity(time.len());
    for (time, delta) in izip!(time.iter(), delta.iter()) {
        match (time, delta) {
            (Some(nanoseconds), Some(delta)) => {
                // See function TODO -- this assumes UTC.
                if let Some(naive) = timestamp_ns_to_datetime(nanoseconds) {
                    let naive = Delta::add_to(naive, delta);
                    result.append_value(naive.timestamp_nanos());
                } else {
                    result.append_null();
                }
            }
            _ => {
                result.append_null();
            }
        }
    }

    Ok(result.finish())
}

/// Produces the time between two timestamps as the given type.
pub fn time_between<Delta>(
    start: &TimestampNanosecondArray,
    end: &TimestampNanosecondArray,
) -> anyhow::Result<PrimitiveArray<Delta>>
where
    Delta: TimeDeltaType,
{
    anyhow::ensure!(
        start.len() == end.len(),
        "time_between arguments must be same length, got {:?} and {:?}",
        start.len(),
        end.len()
    );

    // There is no arrow function for this, but it should be moved into Arrow and
    // based on `math_op` (eg., use combine_option_bitmap, etc.)
    let result = start
        .iter()
        .zip(end.iter())
        .map(|(start, end)| match (start, end) {
            (Some(start), Some(end)) => {
                let start = timestamp_ns_to_datetime(start);
                let end = timestamp_ns_to_datetime(end);
                match (start, end) {
                    (Some(start), Some(end)) => Delta::between(start, end),
                    _ => None,
                }
            }
            _ => None,
        })
        .collect();
    Ok(result)
}

#[cfg(test)]
mod tests {
    use arrow::array::{DurationSecondArray, PrimitiveArray, TimestampNanosecondArray};
    use arrow::datatypes::TimestampNanosecondType;

    use super::*;

    #[test]
    fn seconds_between_diff() {
        let t1: PrimitiveArray<TimestampNanosecondType> = TimestampNanosecondArray::from(vec![
            Some(50000000000),
            Some(2000000000),
            Some(0),
            None,
        ]);
        let t2: PrimitiveArray<TimestampNanosecondType> = TimestampNanosecondArray::from(vec![
            Some(10000000000),
            Some(5000000000),
            Some(0),
            None,
        ]);

        let diff = time_between::<DurationSecondType>(&t1, &t2).unwrap();
        assert_eq!(
            &diff,
            &DurationSecondArray::from(vec![Some(-40), Some(3), Some(0), None])
        );
    }
}
