use std::str::FromStr;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use arrow::array::{Array, ArrayRef, BooleanArray, NullArray, PrimitiveArray, StringArray};
use arrow::datatypes::*;
use arrow_array::LargeStringArray;
use decorum::Total;
use itertools::izip;
use num::{One, Signed, Zero};

use crate::downcast::{
    downcast_boolean_array, downcast_primitive_array, downcast_string_array, downcast_struct_array,
};
use crate::utils::make_struct_array;

/// Represents a single value of a given data type.
///
/// This corresponds to a single row of an Arrow array.
#[derive(
    Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum ScalarValue {
    Null,
    Boolean(Option<bool>),
    Int8(Option<i8>),
    Int16(Option<i16>),
    Int32(Option<i32>),
    Int64(Option<i64>),
    UInt8(Option<u8>),
    UInt16(Option<u16>),
    UInt32(Option<u32>),
    UInt64(Option<u64>),
    Float32(Option<Total<f32>>),
    Float64(Option<Total<f64>>),
    Timestamp(Box<ScalarTimestamp>),
    /// A 32-bit date representing the days since the epoch.
    Date32(Option<i32>),
    /// A 64-bit date representing milliseconds since the epoch.
    /// Values should be divisible by 86_400_000.
    Date64(Option<i64>),
    /// A 32-bit time representing the elapsed time since midnight in the unit
    /// of `TimeUnit`.
    Time32(Option<i32>, TimeUnit),
    /// A 64-bit time representing the elapsed time since midnight in the unit
    /// of `TimeUnit`.
    Time64(Option<i64>, TimeUnit),
    /// Measure of elapsed time in either seconds, milliseconds, microseconds or
    /// nanoseconds.
    Duration(Option<i64>, TimeUnit),
    /// Calendar days and milliseconds as a pair.
    IntervalDayTime(Option<(i32, i32)>),
    /// Calendar months.
    IntervalMonths(Option<i32>),
    /// UTF-8 encoded strings with 32 bit offsets.
    Utf8(Option<String>),
    /// Large UTF-8 encoded strings with 64 bit offsets.
    LargeUtf8(Option<String>),
    /// Records.
    Record(Box<ScalarRecord>),
}

impl From<bool> for ScalarValue {
    fn from(b: bool) -> Self {
        ScalarValue::Boolean(Some(b))
    }
}
#[derive(
    Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct ScalarTimestamp {
    value: Option<i64>,
    unit: TimeUnit,
    tz: Option<Arc<str>>,
}

#[derive(
    Clone, Debug, PartialEq, PartialOrd, Eq, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct ScalarRecord {
    value: Option<Vec<ScalarValue>>,
    fields: Fields,
}

// Return a suffix for the given time unit.
pub fn timeunit_suffix(timeunit: &TimeUnit) -> &'static str {
    match timeunit {
        TimeUnit::Second => "s",
        TimeUnit::Millisecond => "ms",
        TimeUnit::Microsecond => "us",
        TimeUnit::Nanosecond => "ns",
    }
}
pub fn timeunit_from_suffix(suffix: &str) -> anyhow::Result<TimeUnit> {
    match suffix {
        "s" => Ok(TimeUnit::Second),
        "ms" => Ok(TimeUnit::Millisecond),
        "us" => Ok(TimeUnit::Microsecond),
        "ns" => Ok(TimeUnit::Nanosecond),
        s => unreachable!("unrecognized time unit suffix {}", s),
    }
}

impl std::fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // TODO: Add type information to null (eg., `null as bool`)?
            null if null.is_null() => write!(f, "null"),
            ScalarValue::Boolean(Some(true)) => write!(f, "true"),
            ScalarValue::Boolean(Some(false)) => write!(f, "false"),
            ScalarValue::Int8(Some(n)) => write!(f, "{n}i8"),
            ScalarValue::Int16(Some(n)) => write!(f, "{n}i16"),
            ScalarValue::Int32(Some(n)) => write!(f, "{n}i32"),
            ScalarValue::Int64(Some(n)) => write!(f, "{n}i64"),
            ScalarValue::UInt8(Some(n)) => write!(f, "{n}u8"),
            ScalarValue::UInt16(Some(n)) => write!(f, "{n}u16"),
            ScalarValue::UInt32(Some(n)) => write!(f, "{n}u32"),
            ScalarValue::UInt64(Some(n)) => write!(f, "{n}u64"),
            ScalarValue::Float32(Some(n)) => write!(f, "{n}f32"),
            ScalarValue::Float64(Some(n)) => write!(f, "{n}f64"),
            ScalarValue::Timestamp(timestamp) => {
                assert!(
                    timestamp.tz.is_none(),
                    "Timestamps with time zones not yet supported"
                );
                let value = timestamp.value.expect("null handled above");
                write!(
                    f,
                    "timestamp_{}:{}",
                    timeunit_suffix(&timestamp.unit),
                    value
                )
            }
            ScalarValue::Date32(Some(n)) => write!(f, "date32:{n}"),
            ScalarValue::Date64(Some(n)) => write!(f, "date64:{n}"),
            ScalarValue::Time32(Some(n), unit) => {
                write!(f, "time32_{}:{}", timeunit_suffix(unit), n)
            }
            ScalarValue::Time64(Some(n), unit) => {
                write!(f, "time64_{}:{}", timeunit_suffix(unit), n)
            }
            ScalarValue::Duration(Some(n), unit) => {
                write!(f, "duration_{}:{}", timeunit_suffix(unit), n)
            }
            ScalarValue::IntervalDayTime(Some((days, ms))) => {
                write!(f, "interval_days:{days},{ms}")
            }
            ScalarValue::IntervalMonths(Some(months)) => write!(f, "interval_months:{months}"),
            ScalarValue::Utf8(Some(str)) => write!(f, "\\\"{str}\\\""),
            ScalarValue::LargeUtf8(Some(str)) => write!(f, "\\\"{str}\\\""),
            unreachable => unreachable!("Unable to format {unreachable:?}"),
        }
    }
}

impl FromStr for ScalarValue {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<ScalarValue> {
        if s == "null" {
            Ok(ScalarValue::Null)
        } else if s == "true" {
            Ok(ScalarValue::Boolean(Some(true)))
        } else if s == "false" {
            Ok(ScalarValue::Boolean(Some(false)))
        } else if let Some(s) = s.strip_suffix("i8") {
            from_str(s, ScalarValue::Int8)
        } else if let Some(s) = s.strip_suffix("i16") {
            from_str(s, ScalarValue::Int16)
        } else if let Some(s) = s.strip_suffix("i32") {
            from_str(s, ScalarValue::Int32)
        } else if let Some(s) = s.strip_suffix("i64") {
            from_str(s, ScalarValue::Int64)
        } else if let Some(s) = s.strip_suffix("u8") {
            from_str(s, ScalarValue::UInt8)
        } else if let Some(s) = s.strip_suffix("u16") {
            from_str(s, ScalarValue::UInt16)
        } else if let Some(s) = s.strip_suffix("u32") {
            from_str(s, ScalarValue::UInt32)
        } else if let Some(s) = s.strip_suffix("u64") {
            from_str(s, ScalarValue::UInt64)
        } else if let Some(s) = s.strip_suffix("f32") {
            from_str(s, ScalarValue::Float32)
        } else if let Some(s) = s.strip_suffix("f64") {
            from_str(s, ScalarValue::Float64)
        } else {
            Err(anyhow!("Unable to parse ScalarValue from string '{}'", s))
        }
    }
}

fn from_str<T>(str: &str, f: impl FnOnce(Option<T>) -> ScalarValue) -> anyhow::Result<ScalarValue>
where
    T: FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    let value = T::from_str(str)
        .with_context(|| format!("Unable to parse ScalarValue from string '{str}'"))?;
    Ok(f(Some(value)))
}

impl ScalarTimestamp {
    pub fn new(value: Option<i64>, unit: TimeUnit, tz: Option<Arc<str>>) -> ScalarTimestamp {
        ScalarTimestamp { value, unit, tz }
    }
    pub fn time_nanos(&self) -> Option<i64> {
        self.value.map(|t| match self.unit {
            TimeUnit::Second => t * 1000 * 1000 * 1000,
            TimeUnit::Millisecond => t * 1000 * 1000,
            TimeUnit::Microsecond => t * 1000,
            TimeUnit::Nanosecond => t,
        })
    }
    pub fn value(&self) -> Option<i64> {
        self.value
    }
    pub fn unit(&self) -> TimeUnit {
        self.unit.clone()
    }
    pub fn tz(&self) -> Option<Arc<str>> {
        self.tz.clone()
    }
}

impl ScalarRecord {
    pub fn new(value: Option<Vec<ScalarValue>>, fields: Fields) -> ScalarRecord {
        ScalarRecord { value, fields }
    }

    pub fn values(&self) -> &Option<Vec<ScalarValue>> {
        &self.value
    }
    pub fn fields(&self) -> &Fields {
        &self.fields
    }
}

impl ScalarValue {
    /// Create a scalar value for timestamp nanoseconds in the given timezone.
    pub fn timestamp_ns(n: i64, tz: Option<Arc<str>>) -> Self {
        Self::Timestamp(Box::new(ScalarTimestamp {
            value: Some(n),
            unit: TimeUnit::Nanosecond,
            tz,
        }))
    }

    /// Create a scalar value for timestamp nanoseconds in the given timezone.
    pub fn timestamp(seconds: i64, nanos: i32, tz: Option<Arc<str>>) -> Self {
        let nanos = chrono::NaiveDateTime::from_timestamp_opt(seconds, nanos as u32)
            .expect("timestamp overflow")
            .timestamp_nanos();

        Self::Timestamp(Box::new(ScalarTimestamp {
            value: Some(nanos),
            unit: TimeUnit::Nanosecond,
            tz,
        }))
    }

    pub fn from_f64(value: f64) -> Self {
        Self::Float64(Some(Total::from(value)))
    }

    /// Create a null scalar value of the given type
    ///
    /// # Errors
    /// If the data type can't be represented as a scalar value.
    pub fn try_new_null(data_type: &DataType) -> anyhow::Result<Self> {
        match data_type {
            DataType::Null => Ok(Self::Null),
            DataType::Boolean => Ok(Self::Boolean(None)),
            DataType::Int8 => Ok(Self::Int8(None)),
            DataType::Int16 => Ok(Self::Int16(None)),
            DataType::Int32 => Ok(Self::Int32(None)),
            DataType::Int64 => Ok(Self::Int64(None)),
            DataType::UInt8 => Ok(Self::UInt8(None)),
            DataType::UInt16 => Ok(Self::UInt16(None)),
            DataType::UInt32 => Ok(Self::UInt32(None)),
            DataType::UInt64 => Ok(Self::UInt64(None)),
            DataType::Float32 => Ok(Self::Float32(None)),
            DataType::Float64 => Ok(Self::Float64(None)),
            DataType::Timestamp(unit, tz) => Ok(Self::Timestamp(Box::new(ScalarTimestamp {
                value: None,
                unit: unit.clone(),
                tz: tz.clone(),
            }))),
            DataType::Date32 => Ok(Self::Date32(None)),
            DataType::Date64 => Ok(Self::Date64(None)),
            DataType::Time32(unit) => Ok(Self::Time32(None, unit.clone())),
            DataType::Time64(unit) => Ok(Self::Time64(None, unit.clone())),
            DataType::Duration(unit) => Ok(Self::Duration(None, unit.clone())),
            DataType::Interval(IntervalUnit::DayTime) => Ok(Self::IntervalDayTime(None)),
            DataType::Interval(IntervalUnit::YearMonth) => Ok(Self::IntervalMonths(None)),
            DataType::Utf8 => Ok(Self::Utf8(None)),
            DataType::LargeUtf8 => Ok(Self::LargeUtf8(None)),
            DataType::Struct(fields) => Ok(Self::Record(Box::new(ScalarRecord {
                value: None,
                fields: fields.clone(),
            }))),
            unsupported => Err(anyhow!(
                "Unsupported data type for scalar value {:?}",
                unsupported
            )),
        }
    }

    /// Return the Arrow DataType describing this scalar value.
    pub fn data_type(&self) -> DataType {
        match self {
            ScalarValue::Null => DataType::Null,
            ScalarValue::Boolean(_) => DataType::Boolean,
            ScalarValue::Int8(_) => DataType::Int8,
            ScalarValue::Int16(_) => DataType::Int16,
            ScalarValue::Int32(_) => DataType::Int32,
            ScalarValue::Int64(_) => DataType::Int64,
            ScalarValue::UInt8(_) => DataType::UInt8,
            ScalarValue::UInt16(_) => DataType::UInt16,
            ScalarValue::UInt32(_) => DataType::UInt32,
            ScalarValue::UInt64(_) => DataType::UInt64,
            ScalarValue::Float32(_) => DataType::Float32,
            ScalarValue::Float64(_) => DataType::Float64,
            ScalarValue::Timestamp(timestamp) => {
                DataType::Timestamp(timestamp.unit.clone(), timestamp.tz.clone())
            }
            ScalarValue::Date32(_) => DataType::Date32,
            ScalarValue::Date64(_) => DataType::Date64,
            ScalarValue::Time32(_, unit) => DataType::Time32(unit.clone()),
            ScalarValue::Time64(_, unit) => DataType::Time64(unit.clone()),
            ScalarValue::Duration(_, unit) => DataType::Duration(unit.clone()),
            ScalarValue::IntervalDayTime(_) => DataType::Interval(IntervalUnit::DayTime),
            ScalarValue::IntervalMonths(_) => DataType::Interval(IntervalUnit::YearMonth),
            ScalarValue::Utf8(_) => DataType::Utf8,
            ScalarValue::LargeUtf8(_) => DataType::LargeUtf8,
            ScalarValue::Record(record) => DataType::Struct(record.fields.clone()),
        }
    }

    /// Create an Arrow array of length `len` containing this scalar value in
    /// each row.
    pub fn to_array(&self, len: usize) -> ArrayRef {
        match self {
            ScalarValue::Null => Arc::new(NullArray::new(len)),
            ScalarValue::Boolean(b) => {
                Arc::new(std::iter::repeat(b).take(len).collect::<BooleanArray>())
            }
            ScalarValue::Int8(n) => fill_primitive::<Int8Type>(len, n),
            ScalarValue::Int16(n) => fill_primitive::<Int16Type>(len, n),
            ScalarValue::Int32(n) => fill_primitive::<Int32Type>(len, n),
            ScalarValue::Int64(n) => fill_primitive::<Int64Type>(len, n),
            ScalarValue::UInt8(n) => fill_primitive::<UInt8Type>(len, n),
            ScalarValue::UInt16(n) => fill_primitive::<UInt16Type>(len, n),
            ScalarValue::UInt32(n) => fill_primitive::<UInt32Type>(len, n),
            ScalarValue::UInt64(n) => fill_primitive::<UInt64Type>(len, n),
            ScalarValue::Float32(n) => {
                fill_primitive::<Float32Type>(len, &n.map(|f| f.into_inner()))
            }
            ScalarValue::Float64(n) => {
                fill_primitive::<Float64Type>(len, &n.map(|f| f.into_inner()))
            }
            ScalarValue::Timestamp(ts) => match ts.unit {
                TimeUnit::Second => fill_primitive::<TimestampSecondType>(len, &ts.value),
                TimeUnit::Millisecond => fill_primitive::<TimestampMillisecondType>(len, &ts.value),
                TimeUnit::Microsecond => fill_primitive::<TimestampMicrosecondType>(len, &ts.value),
                TimeUnit::Nanosecond => fill_primitive::<TimestampNanosecondType>(len, &ts.value),
            },
            ScalarValue::Date32(n) => fill_primitive::<Date32Type>(len, n),
            ScalarValue::Date64(n) => fill_primitive::<Date64Type>(len, n),
            ScalarValue::Time32(n, unit) => match unit {
                TimeUnit::Second => fill_primitive::<Time32SecondType>(len, n),
                TimeUnit::Millisecond => fill_primitive::<Time32MillisecondType>(len, n),
                unit => {
                    unimplemented!("Arrow doesn't support Time32 with unit {:?}", unit)
                }
            },
            ScalarValue::Time64(n, unit) => match unit {
                TimeUnit::Microsecond => fill_primitive::<Time64MicrosecondType>(len, n),
                TimeUnit::Nanosecond => fill_primitive::<Time64NanosecondType>(len, n),
                unit => {
                    unimplemented!("Arrow doesn't support Time64 with unit {:?}", unit)
                }
            },
            ScalarValue::Duration(n, unit) => match unit {
                TimeUnit::Second => fill_primitive::<DurationSecondType>(len, n),
                TimeUnit::Millisecond => fill_primitive::<DurationMillisecondType>(len, n),
                TimeUnit::Microsecond => fill_primitive::<DurationMicrosecondType>(len, n),
                TimeUnit::Nanosecond => fill_primitive::<DurationNanosecondType>(len, n),
            },
            ScalarValue::IntervalDayTime(days_ms) => {
                let value = days_ms.map(|(day, time)| day_time_to_i64(day, time));
                fill_primitive::<IntervalDayTimeType>(len, &value)
            }
            ScalarValue::IntervalMonths(n) => fill_primitive::<IntervalYearMonthType>(len, n),
            ScalarValue::Utf8(s) => {
                let iter = std::iter::repeat(s).take(len);
                Arc::new(iter.cloned().collect::<StringArray>())
            }
            ScalarValue::LargeUtf8(s) => {
                let iter = std::iter::repeat(s).take(len);
                Arc::new(iter.cloned().collect::<LargeStringArray>())
            }
            ScalarValue::Record(record) => {
                let fields: Vec<(FieldRef, ArrayRef)> = if let Some(values) = &record.value {
                    izip!(&record.fields, values)
                        .map(|(field, value)| (field.clone(), value.to_array(len)))
                        .collect()
                } else {
                    record
                        .fields
                        .iter()
                        .map(|field| {
                            (
                                field.clone(),
                                ScalarValue::try_new_null(field.data_type())
                                    .unwrap()
                                    .to_array(len),
                            )
                        })
                        .collect()
                };

                let result = make_struct_array(len, fields);
                Arc::new(result)
            }
        }
    }

    /// Create a singleton (1 row) array containing containing this scalar
    /// value.
    pub fn to_singleton_array(&self) -> ArrayRef {
        self.to_array(1)
    }

    /// Create a scalar value from the only row in the array.
    ///
    /// # Errors
    /// If the array doesn't contain exactly one row, or if the type of the
    /// array can't be converted to a scalar value.
    pub fn from_singleton_array(array: &dyn Array) -> anyhow::Result<Self> {
        anyhow::ensure!(
            array.len() == 1,
            "Expected singleton array, but was {:?}",
            array
        );
        Self::from_array(array, 0)
    }

    /// Create a scalar value from the given row in the array.
    ///
    /// # Errors
    /// If the row is out of bounds for the gvien array or if the type of the
    /// array can't be converted to a scalar value.
    pub fn from_array(array: &dyn Array, row: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(
            row < array.len(),
            "Row must be less than array length ({:?}), but was {:?}",
            array.len(),
            row
        );

        match array.data_type() {
            DataType::Null => Ok(Self::Null),
            DataType::Boolean => {
                if array.is_valid(row) {
                    let array: &BooleanArray = downcast_boolean_array(array)?;
                    Ok(Self::Boolean(Some(array.value(row))))
                } else {
                    Ok(Self::Boolean(None))
                }
            }
            DataType::Int8 => Ok(Self::Int8(from_primitive::<Int8Type>(row, array)?)),
            DataType::Int16 => Ok(Self::Int16(from_primitive::<Int16Type>(row, array)?)),
            DataType::Int32 => Ok(Self::Int32(from_primitive::<Int32Type>(row, array)?)),
            DataType::Int64 => Ok(Self::Int64(from_primitive::<Int64Type>(row, array)?)),
            DataType::UInt8 => Ok(Self::UInt8(from_primitive::<UInt8Type>(row, array)?)),
            DataType::UInt16 => Ok(Self::UInt16(from_primitive::<UInt16Type>(row, array)?)),
            DataType::UInt32 => Ok(Self::UInt32(from_primitive::<UInt32Type>(row, array)?)),
            DataType::UInt64 => Ok(Self::UInt64(from_primitive::<UInt64Type>(row, array)?)),
            DataType::Float32 => Ok(Self::Float32(
                from_primitive::<Float32Type>(row, array)?.map(Total::from_inner),
            )),
            DataType::Float64 => Ok(Self::Float64(
                from_primitive::<Float64Type>(row, array)?.map(Total::from_inner),
            )),
            DataType::Timestamp(unit, tz) => {
                let value = match unit {
                    TimeUnit::Second => from_primitive::<TimestampSecondType>(row, array)?,
                    TimeUnit::Millisecond => {
                        from_primitive::<TimestampMillisecondType>(row, array)?
                    }
                    TimeUnit::Microsecond => {
                        from_primitive::<TimestampMicrosecondType>(row, array)?
                    }
                    TimeUnit::Nanosecond => from_primitive::<TimestampNanosecondType>(row, array)?,
                };
                let timestamp = ScalarTimestamp {
                    value,
                    unit: unit.clone(),
                    tz: tz.clone(),
                };
                Ok(Self::Timestamp(Box::new(timestamp)))
            }
            DataType::Date32 => Ok(Self::Date32(from_primitive::<Date32Type>(row, array)?)),
            DataType::Date64 => Ok(Self::Date64(from_primitive::<Date64Type>(row, array)?)),
            DataType::Time32(unit) => match unit {
                TimeUnit::Second => Ok(Self::Time32(
                    from_primitive::<Time32SecondType>(row, array)?,
                    TimeUnit::Second,
                )),
                TimeUnit::Millisecond => Ok(Self::Time32(
                    from_primitive::<Time32MillisecondType>(row, array)?,
                    TimeUnit::Millisecond,
                )),
                unit => {
                    unimplemented!("Arrow doesn't support Time32 with unit {:?}", unit)
                }
            },
            DataType::Time64(unit) => match unit {
                TimeUnit::Microsecond => Ok(Self::Time64(
                    from_primitive::<Time64MicrosecondType>(row, array)?,
                    TimeUnit::Microsecond,
                )),
                TimeUnit::Nanosecond => Ok(Self::Time64(
                    from_primitive::<Time64NanosecondType>(row, array)?,
                    TimeUnit::Nanosecond,
                )),
                unit => {
                    unimplemented!("Arrow doesn't support Time64 with unit {:?}", unit)
                }
            },
            DataType::Duration(unit) => {
                let value = match unit {
                    TimeUnit::Second => from_primitive::<DurationSecondType>(row, array)?,
                    TimeUnit::Millisecond => from_primitive::<DurationMillisecondType>(row, array)?,
                    TimeUnit::Microsecond => from_primitive::<DurationMicrosecondType>(row, array)?,
                    TimeUnit::Nanosecond => from_primitive::<DurationNanosecondType>(row, array)?,
                };
                Ok(Self::Duration(value, unit.clone()))
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                let value = from_primitive::<IntervalDayTimeType>(row, array)?;
                let value = value.map(|v| ((v >> 32) as i32, v as i32));
                Ok(Self::IntervalDayTime(value))
            }
            DataType::Interval(IntervalUnit::YearMonth) => Ok(Self::IntervalMonths(
                from_primitive::<IntervalYearMonthType>(row, array)?,
            )),
            DataType::Utf8 => {
                if array.is_valid(row) {
                    let array: &StringArray = downcast_string_array(array)?;
                    let string = array.value(row).to_owned();
                    Ok(Self::Utf8(Some(string)))
                } else {
                    Ok(Self::Utf8(None))
                }
            }
            DataType::LargeUtf8 => {
                if array.is_valid(row) {
                    let array: &LargeStringArray = downcast_string_array(array)?;
                    let string = array.value(row).to_owned();
                    Ok(Self::LargeUtf8(Some(string)))
                } else {
                    Ok(Self::LargeUtf8(None))
                }
            }
            DataType::Struct(fields) => {
                let value = if array.is_valid(row) {
                    let array = downcast_struct_array(array)?;
                    let values: Result<Vec<_>, _> = array
                        .columns()
                        .iter()
                        .map(|column| Self::from_array(column.as_ref(), row))
                        .collect();
                    Some(values?)
                } else {
                    None
                };
                let fields = fields.clone();
                Ok(Self::Record(Box::new(ScalarRecord { value, fields })))
            }
            unsupported => Err(anyhow!(
                "Unable to convert value of type {:?} to ScalarValue",
                unsupported
            )),
        }
    }

    pub fn is_null(&self) -> bool {
        match self {
            ScalarValue::Null => true,
            ScalarValue::Boolean(b) => b.is_none(),
            ScalarValue::Int8(n) => n.is_none(),
            ScalarValue::Int16(n) => n.is_none(),
            ScalarValue::Int32(n) => n.is_none(),
            ScalarValue::Int64(n) => n.is_none(),
            ScalarValue::UInt8(n) => n.is_none(),
            ScalarValue::UInt16(n) => n.is_none(),
            ScalarValue::UInt32(n) => n.is_none(),
            ScalarValue::UInt64(n) => n.is_none(),
            ScalarValue::Float32(n) => n.is_none(),
            ScalarValue::Float64(n) => n.is_none(),
            ScalarValue::Timestamp(n) => n.value.is_none(),
            ScalarValue::Date32(n) => n.is_none(),
            ScalarValue::Date64(n) => n.is_none(),
            ScalarValue::Time32(n, _) => n.is_none(),
            ScalarValue::Time64(n, _) => n.is_none(),
            ScalarValue::Duration(n, _) => n.is_none(),
            ScalarValue::IntervalDayTime(n) => n.is_none(),
            ScalarValue::IntervalMonths(n) => n.is_none(),
            ScalarValue::Utf8(n) => n.is_none(),
            ScalarValue::LargeUtf8(n) => n.is_none(),
            ScalarValue::Record(record) => record.value.is_none(),
        }
    }

    /// Return a null value of the same type.
    pub fn null(&self) -> Self {
        match self {
            ScalarValue::Null => ScalarValue::Null,
            ScalarValue::Boolean(_) => ScalarValue::Boolean(None),
            ScalarValue::Int8(_) => ScalarValue::Int8(None),
            ScalarValue::Int16(_) => ScalarValue::Int16(None),
            ScalarValue::Int32(_) => ScalarValue::Int32(None),
            ScalarValue::Int64(_) => ScalarValue::Int64(None),
            ScalarValue::UInt8(_) => ScalarValue::UInt8(None),
            ScalarValue::UInt16(_) => ScalarValue::UInt16(None),
            ScalarValue::UInt32(_) => ScalarValue::UInt32(None),
            ScalarValue::UInt64(_) => ScalarValue::UInt64(None),
            ScalarValue::Float32(_) => ScalarValue::Float32(None),
            ScalarValue::Float64(_) => ScalarValue::Float64(None),
            ScalarValue::Timestamp(timestamp) => {
                ScalarValue::Timestamp(Box::new(ScalarTimestamp {
                    value: None,
                    unit: timestamp.unit.clone(),
                    tz: timestamp.tz.clone(),
                }))
            }
            ScalarValue::Date32(_) => ScalarValue::Date32(None),
            ScalarValue::Date64(_) => ScalarValue::Date64(None),
            ScalarValue::Time32(_, time_unit) => ScalarValue::Time32(None, time_unit.clone()),
            ScalarValue::Time64(_, time_unit) => ScalarValue::Time64(None, time_unit.clone()),
            ScalarValue::Duration(_, time_unit) => ScalarValue::Duration(None, time_unit.clone()),
            ScalarValue::IntervalDayTime(_) => ScalarValue::IntervalDayTime(None),
            ScalarValue::IntervalMonths(_) => ScalarValue::IntervalMonths(None),
            ScalarValue::Utf8(_) => ScalarValue::Utf8(None),
            ScalarValue::LargeUtf8(_) => ScalarValue::LargeUtf8(None),
            ScalarValue::Record(record) => ScalarValue::Record(Box::new(ScalarRecord {
                value: None,
                fields: record.fields.clone(),
            })),
        }
    }

    pub fn is_zero(&self) -> bool {
        match self {
            ScalarValue::Int8(Some(0))
            | ScalarValue::Int16(Some(0))
            | ScalarValue::Int32(Some(0))
            | ScalarValue::Int64(Some(0))
            | ScalarValue::UInt8(Some(0))
            | ScalarValue::UInt16(Some(0))
            | ScalarValue::UInt32(Some(0))
            | ScalarValue::UInt64(Some(0)) => true,
            ScalarValue::Float32(Some(f)) => f.is_zero(),
            ScalarValue::Float64(Some(f)) => f.is_zero(),
            _ => false,
        }
    }

    pub fn is_one(&self) -> bool {
        match self {
            ScalarValue::Int8(Some(1))
            | ScalarValue::Int16(Some(1))
            | ScalarValue::Int32(Some(1))
            | ScalarValue::Int64(Some(1))
            | ScalarValue::UInt8(Some(1))
            | ScalarValue::UInt16(Some(1))
            | ScalarValue::UInt32(Some(1))
            | ScalarValue::UInt64(Some(1)) => true,
            ScalarValue::Float32(Some(f)) => f.is_one(),
            ScalarValue::Float64(Some(f)) => f.is_one(),
            _ => false,
        }
    }

    pub fn is_neg_one(&self) -> bool {
        match self {
            ScalarValue::Int8(Some(-1))
            | ScalarValue::Int16(Some(-1))
            | ScalarValue::Int32(Some(-1))
            | ScalarValue::Int64(Some(-1)) => true,
            ScalarValue::Float32(Some(f)) => f.is_negative() && f.abs().is_one(),
            ScalarValue::Float64(Some(f)) => f.is_negative() && f.abs().is_one(),
            _ => false,
        }
    }

    pub fn is_false(&self) -> bool {
        matches!(self, ScalarValue::Boolean(Some(false)))
    }

    pub fn is_true(&self) -> bool {
        matches!(self, ScalarValue::Boolean(Some(true)))
    }
}

#[inline]
fn fill_primitive<T>(len: usize, value: &Option<T::Native>) -> ArrayRef
where
    T: ArrowPrimitiveType,
{
    let iter = std::iter::repeat(value).take(len);
    // Safety: The iterator is of a fixed size.
    let array = unsafe { PrimitiveArray::<T>::from_trusted_len_iter(iter) };
    Arc::new(array)
}

#[inline]
fn from_primitive<T>(row: usize, array: &dyn Array) -> anyhow::Result<Option<T::Native>>
where
    T: ArrowPrimitiveType,
{
    if array.is_valid(row) {
        Ok(Some(downcast_primitive_array::<T>(array)?.value(row)))
    } else {
        Ok(None)
    }
}

fn day_time_to_i64(days: i32, ms: i32) -> i64 {
    ((days as i64) << 32) | (ms as i64)
}

pub trait NativeFromScalar: ArrowPrimitiveType {
    fn native_from_scalar(scalar: &ScalarValue) -> anyhow::Result<Option<Self::Native>>;
}

impl NativeFromScalar for Float32Type {
    fn native_from_scalar(scalar: &ScalarValue) -> anyhow::Result<Option<Self::Native>> {
        match scalar {
            ScalarValue::Float32(n) => Ok(n.map(Total::into_inner)),
            _ => Err(anyhow!(
                "Unable to convert {:?} to {:?}",
                scalar,
                Self::DATA_TYPE
            )),
        }
    }
}

impl NativeFromScalar for Float64Type {
    fn native_from_scalar(scalar: &ScalarValue) -> anyhow::Result<Option<Self::Native>> {
        match scalar {
            ScalarValue::Float64(n) => Ok(n.map(Total::into_inner)),
            _ => Err(anyhow!(
                "Unable to convert {:?} to {:?}",
                scalar,
                Self::DATA_TYPE
            )),
        }
    }
}

macro_rules! native_from_scalar {
    ($arrow_type:ty, $case:ident) => {
        impl NativeFromScalar for $arrow_type {
            fn native_from_scalar(scalar: &ScalarValue) -> anyhow::Result<Option<Self::Native>> {
                match scalar {
                    ScalarValue::$case(n) => Ok(*n),
                    _ => Err(anyhow::anyhow!(
                        "Unable to convert s{:?} to {:?}",
                        scalar,
                        Self::DATA_TYPE
                    )),
                }
            }
        }
    };
}

native_from_scalar!(Int8Type, Int8);
native_from_scalar!(Int16Type, Int16);
native_from_scalar!(Int32Type, Int32);
native_from_scalar!(Int64Type, Int64);

native_from_scalar!(UInt8Type, UInt8);
native_from_scalar!(UInt16Type, UInt16);
native_from_scalar!(UInt32Type, UInt32);
native_from_scalar!(UInt64Type, UInt64);

native_from_scalar!(Date32Type, Date32);
native_from_scalar!(Date64Type, Date64);

native_from_scalar!(IntervalYearMonthType, IntervalMonths);

macro_rules! native_timestamp {
    ($arrow_type:ty, $time_case:ident, $timeunit:expr) => {
        impl NativeFromScalar for $arrow_type {
            fn native_from_scalar(scalar: &ScalarValue) -> anyhow::Result<Option<Self::Native>> {
                match scalar {
                    ScalarValue::$time_case(timestamp) => {
                        anyhow::ensure!(
                            timestamp.unit == $timeunit,
                            "Unexpected time unit {:?} for {}",
                            timestamp.unit,
                            stringify!($arrow_type)
                        );
                        anyhow::ensure!(
                            timestamp.tz.is_none(),
                            "Unexpected time zone {:?} for {}",
                            timestamp.tz,
                            stringify!($arrow_type)
                        );
                        Ok(timestamp.value)
                    }
                    _ => Err(anyhow::anyhow!(
                        "Unable to convert {:?} to {:?}",
                        scalar,
                        Self::DATA_TYPE
                    )),
                }
            }
        }
    };
}

native_timestamp!(TimestampSecondType, Timestamp, TimeUnit::Second);
native_timestamp!(TimestampMillisecondType, Timestamp, TimeUnit::Millisecond);
native_timestamp!(TimestampMicrosecondType, Timestamp, TimeUnit::Microsecond);
native_timestamp!(TimestampNanosecondType, Timestamp, TimeUnit::Nanosecond);

macro_rules! native_time {
    ($arrow_type:ty, $time_case:ident, $timeunit:expr) => {
        impl NativeFromScalar for $arrow_type {
            fn native_from_scalar(scalar: &ScalarValue) -> anyhow::Result<Option<Self::Native>> {
                match scalar {
                    ScalarValue::$time_case(timestamp, time_unit) => {
                        anyhow::ensure!(
                            time_unit == &$timeunit,
                            "Unexpected time unit {:?} for {}",
                            time_unit,
                            stringify!($arrow_type)
                        );
                        Ok(*timestamp)
                    }
                    _ => Err(anyhow::anyhow!(
                        "Unable to convert {:?} to {:?}",
                        scalar,
                        Self::DATA_TYPE
                    )),
                }
            }
        }
    };
}

native_time!(Time32SecondType, Time32, TimeUnit::Second);
native_time!(Time32MillisecondType, Time32, TimeUnit::Millisecond);
native_time!(Time64MicrosecondType, Time64, TimeUnit::Microsecond);
native_time!(Time64NanosecondType, Time64, TimeUnit::Nanosecond);

native_time!(DurationSecondType, Duration, TimeUnit::Second);
native_time!(DurationMillisecondType, Duration, TimeUnit::Millisecond);
native_time!(DurationMicrosecondType, Duration, TimeUnit::Microsecond);
native_time!(DurationNanosecondType, Duration, TimeUnit::Nanosecond);

impl NativeFromScalar for IntervalDayTimeType {
    fn native_from_scalar(scalar: &ScalarValue) -> anyhow::Result<Option<Self::Native>> {
        match scalar {
            ScalarValue::IntervalDayTime(None) => Ok(None),
            ScalarValue::IntervalDayTime(Some((days, ms))) => Ok(Some(day_time_to_i64(*days, *ms))),
            _ => Err(anyhow!(
                "Unable to convert {:?} to {:?}",
                scalar,
                Self::DATA_TYPE
            )),
        }
    }
}
