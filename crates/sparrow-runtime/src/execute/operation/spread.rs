use std::marker::PhantomData;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{
    new_null_array, Array, ArrayData, ArrayRef, BooleanArray, BooleanBufferBuilder,
    GenericStringArray, GenericStringBuilder, Int32Builder, OffsetSizeTrait, PrimitiveArray,
    PrimitiveBuilder, StructArray, UInt32Array, UInt32Builder,
};
use arrow::datatypes::{self, ArrowPrimitiveType, DataType, Fields};
use bitvec::vec::BitVec;
use itertools::{izip, Itertools};
use sparrow_arrow::downcast::{
    downcast_boolean_array, downcast_primitive_array, downcast_string_array, downcast_struct_array,
};
use sparrow_arrow::utils::make_null_array;
use sparrow_instructions::GroupingIndices;

use crate::execute::operation;

#[repr(transparent)]
#[derive(Debug)]
pub struct Spread {
    spread_impl: Box<dyn SpreadImpl>,
}

impl serde::Serialize for Spread {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let e = self.spread_impl.to_serialized_spread();
        e.serialize(serializer)
    }
}

impl<'de> serde::Deserialize<'de> for Spread {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let e = SerializedSpread::deserialize(deserializer)?;
        let Some(spread_impl) = e.into_spread_impl() else {
            use serde::de::Error;
            return Err(D::Error::custom("expected owned"));
        };

        Ok(Self { spread_impl })
    }
}

// A borrowed-or-owned `T`.
#[derive(Debug)]
enum Boo<'a, T> {
    Borrowed(&'a T),
    Owned(T),
}

impl<'a, T: serde::Serialize> serde::Serialize for Boo<'a, T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Boo::Borrowed(t) => (*t).serialize(serializer),
            Boo::Owned(t) => t.serialize(serializer),
        }
    }
}

impl<'a, 'de, T: serde::Deserialize<'de>> serde::Deserialize<'de> for Boo<'a, T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        T::deserialize(deserializer).map(Self::Owned)
    }
}

trait ToSerializedSpread {
    fn to_serialized_spread(&self) -> SerializedSpread<'_>;
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
enum SerializedSpread<'a> {
    Null(Boo<'a, NullSpread>),
    LatchedBoolean(Boo<'a, LatchedBooleanSpread>),
    UnlatchedBoolean(Boo<'a, UnlatchedBooleanSpread>),
    LatchedInt8(Boo<'a, LatchedPrimitiveSpread<datatypes::Int8Type>>),
    UnlatchedInt8(Boo<'a, UnlatchedPrimitiveSpread<datatypes::Int8Type>>),
    LatchedInt16(Boo<'a, LatchedPrimitiveSpread<datatypes::Int16Type>>),
    UnlatchedInt16(Boo<'a, UnlatchedPrimitiveSpread<datatypes::Int16Type>>),
    LatchedInt32(Boo<'a, LatchedPrimitiveSpread<datatypes::Int32Type>>),
    UnlatchedInt32(Boo<'a, UnlatchedPrimitiveSpread<datatypes::Int32Type>>),
    LatchedInt64(Boo<'a, LatchedPrimitiveSpread<datatypes::Int64Type>>),
    UnlatchedInt64(Boo<'a, UnlatchedPrimitiveSpread<datatypes::Int64Type>>),
    LatchedUInt8(Boo<'a, LatchedPrimitiveSpread<datatypes::UInt8Type>>),
    UnlatchedUInt8(Boo<'a, UnlatchedPrimitiveSpread<datatypes::UInt8Type>>),
    LatchedUInt16(Boo<'a, LatchedPrimitiveSpread<datatypes::UInt16Type>>),
    UnlatchedUInt16(Boo<'a, UnlatchedPrimitiveSpread<datatypes::UInt16Type>>),
    LatchedUInt32(Boo<'a, LatchedPrimitiveSpread<datatypes::UInt32Type>>),
    UnlatchedUInt32(Boo<'a, UnlatchedPrimitiveSpread<datatypes::UInt32Type>>),
    LatchedUInt64(Boo<'a, LatchedPrimitiveSpread<datatypes::UInt64Type>>),
    UnlatchedUInt64(Boo<'a, UnlatchedPrimitiveSpread<datatypes::UInt64Type>>),
    LatchedFloat16(Boo<'a, LatchedPrimitiveSpread<datatypes::Float16Type>>),
    UnlatchedFloat16(Boo<'a, UnlatchedPrimitiveSpread<datatypes::Float16Type>>),
    LatchedFloat32(Boo<'a, LatchedPrimitiveSpread<datatypes::Float32Type>>),
    UnlatchedFloat32(Boo<'a, UnlatchedPrimitiveSpread<datatypes::Float32Type>>),
    LatchedFloat64(Boo<'a, LatchedPrimitiveSpread<datatypes::Float64Type>>),
    UnlatchedFloat64(Boo<'a, UnlatchedPrimitiveSpread<datatypes::Float64Type>>),
    LatchedTimestampMicrosecond(
        Boo<'a, LatchedPrimitiveSpread<datatypes::TimestampMicrosecondType>>,
    ),
    UnlatchedTimestampMicrosecond(
        Boo<'a, UnlatchedPrimitiveSpread<datatypes::TimestampMicrosecondType>>,
    ),
    LatchedTimestampMillisecond(
        Boo<'a, LatchedPrimitiveSpread<datatypes::TimestampMillisecondType>>,
    ),
    UnlatchedTimestampMillisecond(
        Boo<'a, UnlatchedPrimitiveSpread<datatypes::TimestampMillisecondType>>,
    ),
    LatchedTimestampNanosecond(Boo<'a, LatchedPrimitiveSpread<datatypes::TimestampNanosecondType>>),
    UnlatchedTimestampNanosecond(
        Boo<'a, UnlatchedPrimitiveSpread<datatypes::TimestampNanosecondType>>,
    ),
    LatchedTimestampSecond(Boo<'a, LatchedPrimitiveSpread<datatypes::TimestampSecondType>>),
    UnlatchedTimestampSecond(Boo<'a, UnlatchedPrimitiveSpread<datatypes::TimestampSecondType>>),
    LatchedDate32(Boo<'a, LatchedPrimitiveSpread<datatypes::Date32Type>>),
    UnlatchedDate32(Boo<'a, UnlatchedPrimitiveSpread<datatypes::Date32Type>>),
    LatchedDate64(Boo<'a, LatchedPrimitiveSpread<datatypes::Date64Type>>),
    UnlatchedDate64(Boo<'a, UnlatchedPrimitiveSpread<datatypes::Date64Type>>),
    LatchedTime32Second(Boo<'a, LatchedPrimitiveSpread<datatypes::Time32SecondType>>),
    UnlatchedTime32Second(Boo<'a, UnlatchedPrimitiveSpread<datatypes::Time32SecondType>>),
    LatchedTime32Millisecond(Boo<'a, LatchedPrimitiveSpread<datatypes::Time32MillisecondType>>),
    UnlatchedTime32Millisecond(Boo<'a, UnlatchedPrimitiveSpread<datatypes::Time32MillisecondType>>),
    LatchedTime64Microsecond(Boo<'a, LatchedPrimitiveSpread<datatypes::Time64MicrosecondType>>),
    UnlatchedTime64Microsecond(Boo<'a, UnlatchedPrimitiveSpread<datatypes::Time64MicrosecondType>>),
    LatchedTime64Nanosecond(Boo<'a, LatchedPrimitiveSpread<datatypes::Time64NanosecondType>>),
    UnlatchedTime64Nanosecond(Boo<'a, UnlatchedPrimitiveSpread<datatypes::Time64NanosecondType>>),
    LatchedDurationMicrosecond(Boo<'a, LatchedPrimitiveSpread<datatypes::DurationMicrosecondType>>),
    UnlatchedDurationMicrosecond(
        Boo<'a, UnlatchedPrimitiveSpread<datatypes::DurationMicrosecondType>>,
    ),
    LatchedDurationMillisecond(Boo<'a, LatchedPrimitiveSpread<datatypes::DurationMillisecondType>>),
    UnlatchedDurationMillisecond(
        Boo<'a, UnlatchedPrimitiveSpread<datatypes::DurationMillisecondType>>,
    ),
    LatchedDurationNanosecond(Boo<'a, LatchedPrimitiveSpread<datatypes::DurationNanosecondType>>),
    UnlatchedDurationNanosecond(
        Boo<'a, UnlatchedPrimitiveSpread<datatypes::DurationNanosecondType>>,
    ),
    LatchedDurationSecond(Boo<'a, LatchedPrimitiveSpread<datatypes::DurationSecondType>>),
    UnlatchedDurationSecond(Boo<'a, UnlatchedPrimitiveSpread<datatypes::DurationSecondType>>),
    LatchedIntervalDayTime(Boo<'a, LatchedPrimitiveSpread<datatypes::IntervalDayTimeType>>),
    UnlatchedIntervalDayTime(Boo<'a, UnlatchedPrimitiveSpread<datatypes::IntervalDayTimeType>>),
    LatchedIntervalMonthDayNano(
        Boo<'a, LatchedPrimitiveSpread<datatypes::IntervalMonthDayNanoType>>,
    ),
    UnlatchedIntervalMonthDayNano(
        Boo<'a, UnlatchedPrimitiveSpread<datatypes::IntervalMonthDayNanoType>>,
    ),
    LatchedIntervalYearMonth(Boo<'a, LatchedPrimitiveSpread<datatypes::IntervalYearMonthType>>),
    UnlatchedIntervalYearMonth(Boo<'a, UnlatchedPrimitiveSpread<datatypes::IntervalYearMonthType>>),
    LatchedString(Boo<'a, LatchedStringSpread<i32>>),
    UnlatchedString(Boo<'a, UnlatchedStringSpread<i32>>),
    LatchedLargeString(Boo<'a, LatchedStringSpread<i64>>),
    UnlatchedLargeString(Boo<'a, UnlatchedStringSpread<i64>>),
    LatchedStruct(Boo<'a, StructSpread<LatchedStructSpreadState>>),
    UnlatchedStruct(Boo<'a, StructSpread<UnlatchedStructSpreadState>>),
    LatchedFallback(Boo<'a, LatchedFallbackSpread>),
    UnlatchedFallback(Boo<'a, UnlatchedFallbackSpread>),
}

fn into_spread_impl<T: SpreadImpl + 'static>(spread: Boo<'_, T>) -> Option<Box<dyn SpreadImpl>> {
    match spread {
        Boo::Borrowed(_) => None,
        Boo::Owned(spread) => Some(Box::new(spread)),
    }
}

impl<'a> SerializedSpread<'a> {
    fn into_spread_impl(self) -> Option<Box<dyn SpreadImpl>> {
        match self {
            SerializedSpread::Null(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedBoolean(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedBoolean(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedInt8(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedInt8(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedInt16(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedInt16(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedInt32(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedInt32(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedInt64(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedInt64(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedUInt8(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedUInt8(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedUInt16(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedUInt16(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedUInt32(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedUInt32(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedUInt64(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedUInt64(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedFloat16(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedFloat16(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedFloat32(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedFloat32(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedFloat64(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedFloat64(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedTimestampMicrosecond(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedTimestampMicrosecond(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedTimestampMillisecond(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedTimestampMillisecond(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedTimestampNanosecond(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedTimestampNanosecond(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedTimestampSecond(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedTimestampSecond(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedDate32(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedDate32(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedDate64(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedDate64(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedTime32Second(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedTime32Second(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedTime32Millisecond(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedTime32Millisecond(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedTime64Microsecond(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedTime64Microsecond(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedTime64Nanosecond(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedTime64Nanosecond(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedDurationMicrosecond(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedDurationMicrosecond(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedDurationMillisecond(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedDurationMillisecond(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedDurationNanosecond(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedDurationNanosecond(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedDurationSecond(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedDurationSecond(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedIntervalDayTime(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedIntervalDayTime(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedIntervalMonthDayNano(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedIntervalMonthDayNano(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedIntervalYearMonth(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedIntervalYearMonth(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedString(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedString(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedLargeString(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedLargeString(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedStruct(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedStruct(spread) => into_spread_impl(spread),
            SerializedSpread::LatchedFallback(spread) => into_spread_impl(spread),
            SerializedSpread::UnlatchedFallback(spread) => into_spread_impl(spread),
        }
    }
}

impl Spread {
    /// Return the implementation of `spread` for the given data type.
    ///
    /// If `latched` is true, the implementation will use a "latched" version of
    /// the spread logic, which will remember the previously output value
    /// for each key. This ensures that continuity is maintained.
    pub(super) fn try_new(latched: bool, data_type: &DataType) -> anyhow::Result<Spread> {
        use datatypes::*;

        let spread_impl: Box<dyn SpreadImpl> = match data_type {
            DataType::Null => Box::new(NullSpread),
            DataType::Boolean => {
                if latched {
                    Box::new(LatchedBooleanSpread::new())
                } else {
                    Box::new(UnlatchedBooleanSpread)
                }
            }
            DataType::Int8 => create_primitive::<Int8Type>(latched),
            DataType::Int16 => create_primitive::<Int16Type>(latched),
            DataType::Int32 => create_primitive::<Int32Type>(latched),
            DataType::Int64 => create_primitive::<Int64Type>(latched),
            DataType::UInt8 => create_primitive::<UInt8Type>(latched),
            DataType::UInt16 => create_primitive::<UInt16Type>(latched),
            DataType::UInt32 => create_primitive::<UInt32Type>(latched),
            DataType::UInt64 => create_primitive::<UInt64Type>(latched),
            DataType::Float16 => create_primitive::<Float16Type>(latched),
            DataType::Float32 => create_primitive::<Float32Type>(latched),
            DataType::Float64 => create_primitive::<Float64Type>(latched),
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                create_primitive::<TimestampMicrosecondType>(latched)
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                create_primitive::<TimestampMillisecondType>(latched)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, _) => {
                create_primitive::<TimestampNanosecondType>(latched)
            }
            DataType::Timestamp(TimeUnit::Second, _) => {
                create_primitive::<TimestampSecondType>(latched)
            }
            DataType::Date32 => create_primitive::<Date32Type>(latched),
            DataType::Date64 => create_primitive::<Date64Type>(latched),
            DataType::Time32(TimeUnit::Second) => create_primitive::<Time32SecondType>(latched),
            DataType::Time32(TimeUnit::Millisecond) => {
                create_primitive::<Time32MillisecondType>(latched)
            }
            DataType::Time64(TimeUnit::Microsecond) => {
                create_primitive::<Time64MicrosecondType>(latched)
            }
            DataType::Time64(TimeUnit::Nanosecond) => {
                create_primitive::<Time64NanosecondType>(latched)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                create_primitive::<DurationMicrosecondType>(latched)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                create_primitive::<DurationMillisecondType>(latched)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                create_primitive::<DurationNanosecondType>(latched)
            }
            DataType::Duration(TimeUnit::Second) => create_primitive::<DurationSecondType>(latched),
            DataType::Interval(IntervalUnit::DayTime) => {
                create_primitive::<IntervalDayTimeType>(latched)
            }
            DataType::Interval(IntervalUnit::MonthDayNano) => {
                create_primitive::<IntervalMonthDayNanoType>(latched)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                create_primitive::<IntervalYearMonthType>(latched)
            }
            DataType::Utf8 => {
                if latched {
                    Box::<operation::spread::LatchedStringSpread<i32>>::default()
                } else {
                    Box::<operation::spread::UnlatchedStringSpread<i32>>::default()
                }
            }
            DataType::LargeUtf8 => {
                if latched {
                    Box::<operation::spread::LatchedStringSpread<i64>>::default()
                } else {
                    Box::<operation::spread::UnlatchedStringSpread<i64>>::default()
                }
            }
            DataType::Struct(fields) => {
                if latched {
                    Box::new(StructSpread::try_new_latched(fields)?)
                } else {
                    Box::new(StructSpread::try_new_unlatched(fields)?)
                }
            }
            fallback => {
                if latched {
                    Box::new(LatchedFallbackSpread::new(fallback))
                } else {
                    Box::new(UnlatchedFallbackSpread)
                }
            }
        };

        Ok(Self { spread_impl })
    }

    /// Spread the values out according to the given signal.
    ///
    /// The result will have the same length as `signal`. For each row,
    /// if `signal` is true then the next value from `values` is taken.
    /// The length of `values` should correspond to the number of `true`
    /// bits in `signal`.
    ///
    /// An "unlatched" spread operation is `null` when the `signal` is not
    /// `true` and the next value (`null` or otherwise) when the `signal`
    /// is `true`.
    ///
    /// A "latched" spread operation repeats the most recent signaled value for
    /// each key "as of" each output row. This is implemented by "latching" the
    /// signaled value.
    ///
    /// Parameters:
    /// * `values`: The actual values.
    /// * `grouping`: The group key for each output row. Used to determine the
    ///   "latched" value, if necessary.
    /// * `signal`: The signal indicating whether a value should be taken.
    ///
    /// `grouping` and `signal` should have the same length. The length of
    /// `values` should correspond to the number of `true` bits in `signal`.
    ///
    /// The result should have the same length as `grouping` (and `signal`).
    pub fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        self.spread_impl.spread_signaled(grouping, values, signal)
    }

    /// Special case of spread when `signal` is all `true`.
    pub fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        self.spread_impl.spread_true(grouping, values)
    }

    /// Special case of `spread` when `signal` is all `false`.
    pub fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        self.spread_impl.spread_false(grouping, value_type)
    }
}

trait SpreadImpl: Send + erased_serde::Serialize + ToSerializedSpread + std::fmt::Debug {
    /// Spread the values out according to the given signal.
    ///
    /// The result will have the same length as `signal`. For each row,
    /// if `signal` is true then the next value from `values` is taken.
    /// The length of `values` should correspond to the number of `true`
    /// bits in `signal`.
    ///
    /// An "unlatched" spread operation is `null` when the `signal` is not
    /// `true` and the next value (`null` or otherwise) when the `signal`
    /// is `true`.
    ///
    /// A "latched" spread operation repeats the most recent signaled value for
    /// each key "as of" each output row. This is implemented by "latching" the
    /// signaled value.
    ///
    /// Parameters:
    /// * `values`: The actual values.
    /// * `grouping`: The group key for each output row. Used to determine the
    ///   "latched" value, if necessary.
    /// * `signal`: The signal indicating whether a value should be taken.
    ///
    /// `grouping` and `signal` should have the same length. The length of
    /// `values` should correspond to the number of `true` bits in `signal`.
    ///
    /// The result should have the same length as `grouping` (and `signal`).
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef>;

    /// Special case of spread when `signal` is all `true`.
    fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef>;

    /// Special case of `spread` when `signal` is all `false`.
    fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        value_type: &DataType,
    ) -> anyhow::Result<ArrayRef>;
}

// Implements `serde` for the SpreadImpl in terms of the erased serialize
// method.
erased_serde::serialize_trait_object!(SpreadImpl);

fn create_primitive<T>(latched: bool) -> Box<dyn SpreadImpl>
where
    T: ArrowPrimitiveType + Send,
    T::Native: serde::Serialize,
    LatchedPrimitiveSpread<T>: ToSerializedSpread,
    UnlatchedPrimitiveSpread<T>: ToSerializedSpread,
{
    if latched {
        Box::new(LatchedPrimitiveSpread::<T>::new())
    } else {
        Box::new(UnlatchedPrimitiveSpread::<T>::new())
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound = "")]
struct UnlatchedPrimitiveSpread<T: ArrowPrimitiveType> {
    // Make the compiler happy by using the type parameter. Conceptually, the aggregation
    // will store values of type T.
    _phantom: PhantomData<T>,
}

impl<T: ArrowPrimitiveType> std::fmt::Debug for UnlatchedPrimitiveSpread<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnlatchedPrimitiveSpread")
            .finish_non_exhaustive()
    }
}

impl<T: ArrowPrimitiveType> UnlatchedPrimitiveSpread<T> {
    fn new() -> Self {
        Self {
            _phantom: PhantomData,
        }
    }
}

/// Create implementations of ToSerializedSpread for a primitive type.
///
/// Ideally, this would only require the name, and could create the identifiers
/// from that. However, that would require `concat_idents!(...)` which is not
/// yet stable. https://doc.rust-lang.org/nightly/std/macro.concat_idents.html
macro_rules! impl_to_serialized_spread_primitives {
    ($unlatched_case:ident, $latched_case:ident, $type:ty) => {
        impl ToSerializedSpread for UnlatchedPrimitiveSpread<$type> {
            fn to_serialized_spread(&self) -> SerializedSpread<'_> {
                SerializedSpread::$unlatched_case(Boo::Borrowed(self))
            }
        }

        impl ToSerializedSpread for LatchedPrimitiveSpread<$type> {
            fn to_serialized_spread(&self) -> SerializedSpread<'_> {
                SerializedSpread::$latched_case(Boo::Borrowed(self))
            }
        }
    };
}

impl_to_serialized_spread_primitives!(UnlatchedInt8, LatchedInt8, datatypes::Int8Type);
impl_to_serialized_spread_primitives!(UnlatchedInt16, LatchedInt16, datatypes::Int16Type);
impl_to_serialized_spread_primitives!(UnlatchedInt32, LatchedInt32, datatypes::Int32Type);
impl_to_serialized_spread_primitives!(UnlatchedInt64, LatchedInt64, datatypes::Int64Type);

impl_to_serialized_spread_primitives!(UnlatchedUInt8, LatchedUInt8, datatypes::UInt8Type);
impl_to_serialized_spread_primitives!(UnlatchedUInt16, LatchedUInt16, datatypes::UInt16Type);
impl_to_serialized_spread_primitives!(UnlatchedUInt32, LatchedUInt32, datatypes::UInt32Type);
impl_to_serialized_spread_primitives!(UnlatchedUInt64, LatchedUInt64, datatypes::UInt64Type);

impl_to_serialized_spread_primitives!(UnlatchedFloat16, LatchedFloat16, datatypes::Float16Type);
impl_to_serialized_spread_primitives!(UnlatchedFloat32, LatchedFloat32, datatypes::Float32Type);
impl_to_serialized_spread_primitives!(UnlatchedFloat64, LatchedFloat64, datatypes::Float64Type);

impl_to_serialized_spread_primitives!(
    UnlatchedTimestampMicrosecond,
    LatchedTimestampMicrosecond,
    datatypes::TimestampMicrosecondType
);
impl_to_serialized_spread_primitives!(
    UnlatchedTimestampMillisecond,
    LatchedTimestampMillisecond,
    datatypes::TimestampMillisecondType
);

impl_to_serialized_spread_primitives!(
    UnlatchedTimestampSecond,
    LatchedTimestampSecond,
    datatypes::TimestampSecondType
);
impl_to_serialized_spread_primitives!(
    UnlatchedTimestampNanosecond,
    LatchedTimestampNanosecond,
    datatypes::TimestampNanosecondType
);
impl_to_serialized_spread_primitives!(UnlatchedDate32, LatchedDate32, datatypes::Date32Type);
impl_to_serialized_spread_primitives!(UnlatchedDate64, LatchedDate64, datatypes::Date64Type);
impl_to_serialized_spread_primitives!(
    UnlatchedTime32Second,
    LatchedTime32Second,
    datatypes::Time32SecondType
);
impl_to_serialized_spread_primitives!(
    UnlatchedTime32Millisecond,
    LatchedTime32Millisecond,
    datatypes::Time32MillisecondType
);
impl_to_serialized_spread_primitives!(
    UnlatchedTime64Microsecond,
    LatchedTime64Microsecond,
    datatypes::Time64MicrosecondType
);
impl_to_serialized_spread_primitives!(
    UnlatchedTime64Nanosecond,
    LatchedTime64Nanosecond,
    datatypes::Time64NanosecondType
);
impl_to_serialized_spread_primitives!(
    UnlatchedDurationMicrosecond,
    LatchedDurationMicrosecond,
    datatypes::DurationMicrosecondType
);
impl_to_serialized_spread_primitives!(
    UnlatchedDurationMillisecond,
    LatchedDurationMillisecond,
    datatypes::DurationMillisecondType
);
impl_to_serialized_spread_primitives!(
    UnlatchedDurationSecond,
    LatchedDurationSecond,
    datatypes::DurationSecondType
);
impl_to_serialized_spread_primitives!(
    UnlatchedDurationNanosecond,
    LatchedDurationNanosecond,
    datatypes::DurationNanosecondType
);
impl_to_serialized_spread_primitives!(
    UnlatchedIntervalDayTime,
    LatchedIntervalDayTime,
    datatypes::IntervalDayTimeType
);
impl_to_serialized_spread_primitives!(
    UnlatchedIntervalMonthDayNano,
    LatchedIntervalMonthDayNano,
    datatypes::IntervalMonthDayNanoType
);
impl_to_serialized_spread_primitives!(
    UnlatchedIntervalYearMonth,
    LatchedIntervalYearMonth,
    datatypes::IntervalYearMonthType
);

impl<T> SpreadImpl for UnlatchedPrimitiveSpread<T>
where
    T: ArrowPrimitiveType + Send,
    UnlatchedPrimitiveSpread<T>: ToSerializedSpread,
{
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        let values: &PrimitiveArray<T> = downcast_primitive_array(values.as_ref())?;
        let mut builder = PrimitiveBuilder::<T>::with_capacity(grouping.len());

        if values.null_count() == 0 {
            let mut value_index = 0;
            let mut last_signal_end_exclusive = 0;
            for (start_inclusive, end_exclusive) in bit_run_iterator(signal) {
                // Pad [last_signal_end_exclusive .. start_inclusive) with nulls.
                // If we start with [1, ...] then the first start inclusive is 0.
                // We'll append 0 nulls.
                // Later, when we end a run at position N and start it at position N + m
                // we'll add `m` nulls.
                builder.append_nulls(start_inclusive - last_signal_end_exclusive);

                let len = end_exclusive - start_inclusive;
                builder.append_slice(&values.values()[value_index..value_index + len]);
                last_signal_end_exclusive = end_exclusive;
                value_index += len;
            }
            builder.append_nulls(signal.len() - last_signal_end_exclusive);
        } else {
            let mut value_index = 0;
            let mut last_signal_end_exclusive = 0;
            for (start_inclusive, end_exclusive) in bit_run_iterator(signal) {
                // Pad [last_signal_end_exclusive .. start_inclusive) with nulls.
                // If we start with [1, ...] then the first start inclusive is 0.
                // We'll append 0 nulls.
                // Later, when we end a run at position N and start it at position N + m
                // we'll add `m` nulls.
                builder.append_nulls(start_inclusive - last_signal_end_exclusive);

                let len = end_exclusive - start_inclusive;
                let is_valid: Vec<_> = (value_index..value_index + len)
                    .map(|index| values.is_valid(index))
                    .collect();
                builder.append_values(&values.values()[value_index..value_index + len], &is_valid);
                last_signal_end_exclusive = end_exclusive;
                value_index += len;
            }
            builder.append_nulls(signal.len() - last_signal_end_exclusive);
        }

        Ok(Arc::new(builder.finish()))
    }

    fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        anyhow::ensure!(grouping.len() == values.len());
        Ok(values.clone())
    }

    fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        _value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        // TODO: It may be faster to call `make_array` directly, but maybe Rust will
        // inline the call to `new_null_array`.
        Ok(new_null_array(&T::DATA_TYPE, grouping.len()))
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(bound(
    serialize = "T::Native: serde::Serialize",
    deserialize = "T::Native: serde::de::Deserialize<'de>"
))]
struct LatchedPrimitiveSpread<T: ArrowPrimitiveType> {
    values: Vec<T::Native>,
    valid: BitVec,
}

impl<T> std::fmt::Debug for LatchedPrimitiveSpread<T>
where
    T: ArrowPrimitiveType,
    T::Native: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LatchedPrimitiveSpread")
            .field("values", &self.values)
            .field("valid", &self.valid)
            .finish()
    }
}

impl<T: ArrowPrimitiveType> LatchedPrimitiveSpread<T> {
    fn new() -> Self {
        Self {
            values: Vec::new(),
            valid: BitVec::new(),
        }
    }
}

impl<T> SpreadImpl for LatchedPrimitiveSpread<T>
where
    T: ArrowPrimitiveType,
    T::Native: serde::Serialize,
    LatchedPrimitiveSpread<T>: ToSerializedSpread,
{
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        debug_assert_eq!(grouping.len(), signal.len());

        // Resize the state if needed.
        if grouping.num_groups() > self.values.len() {
            self.values
                .resize(grouping.num_groups(), T::default_value());
            self.valid.resize(grouping.num_groups(), false);
        }

        let values: &PrimitiveArray<T> = downcast_primitive_array(values.as_ref())?;
        let mut values = values.iter();

        // TODO: Could use "next set bit" operations to more quickly handle
        // signal arrays.
        let result = signal
            .iter()
            .zip(grouping.group_iter())
            .map(|(signal, group)| {
                match signal {
                    Some(true) => {
                        let value = values.next().expect("ran out of values in spread");

                        // SAFETY: Resized to contain groups above.
                        unsafe { self.valid.set_unchecked(group, value.is_some()) };

                        if let Some(value) = value {
                            self.values[group] = value;
                        }
                        value
                    }
                    _ => {
                        // SAFETY: Resized to contain groups above.
                        let is_valid = *unsafe { self.valid.get_unchecked(group) };
                        is_valid.then(|| self.values[group])
                    }
                }
            });

        // SAFETY: Primitive iterators have trusted length.
        let result: PrimitiveArray<T> = unsafe { PrimitiveArray::from_trusted_len_iter(result) };
        let result = Arc::new(result);
        Ok(result)
    }

    fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        anyhow::ensure!(grouping.len() == values.len());

        // Resize the state if needed.
        if grouping.num_groups() > self.values.len() {
            self.values
                .resize(grouping.num_groups(), T::default_value());
            self.valid.resize(grouping.num_groups(), false);
        }

        let values_array: &PrimitiveArray<T> = downcast_primitive_array(values.as_ref())?;

        for (group, value) in grouping.group_iter().zip(values_array.iter()) {
            // SAFETY: Resized to contain groups above.
            unsafe { self.valid.set_unchecked(group, value.is_some()) };

            if let Some(value) = value {
                self.values[group] = value;
            }
        }

        // If the signal is always true, the result is the same as the values.
        // We just needed to run the above code to "capture" any values for
        // future iterations.
        Ok(values.clone())
    }

    fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        _value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        // Resize the state if needed.
        if grouping.num_groups() > self.values.len() {
            self.values
                .resize(grouping.num_groups(), T::default_value());
            self.valid.resize(grouping.num_groups(), false);
        }

        // TODO: Could use "next set bit" operations to more quickly handle
        // signal arrays.
        let result = grouping.group_iter().map(|group| {
            // SAFETY: Resized to contain groups above.
            let is_valid = *unsafe { self.valid.get_unchecked(group) };
            is_valid.then(|| self.values[group])
        });

        // SAFETY: Primitive iterators have trusted length.
        let result: PrimitiveArray<T> = unsafe { PrimitiveArray::from_trusted_len_iter(result) };
        let result = Arc::new(result);

        Ok(result)
    }
}

/// Runs a spread operation on each field in a `StructArray`.
///
/// This works for both stateless and stateful (latched) take, since
/// the necessary state is maintained in the individual spreads.
///
/// TODO: To handle the latched case, we need this to store a bitvec
/// to remember whether the latched record is `null` or not. This state
/// cannot be reconstructed from the individual operations.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct StructSpread<T: StructSpreadState> {
    fields: Fields,
    spreads: Vec<Spread>,
    state: T,
}

impl StructSpread<UnlatchedStructSpreadState> {
    fn try_new_unlatched(fields: &Fields) -> anyhow::Result<Self> {
        let spreads = fields
            .iter()
            .map(|field| Spread::try_new(false, field.data_type()))
            .try_collect()?;

        Ok(Self {
            fields: fields.clone(),
            spreads,
            state: UnlatchedStructSpreadState,
        })
    }
}

impl StructSpread<LatchedStructSpreadState> {
    fn try_new_latched(fields: &Fields) -> anyhow::Result<Self> {
        let spreads = fields
            .iter()
            .map(|field| Spread::try_new(true, field.data_type()))
            .try_collect()?;

        Ok(Self {
            fields: fields.clone(),
            spreads,
            state: LatchedStructSpreadState::default(),
        })
    }
}

impl ToSerializedSpread for StructSpread<UnlatchedStructSpreadState> {
    fn to_serialized_spread(&self) -> SerializedSpread<'_> {
        SerializedSpread::UnlatchedStruct(Boo::Borrowed(self))
    }
}

impl ToSerializedSpread for StructSpread<LatchedStructSpreadState> {
    fn to_serialized_spread(&self) -> SerializedSpread<'_> {
        SerializedSpread::LatchedStruct(Boo::Borrowed(self))
    }
}

impl<T: StructSpreadState> StructSpread<T> {
    fn make_struct(
        &self,
        data_type: DataType,
        length: usize,
        columns: Vec<ArrayRef>,
    ) -> anyhow::Result<StructArray> {
        let child_data = columns.into_iter().map(|a| a.into_data()).collect();
        let array_data = ArrayData::builder(data_type)
            .child_data(child_data)
            .len(length);
        let array_data = array_data.build()?;
        Ok(StructArray::from(array_data))
    }
}

impl<T> SpreadImpl for StructSpread<T>
where
    T: StructSpreadState + serde::Serialize + std::fmt::Debug,
    StructSpread<T>: ToSerializedSpread,
{
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        input_array: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        let input_array = downcast_struct_array(input_array.as_ref())?;

        let data_type = input_array.data_type().clone();
        let columns = izip!(self.spreads.iter_mut(), input_array.columns())
            .map(|(spread, column)| spread.spread_signaled(grouping, column, signal))
            .try_collect()?;
        let output_array = self.make_struct(data_type, grouping.len(), columns)?;

        self.state
            .make_result(grouping, input_array, output_array, signal)
    }

    fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        input_array: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        if T::LATCHED {
            // PERFORMANCE: We handle the "latched" case by calling each of the
            // `spread_true` operations to update the the state of each column.
            // These will clone the respective columns and return them,
            // which will we then discard. Since these are `Arc<dyn Array>`, it should be
            // pretty cheap to clone the arrays.

            let input_array = downcast_struct_array(input_array.as_ref())?;

            for (spread, column) in izip!(self.spreads.iter_mut(), input_array.columns()) {
                spread.spread_true(grouping, column)?;
            }
        }

        let struct_array = downcast_struct_array(input_array.as_ref())?;
        let data_type = input_array.data_type().clone();
        let columns = struct_array
            .columns()
            .iter()
            .map(|a| (*a).clone())
            .collect();
        let output_array = self.make_struct(data_type, grouping.len(), columns)?;

        let signal = BooleanArray::from_iter(std::iter::repeat(Some(true)).take(input_array.len()));
        self.state
            .make_result(grouping, struct_array, output_array, &signal)
    }

    fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        let data_type = value_type.clone();
        let DataType::Struct(fields) = value_type else {
            anyhow::bail!("Expected struct but was {value_type:?}")
        };
        let columns = self
            .spreads
            .iter_mut()
            .zip(fields.iter())
            .map(|(spread, field)| spread.spread_false(grouping, field.data_type()))
            .try_collect()?;
        let output_array = self.make_struct(data_type, grouping.len(), columns)?;
        self.state.make_result_false(grouping, output_array)
    }
}

trait StructSpreadState: Send {
    const LATCHED: bool;

    /// Create the spread result from the given `spread_columns`.
    ///
    /// The `spread_columns` have *already* been spread according to the signal.
    /// This method is only responsible for creating the struct array out of
    /// them. To do this it needs to determine for each row whether the
    /// struct should be null based on (a) the signal and (b) the null-ness
    /// of the struct array.
    fn make_result(
        &mut self,
        grouping: &GroupingIndices,
        input_array: &StructArray,
        output_array: StructArray,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef>;

    /// Equivalent to `make_result` when the `signal` is always false.
    ///
    /// This doesn't use `make_result` directly, because it can be
    /// optimized differently.
    fn make_result_false(
        &mut self,
        grouping: &GroupingIndices,
        output_array: StructArray,
    ) -> anyhow::Result<ArrayRef>;
}

/// Unlatched (discrete) version of struct-spread.
///
/// An output row is null if (a) the signal is false or (b) the input row is
/// null.
#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct UnlatchedStructSpreadState;

impl StructSpreadState for UnlatchedStructSpreadState {
    const LATCHED: bool = false;

    fn make_result(
        &mut self,
        grouping: &GroupingIndices,
        input_array: &StructArray,
        output_array: StructArray,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        debug_assert_eq!(
            output_array.offset(),
            0,
            "For simplicity, the output array should be unsliced"
        );
        debug_assert_eq!(
            output_array.null_count(),
            0,
            "Output array should be created with no null values (eg., by StructArray::from)"
        );

        let null_buffer = if let Some(null_bits) = input_array.nulls() {
            let mut null_bits = null_bits.iter();
            // We need to create a new null buffer which "spreads" the
            // null values of `null_buffer` out based on the `signal`.
            // Specifically, if `signal` is true, we take the next null
            // bit from `null_buffer`. If `signal` is false, we take
            // produce a `null` (false) bit.
            let mut builder = BooleanBufferBuilder::new(grouping.len());

            // TODO: We could iterate over the `signals` in "runs" of contiguous
            // bits. For a sequence of `N false` values, we append `N null` (false)
            // values to the builder. For a sequence of `N true` values, we append
            // a slice of the next `N` bits from the `null_buffer`. This would require
            // more machinery than we currently have, but would allow this to use
            // much faster operations.
            for signal in signal.values().iter() {
                if signal {
                    builder.append(null_bits.next().expect("not enough bits"));
                } else {
                    builder.append(false);
                }
            }
            builder.finish().into_inner()
        } else {
            debug_assert_eq!(signal.null_count(), 0);
            signal.values().inner().clone()
        };

        // Create a new struct array (with the same data) but updating the null buffer.
        let data = output_array
            .into_data()
            .into_builder()
            .null_bit_buffer(Some(null_buffer))
            .build()?;
        Ok(arrow::array::make_array(data))
    }

    fn make_result_false(
        &mut self,
        grouping: &GroupingIndices,
        output_array: StructArray,
    ) -> anyhow::Result<ArrayRef> {
        // For the unlatched false case, we just need to make a "null array".
        // We already have a struct array containing all null columns, so we
        // just need to change the null bits to indicate nothing is valid.
        let mut builder = BooleanBufferBuilder::new(grouping.len());
        builder.append_n(grouping.len(), false);
        let null_buffer = builder.finish().into_inner();

        let data = output_array
            .into_data()
            .into_builder()
            .null_bit_buffer(Some(null_buffer))
            .build()?;
        Ok(arrow::array::make_array(data))
    }
}

/// Latched (continuous, as-of) version of struct-spread.
///
/// The null-ness of the most recent row (inclusive) in which the signal is
/// `true` is remembered and used. Thus, a row is `null` if:
///   a) The `signal` is `true` and the current input is `null`, OR
///   b) The `signal` is `false` and the "remembered" row is `null`.
#[derive(Default, Debug, serde::Serialize, serde::Deserialize)]
struct LatchedStructSpreadState {
    fields: Fields,
    /// For each grouping index, remembers if the most-recently latched
    /// value was null.
    struct_valid: BitVec,
}

impl StructSpreadState for LatchedStructSpreadState {
    const LATCHED: bool = true;

    fn make_result(
        &mut self,
        grouping: &GroupingIndices,
        input_array: &StructArray,
        output_array: StructArray,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        // PERFORMANCE: This currently creates the spread columns without considering
        // whether the struct is null or not. This means that complex values, such as
        // a string, are copied -- even if they will be "masked" by a null struct.
        // We could instead pass the "null bits" down. This would allow the individual
        // spread operations to determine whether a given row was "masked" by an
        // enclosing null.

        // Resize the state if needed.
        if grouping.num_groups() > self.struct_valid.len() {
            self.struct_valid.resize(grouping.num_groups(), false)
        }

        // Check all the lengths.
        debug_assert_eq!(grouping.len(), output_array.len());
        debug_assert_eq!(signal.len(), output_array.len());

        let null_buffer = if let Some(valid_bits) = input_array.nulls() {
            let mut valid_bits = valid_bits.iter();

            // We need to create a new null buffer which "spreads" the
            // null values of `null_buffer` out based on the `signal`.
            // Specifically, if `signal` is true, we take the next null
            // bit from `null_buffer`. If `signal` is false, we return
            // the captured null bit.
            let mut builder = BooleanBufferBuilder::new(grouping.len());

            let signal = signal.values().iter();
            for (signal, group) in signal.zip(grouping.group_iter()) {
                let is_valid = if signal {
                    let is_valid = valid_bits.next().expect("not enough bits");
                    self.struct_valid.set(group, is_valid);
                    is_valid
                } else {
                    self.struct_valid[group]
                };

                builder.append(is_valid);
            }
            builder.finish()
        } else {
            debug_assert_eq!(signal.null_count(), 0);

            let mut builder = BooleanBufferBuilder::new(grouping.len());

            let signal = signal.values().iter();
            for (signal, group) in signal.zip(grouping.group_iter()) {
                let is_valid = if signal {
                    self.struct_valid.set(group, true);
                    true
                } else {
                    self.struct_valid[group]
                };
                builder.append(is_valid);
            }
            builder.finish()
        };
        // Create a new struct array (with the same data) but updating the null buffer.
        let data = output_array
            .into_data()
            .into_builder()
            .null_bit_buffer(Some(null_buffer.into_inner()))
            .build()?;
        Ok(arrow::array::make_array(data))
    }

    fn make_result_false(
        &mut self,
        grouping: &GroupingIndices,
        output_array: StructArray,
    ) -> anyhow::Result<ArrayRef> {
        // Resize the state if needed.
        if grouping.num_groups() > self.struct_valid.len() {
            self.struct_valid.resize(grouping.num_groups(), false)
        }

        // The "false" case means the input array would be all null
        // and the signal is always false.
        debug_assert_eq!(grouping.len(), output_array.len());

        let mut builder = BooleanBufferBuilder::new(grouping.len());
        for group in grouping.group_iter() {
            builder.append(self.struct_valid[group]);
        }
        let null_buffer = builder.finish().into_inner();

        // Create a new struct array (with the same data) but updating the null buffer.
        let data = output_array
            .into_data()
            .into_builder()
            .null_bit_buffer(Some(null_buffer))
            .build()?;
        Ok(arrow::array::make_array(data))
    }
}

#[derive(Default, serde::Serialize, serde::Deserialize, Debug)]
struct UnlatchedStringSpread<O>
where
    O: OffsetSizeTrait,
{
    _phantom: PhantomData<O>,
}

impl ToSerializedSpread for UnlatchedStringSpread<i32> {
    fn to_serialized_spread(&self) -> SerializedSpread<'_> {
        SerializedSpread::UnlatchedString(Boo::Borrowed(self))
    }
}

impl ToSerializedSpread for UnlatchedStringSpread<i64> {
    fn to_serialized_spread(&self) -> SerializedSpread<'_> {
        SerializedSpread::UnlatchedLargeString(Boo::Borrowed(self))
    }
}

impl<O> SpreadImpl for UnlatchedStringSpread<O>
where
    O: OffsetSizeTrait,
    UnlatchedStringSpread<O>: ToSerializedSpread,
{
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        let values: &GenericStringArray<O> = downcast_string_array(values.as_ref())?;
        let mut values = values.iter();

        let mut builder = GenericStringBuilder::<O>::with_capacity(grouping.len(), 1024);
        for signal in signal.iter() {
            match signal {
                Some(true) => builder.append_option(values.next().context("missing value")?),
                _ => builder.append_null(),
            };
        }

        Ok(Arc::new(builder.finish()))
    }

    fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        anyhow::ensure!(grouping.len() == values.len());
        Ok(values.clone())
    }

    fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        Ok(new_null_array(value_type, grouping.len()))
    }
}

#[derive(Default, Debug, serde::Serialize, serde::Deserialize)]
struct LatchedStringSpread<O: OffsetSizeTrait> {
    values: Vec<String>,
    valid: BitVec,
    _phantom: PhantomData<O>,
}

impl ToSerializedSpread for LatchedStringSpread<i32> {
    fn to_serialized_spread(&self) -> SerializedSpread<'_> {
        SerializedSpread::LatchedString(Boo::Borrowed(self))
    }
}

impl ToSerializedSpread for LatchedStringSpread<i64> {
    fn to_serialized_spread(&self) -> SerializedSpread<'_> {
        SerializedSpread::LatchedLargeString(Boo::Borrowed(self))
    }
}

impl<O> SpreadImpl for LatchedStringSpread<O>
where
    O: OffsetSizeTrait,
    LatchedStringSpread<O>: ToSerializedSpread,
{
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        debug_assert_eq!(grouping.len(), signal.len());

        // Resize the state if needed.
        if grouping.num_groups() > self.values.len() {
            self.values.resize_with(grouping.num_groups(), String::new);
            self.valid.resize(grouping.num_groups(), false);
        }

        let values: &GenericStringArray<O> = downcast_string_array(values.as_ref())?;
        let mut values = values.iter();

        let mut builder = GenericStringBuilder::<O>::with_capacity(grouping.len(), 1024);

        // TODO: Could use "next set bit" operations to more quickly handle
        // signal arrays.
        for (signal, group) in signal.iter().zip(grouping.group_iter()) {
            match signal {
                Some(true) => {
                    let value = values.next().context("ran out of values")?;

                    // SAFETY: Resized to contain groups above.
                    unsafe { self.valid.set_unchecked(group, value.is_some()) };

                    if let Some(value) = value {
                        self.values[group] = value.to_owned();
                        builder.append_value(value);
                    } else {
                        builder.append_null();
                    }
                }
                _ => {
                    // SAFETY: Resized to contain groups above.
                    let is_valid = *unsafe { self.valid.get_unchecked(group) };
                    if is_valid {
                        builder.append_value(&self.values[group]);
                    } else {
                        builder.append_null();
                    }
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        debug_assert_eq!(grouping.len(), values.len());

        // Resize the state if needed.
        if grouping.num_groups() > self.values.len() {
            self.values.resize_with(grouping.num_groups(), String::new);
            self.valid.resize(grouping.num_groups(), false);
        }

        let values_array: &GenericStringArray<O> = downcast_string_array(values.as_ref())?;

        for (group, value) in grouping.group_iter().zip(values_array.iter()) {
            // SAFETY: Resized to contain groups above.
            unsafe { self.valid.set_unchecked(group, value.is_some()) };

            if let Some(value) = value {
                self.values[group] = value.to_owned();
            }
        }

        // If the signal is always true, the result is the same as the values.
        // We just needed to run the above code to "capture" any values for
        // future iterations.
        Ok(values.clone())
    }

    fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        _value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        // Resize the state if needed.
        if grouping.num_groups() > self.values.len() {
            self.values.resize_with(grouping.num_groups(), String::new);
            self.valid.resize(grouping.num_groups(), false);
        }

        let mut builder = GenericStringBuilder::<O>::with_capacity(grouping.len(), 1024);

        // TODO: Could use "next set bit" operations to more quickly handle
        // signal arrays.
        for group in grouping.group_iter() {
            // SAFETY: Resized to contain groups above.
            let is_valid = *unsafe { self.valid.get_unchecked(group) };
            if is_valid {
                builder.append_value(&self.values[group]);
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct NullSpread;

impl ToSerializedSpread for NullSpread {
    fn to_serialized_spread(&self) -> SerializedSpread<'_> {
        SerializedSpread::Null(Boo::Borrowed(self))
    }
}

impl SpreadImpl for NullSpread {
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        _signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        Ok(make_null_array(values.data_type(), grouping.len()))
    }

    fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        Ok(make_null_array(values.data_type(), grouping.len()))
    }

    fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        Ok(make_null_array(value_type, grouping.len()))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct UnlatchedBooleanSpread;

impl ToSerializedSpread for UnlatchedBooleanSpread {
    fn to_serialized_spread(&self) -> SerializedSpread<'_> {
        SerializedSpread::UnlatchedBoolean(Boo::Borrowed(self))
    }
}

impl SpreadImpl for UnlatchedBooleanSpread {
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        // PERFORMANCE: There are probably ways to do this using bit masks / operating
        // on bytes. But they are probably reasonably complicated. If we find
        // that (a) we're frequently merging boolean columns, they may be worth
        // exploring.
        anyhow::ensure!(grouping.len() == signal.len());
        anyhow::ensure!(signal.null_count() == 0);

        let mut values_iter = downcast_boolean_array(values.as_ref())?.iter();

        let mut builder = BooleanArray::builder(grouping.len());
        for signal in signal.iter() {
            match signal {
                Some(true) => {
                    // This should exist since `values.len() == signal.count_true()`.
                    let value = values_iter.next().context("next value")?;
                    builder.append_option(value);
                }
                _ => {
                    builder.append_null();
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        anyhow::ensure!(grouping.len() == values.len());
        Ok(values.clone())
    }

    fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        _value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        Ok(make_null_array(&DataType::Boolean, grouping.len()))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct LatchedBooleanSpread {
    valid: BitVec,
    value: BitVec,
}

impl LatchedBooleanSpread {
    fn new() -> Self {
        Self {
            valid: BitVec::new(),
            value: BitVec::new(),
        }
    }
}

impl ToSerializedSpread for LatchedBooleanSpread {
    fn to_serialized_spread(&self) -> SerializedSpread<'_> {
        SerializedSpread::LatchedBoolean(Boo::Borrowed(self))
    }
}

impl SpreadImpl for LatchedBooleanSpread {
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        // PERFORMANCE: There are probably ways to do this using bit masks / operating
        // on bytes. But they are probably reasonably complicated. If we find
        // that (a) we're frequently merging boolean columns, they may be worth
        // exploring.
        anyhow::ensure!(grouping.len() == signal.len());
        anyhow::ensure!(signal.null_count() == 0);

        // Resize the state if needed.
        if grouping.num_groups() > self.value.len() {
            self.value.resize(grouping.num_groups(), false);
            self.valid.resize(grouping.num_groups(), false);
        }

        let mut values_iter = downcast_boolean_array(values.as_ref())?.iter();

        let mut builder = BooleanArray::builder(grouping.len());
        for (group, signal) in grouping.group_iter().zip(signal.iter()) {
            match signal {
                Some(true) => {
                    // This should exist since `values.len() == signal.count_true()`.
                    let value = values_iter.next().context("next value")?;

                    // SAFETY: Resized to contain groups above.
                    unsafe { self.valid.set_unchecked(group, value.is_some()) };

                    if let Some(value) = value {
                        // SAFETY: Resized to contain groups above.
                        unsafe { self.value.set_unchecked(group, value) };
                    }
                    builder.append_option(value);
                }
                _ => {
                    // SAFETY: Resized to contain groups above.
                    let is_valid = *unsafe { self.valid.get_unchecked(group) };
                    if is_valid {
                        builder.append_value(self.value[group]);
                    } else {
                        builder.append_null();
                    }
                }
            }
        }

        Ok(Arc::new(builder.finish()))
    }

    fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        // PERFORMANCE: There are probably ways to do this using bit masks / operating
        // on bytes. But they are probably reasonably complicated. If we find
        // that (a) we're frequently merging boolean columns, they may be worth
        // exploring.
        anyhow::ensure!(grouping.len() == values.len());

        // Resize the state if needed.
        if grouping.num_groups() > self.value.len() {
            self.value.resize(grouping.num_groups(), false);
            self.valid.resize(grouping.num_groups(), false);
        }

        let values_bool = downcast_boolean_array(values.as_ref())?;
        for (group, value) in grouping.group_iter().zip(values_bool.iter()) {
            // SAFETY: Resized to contain groups above.
            unsafe { self.valid.set_unchecked(group, value.is_some()) };

            if let Some(value) = value {
                // SAFETY: Resized to contain groups above.
                unsafe { self.value.set_unchecked(group, value) };
            }
        }

        Ok(values.clone())
    }

    fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        _value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        // PERFORMANCE: There are probably ways to do this using bit masks / operating
        // on bytes. But they are probably reasonably complicated. If we find
        // that (a) we're frequently merging boolean columns, they may be worth
        // exploring.

        // Resize the state if needed.
        if grouping.num_groups() > self.value.len() {
            self.value.resize(grouping.num_groups(), false);
            self.valid.resize(grouping.num_groups(), false);
        }

        let mut builder = BooleanArray::builder(grouping.len());
        for group in grouping.group_iter() {
            // SAFETY: Resized to contain groups above.
            let is_valid = *unsafe { self.valid.get_unchecked(group) };
            if is_valid {
                builder.append_value(self.value[group]);
            } else {
                builder.append_null();
            }
        }

        Ok(Arc::new(builder.finish()))
    }
}

/// Returns an iterator over contiguous chunks of the boolean array.
///
/// The iterator will return pairs `[inclusive, exclusive)` indicating the
/// boolean array contains true values in the given ranges.
pub(super) fn bit_run_iterator(
    array: &BooleanArray,
) -> arrow::util::bit_iterator::BitSliceIterator<'_> {
    debug_assert_eq!(array.null_count(), 0);
    arrow::util::bit_iterator::BitSliceIterator::new(
        array.values().inner(),
        array.offset(),
        array.len(),
    )
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct UnlatchedFallbackSpread;

impl ToSerializedSpread for UnlatchedFallbackSpread {
    fn to_serialized_spread(&self) -> SerializedSpread<'_> {
        SerializedSpread::UnlatchedFallback(Boo::Borrowed(self))
    }
}

impl SpreadImpl for UnlatchedFallbackSpread {
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        let mut indices = Int32Builder::with_capacity(grouping.len());
        let mut next_index = 0;
        for signal in signal.iter() {
            if signal.unwrap_or(false) {
                indices.append_value(next_index);
                next_index += 1;
            } else {
                indices.append_null();
            }
        }
        let indices = indices.finish();
        arrow::compute::take(values.as_ref(), &indices, None).context("failed to take values")
    }

    fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        anyhow::ensure!(grouping.len() == values.len());
        Ok(values.clone())
    }

    fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        Ok(new_null_array(value_type, grouping.len()))
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
struct LatchedFallbackSpread {
    // The index of the group is used as the index in the data.
    #[serde(with = "sparrow_arrow::serde::array_ref")]
    data: ArrayRef,
}

impl LatchedFallbackSpread {
    fn new(data_type: &DataType) -> Self {
        // TODO: Upgrade to arrow>=45 and this can be `make_empty(data_type)`
        let data = ArrayData::new_empty(data_type);
        let data = arrow::array::make_array(data);

        Self { data }
    }
}

impl ToSerializedSpread for LatchedFallbackSpread {
    fn to_serialized_spread(&self) -> SerializedSpread<'_> {
        SerializedSpread::LatchedFallback(Boo::Borrowed(self))
    }
}

impl SpreadImpl for LatchedFallbackSpread {
    fn spread_signaled(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
        signal: &BooleanArray,
    ) -> anyhow::Result<ArrayRef> {
        debug_assert_eq!(grouping.len(), signal.len());
        anyhow::ensure!(self.data.len() <= grouping.num_groups());

        // TODO: We could do this using a separate null buffer and value buffer.
        // This would allow us to avoid copying the data from this vector to the
        // data buffers for `take`.
        let mut state_take_indices: Vec<Option<u32>> = (0..grouping.num_groups())
            .map(|index| {
                if index < self.data.len() {
                    Some(index as u32)
                } else {
                    None
                }
            })
            .collect();

        let mut indices = UInt32Builder::with_capacity(grouping.len());
        let mut next_index = self.data.len() as u32;
        for (signal, group) in signal.iter().zip(grouping.group_iter()) {
            if signal.unwrap_or(false) {
                indices.append_value(next_index);
                state_take_indices[group] = Some(next_index);
                next_index += 1;
            } else {
                indices.append_option(state_take_indices[group]);
            }
        }
        let indices = indices.finish();

        let state_take_indices = UInt32Array::from(state_take_indices);
        let result = sparrow_arrow::concat_take(&self.data, values, &indices)?;
        self.data = sparrow_arrow::concat_take(&self.data, values, &state_take_indices)?;
        Ok(result)
    }

    fn spread_true(
        &mut self,
        grouping: &GroupingIndices,
        values: &ArrayRef,
    ) -> anyhow::Result<ArrayRef> {
        anyhow::ensure!(grouping.len() == values.len());
        anyhow::ensure!(self.data.len() <= grouping.num_groups());

        // TODO: We could do this using a separate null buffer and value buffer.
        // This would allow us to avoid copying the data from this vector to the
        // data buffers for `take`.
        let mut state_take_indices: Vec<Option<u32>> = (0..grouping.num_groups())
            .map(|index| {
                if index < self.data.len() {
                    Some(index as u32)
                } else {
                    None
                }
            })
            .collect();

        // This will update the new_state_indices to the last value for each group.
        // This will null-out the data if the value is null at that point, so we
        // don't need to hadle that case specially.
        for (index, group) in grouping.group_iter().enumerate() {
            state_take_indices[group] = Some((index + self.data.len()) as u32)
        }
        let state_take_indices = UInt32Array::from(state_take_indices);
        self.data = sparrow_arrow::concat_take(&self.data, values, &state_take_indices)?;

        Ok(values.clone())
    }

    fn spread_false(
        &mut self,
        grouping: &GroupingIndices,
        _value_type: &DataType,
    ) -> anyhow::Result<ArrayRef> {
        arrow::compute::take(self.data.as_ref(), grouping.group_indices(), None)
            .context("failed to take values")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{
        Array, ArrayRef, BooleanArray, Float64Array, Int32Builder, Int64Array, LargeStringArray,
        ListArray, MapBuilder, StringArray, StringBuilder, StructArray, UInt32Array,
    };
    use arrow::datatypes::{DataType, Field, UInt64Type};
    use sparrow_arrow::downcast::{
        downcast_primitive_array, downcast_string_array, downcast_struct_array,
    };
    use sparrow_arrow::utils::make_struct_array_null;
    use sparrow_instructions::GroupingIndices;

    use crate::execute::operation::spread::Spread;

    #[test]
    fn test_bit_run_iterator() {
        // This method is provided by Arrow. We test it clear how it behaves.
        let array = BooleanArray::from(vec![false, true, true, true, false, false, true]);
        itertools::assert_equal(super::bit_run_iterator(&array), vec![(1, 4), (6, 7)]);
    }

    #[test]
    fn test_primitive_i64_unlatched_start_end_included() {
        let nums = Int64Array::from(vec![Some(5), Some(8), None, Some(10)]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0],
            vec![true, false, true, false, true, false, true],
            false,
        );
        let result: &Int64Array = downcast_primitive_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &Int64Array::from(vec![Some(5), None, Some(8), None, None, None, Some(10)])
        );
    }

    #[test]
    fn test_primitive_i64_unlatched_start_end_excluded() {
        let nums = Int64Array::from(vec![Some(5), Some(8), None, Some(10)]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2],
            vec![false, true, false, true, false, true, false, true, false],
            false,
        );
        let result: &Int64Array = downcast_primitive_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &Int64Array::from(vec![
                None,
                Some(5),
                None,
                Some(8),
                None,
                None,
                None,
                Some(10),
                None
            ])
        );
    }

    #[test]
    fn test_primitive_u64_latched() {
        let nums = Int64Array::from(vec![Some(5), Some(8), None, Some(10), None, Some(12)]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 0],
            vec![
                false, true, false, true, false, true, false, true, false, true, false,
            ],
            true,
        );
        let result: &Int64Array = downcast_primitive_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &Int64Array::from(vec![
                None,
                Some(5),
                None,
                Some(8),
                Some(5), // signal false, remember last value for key 1=5
                None,
                Some(8), // signal false, remember last value for key 0=8
                Some(10),
                None,
                None,
                None
            ])
        );
    }

    #[test]
    fn test_primitive_f64_latched() {
        let nums = Float64Array::from(vec![
            Some(5.0),
            Some(8.2),
            None,
            Some(10.3),
            None,
            Some(12.0),
        ]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 0],
            vec![
                false, true, false, true, false, true, false, true, false, true, false,
            ],
            true,
        );
        let result: &Float64Array = downcast_primitive_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &Float64Array::from(vec![
                None,
                Some(5.0),
                None,
                Some(8.2),
                Some(5.0), // signal false, remember last value for key 1=5
                None,
                Some(8.2), // signal false, remember last value for key 0=8
                Some(10.3),
                None,
                None,
                None
            ])
        );
    }

    #[test]
    fn test_primitive_f64_unlatched() {
        let nums = Float64Array::from(vec![
            Some(5.0),
            Some(8.2),
            None,
            Some(10.3),
            None,
            Some(12.0),
        ]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 0],
            vec![
                false, true, false, true, false, true, false, true, false, true, false,
            ],
            false,
        );
        let result: &Float64Array = downcast_primitive_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &Float64Array::from(vec![
                None,
                Some(5.0),
                None,
                Some(8.2),
                None,
                None,
                None,
                Some(10.3),
                None,
                None,
                None
            ])
        );
    }

    #[test]
    fn test_struct_unlatched() {
        let input = make_test_struct_array(vec![
            Some(TestStruct {
                a: Some(5),
                b: Some(8.7),
            }),
            None,
            Some(TestStruct {
                a: None,
                b: Some(42.3),
            }),
        ]);
        let result = run_spread(
            Arc::new(input),
            vec![0, 0, 0, 0, 0, 0],
            vec![true, false, true, false, true, false],
            false,
        );
        let result = from_test_struct_array(result);

        insta::assert_json_snapshot!(result, @r###"
        [
          {
            "a": 5,
            "b": 8.7
          },
          null,
          null,
          null,
          {
            "a": null,
            "b": 42.3
          },
          null
        ]
        "###);
    }

    #[test]
    fn test_struct_latched_same_grouping() {
        let input = make_test_struct_array(vec![
            Some(TestStruct {
                a: Some(5),
                b: Some(8.7),
            }),
            None,
            Some(TestStruct {
                a: None,
                b: Some(42.3),
            }),
        ]);
        let result = run_spread(
            Arc::new(input),
            vec![0, 0, 0, 0, 0, 0],
            vec![true, false, true, false, true, false],
            true,
        );
        let result = from_test_struct_array(result);

        insta::assert_json_snapshot!(result, @r###"
        [
          {
            "a": 5,
            "b": 8.7
          },
          {
            "a": 5,
            "b": 8.7
          },
          null,
          null,
          {
            "a": null,
            "b": 42.3
          },
          {
            "a": null,
            "b": 42.3
          }
        ]
        "###);
    }

    #[test]
    fn test_string_unlatched_start_end_included() {
        let nums = StringArray::from(vec![Some("5"), Some("8"), None, Some("10")]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0],
            vec![true, false, true, false, true, false, true],
            false,
        );
        let result: &StringArray = downcast_string_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &StringArray::from(vec![
                Some("5"),
                None,
                Some("8"),
                None,
                None,
                None,
                Some("10")
            ])
        );
    }

    #[test]
    fn test_string_unlatched_start_end_excluded() {
        let nums = StringArray::from(vec![Some("5"), Some("8"), None, Some("10")]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2],
            vec![false, true, false, true, false, true, false, true, false],
            false,
        );
        let result: &StringArray = downcast_string_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &StringArray::from(vec![
                None,
                Some("5"),
                None,
                Some("8"),
                None,
                None,
                None,
                Some("10"),
                None
            ])
        );
    }

    #[test]
    fn test_string_latched() {
        let nums = StringArray::from(vec![
            Some("5"),
            Some("8"),
            None,
            Some("10"),
            None,
            Some("12"),
        ]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 0],
            vec![
                false, true, false, true, false, true, false, true, false, true, false,
            ],
            true,
        );
        let result: &StringArray = downcast_string_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &StringArray::from(vec![
                None,
                Some("5"),
                None,
                Some("8"),
                Some("5"), // signal false, remember last value for key 1=5
                None,
                Some("8"), // signal false, remember last value for key 0=8
                Some("10"),
                None,
                None,
                None
            ])
        );
    }

    #[test]
    fn test_large_string_latched() {
        let nums = LargeStringArray::from(vec![
            Some("5"),
            Some("8"),
            None,
            Some("10"),
            None,
            Some("12"),
        ]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 0],
            vec![
                false, true, false, true, false, true, false, true, false, true, false,
            ],
            true,
        );
        let result: &LargeStringArray = downcast_string_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &LargeStringArray::from(vec![
                None,
                Some("5"),
                None,
                Some("8"),
                Some("5"), // signal false, remember last value for key 1=5
                None,
                Some("8"), // signal false, remember last value for key 0=8
                Some("10"),
                None,
                None,
                None
            ])
        );
    }

    #[test]
    fn test_large_string_unlatched() {
        let nums = LargeStringArray::from(vec![
            Some("5"),
            Some("8"),
            None,
            Some("10"),
            None,
            Some("12"),
        ]);
        let result = run_spread(
            Arc::new(nums),
            vec![0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 0],
            vec![
                false, true, false, true, false, true, false, true, false, true, false,
            ],
            false,
        );
        let result: &LargeStringArray = downcast_string_array(result.as_ref()).unwrap();

        assert_eq!(
            result,
            &LargeStringArray::from(vec![
                None,
                Some("5"),
                None,
                Some("8"),
                None,
                None,
                None,
                Some("10"),
                None,
                None,
                None
            ])
        );
    }

    #[test]
    fn test_unlatched_uint64_list_spread() {
        let data = vec![
            Some(vec![]),
            None,
            Some(vec![Some(3), Some(5), Some(19)]),
            Some(vec![Some(6)]),
        ];
        let list_array = ListArray::from_iter_primitive::<UInt64Type, _, _>(data);

        let result = run_spread(
            Arc::new(list_array),
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            vec![true, false, false, true, false, true, false, true],
            false,
        );

        let expected = vec![
            Some(vec![]),
            None,
            None,
            None,
            None,
            Some(vec![Some(3), Some(5), Some(19)]),
            None,
            Some(vec![Some(6)]),
        ];
        let expected = ListArray::from_iter_primitive::<UInt64Type, _, _>(expected);

        let expected: ArrayRef = Arc::new(expected);
        assert_eq!(&result, &expected)
    }

    #[test]
    fn test_latched_uint64_list_spread() {
        let data = vec![
            Some(vec![]),
            None,
            Some(vec![Some(3), Some(5), Some(19)]),
            Some(vec![Some(6)]),
        ];
        let list_array = ListArray::from_iter_primitive::<UInt64Type, _, _>(data);

        let result = run_spread(
            Arc::new(list_array),
            vec![0, 1, 0, 0, 0, 0, 0, 0],
            vec![true, false, false, true, false, true, false, true],
            true,
        );

        let expected = vec![
            Some(vec![]),
            None,
            Some(vec![]),
            None,
            None,
            Some(vec![Some(3), Some(5), Some(19)]),
            Some(vec![Some(3), Some(5), Some(19)]),
            Some(vec![Some(6)]),
        ];
        let expected = ListArray::from_iter_primitive::<UInt64Type, _, _>(expected);

        let expected: ArrayRef = Arc::new(expected);
        assert_eq!(&result, &expected)
    }

    #[test]
    fn test_unlatched_uint64_list_spread_sliced() {
        let data = vec![
            Some(vec![Some(1)]),
            Some(vec![]),
            None,
            Some(vec![Some(3), Some(5), Some(19)]),
            Some(vec![Some(6)]),
        ];
        let list_array = ListArray::from_iter_primitive::<UInt64Type, _, _>(data);
        let list_array = list_array.slice(1, 4);
        let list_array = Arc::new(list_array);

        let result = run_spread(
            list_array,
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            vec![true, false, false, true, false, true, false, true],
            false,
        );

        let expected = vec![
            Some(vec![]),
            None,
            None,
            None,
            None,
            Some(vec![Some(3), Some(5), Some(19)]),
            None,
            Some(vec![Some(6)]),
        ];
        let expected = ListArray::from_iter_primitive::<UInt64Type, _, _>(expected);

        let expected: ArrayRef = Arc::new(expected);
        assert_eq!(&result, &expected)
    }

    #[test]
    fn test_unlatched_map_spread() {
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::with_capacity(8);
        let mut builder = MapBuilder::new(None, string_builder, int_builder);

        builder.keys().append_value("joe");
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.keys().append_value("blogs");
        builder.values().append_value(2);
        builder.keys().append_value("foo");
        builder.values().append_value(4);
        builder.append(true).unwrap();

        builder.append(false).unwrap();

        builder.keys().append_value("joe");
        builder.values().append_value(10);
        builder.keys().append_value("foo");
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.keys().append_value("alice");
        builder.values().append_value(2);
        builder.append(true).unwrap();

        let map_array = builder.finish();

        let result = run_spread(
            Arc::new(map_array),
            vec![0, 1, 2, 3, 4, 5, 6, 7],
            vec![true, false, false, true, false, true, false, true],
            false,
        );

        let string_builder2 = StringBuilder::new();
        let int_builder2 = Int32Builder::with_capacity(8);
        let mut builder = MapBuilder::new(None, string_builder2, int_builder2);
        builder.keys().append_value("joe");
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.append(false).unwrap();
        builder.append(false).unwrap();

        builder.keys().append_value("blogs");
        builder.values().append_value(2);
        builder.keys().append_value("foo");
        builder.values().append_value(4);
        builder.append(true).unwrap();

        builder.append(false).unwrap();
        builder.append(false).unwrap();
        builder.append(false).unwrap();

        builder.keys().append_value("joe");
        builder.values().append_value(10);
        builder.keys().append_value("foo");
        builder.values().append_value(1);
        builder.append(true).unwrap();

        let expected = builder.finish();
        let expected: ArrayRef = Arc::new(expected);
        assert_eq!(&result, &expected)
    }

    fn run_spread(
        values: ArrayRef,
        indices: Vec<u32>,
        signal: Vec<bool>,
        latched: bool,
    ) -> ArrayRef {
        debug_assert_eq!(indices.len(), signal.len());
        let mut spread = Spread::try_new(latched, values.data_type()).unwrap();
        let num_groups = indices.iter().copied().max().unwrap() + 1;
        let grouping = GroupingIndices::new(num_groups as usize, UInt32Array::from(indices));

        let signal = BooleanArray::from(signal);
        let result = spread.spread_signaled(&grouping, &values, &signal).unwrap();

        assert_round_trips(&spread);

        result
    }
    #[derive(serde::Serialize)]
    struct TestStruct {
        a: Option<i64>,
        b: Option<f64>,
    }

    fn make_test_struct_array(values: Vec<Option<TestStruct>>) -> StructArray {
        let a: Int64Array = values
            .iter()
            .map(|v| v.as_ref().and_then(|v| v.a))
            .collect();
        let b: Float64Array = values
            .iter()
            .map(|v| v.as_ref().and_then(|v| v.b))
            .collect();

        let null_buffer = values.iter().map(|v| v.is_some()).collect();
        make_struct_array_null(
            values.len(),
            vec![
                (
                    // TODO(https://github.com/kaskada-ai/kaskada/issues/417): Avoid copying.
                    Arc::new(Field::new("a", DataType::Int64, true)),
                    Arc::new(a),
                ),
                (
                    // TODO(https://github.com/kaskada-ai/kaskada/issues/417): Avoid copying.
                    Arc::new(Field::new("b", DataType::Float64, true)),
                    Arc::new(b),
                ),
            ],
            null_buffer,
        )
    }

    fn from_test_struct_array(array: ArrayRef) -> Vec<Option<TestStruct>> {
        let struct_array = downcast_struct_array(array.as_ref()).unwrap();
        let a: &Int64Array = downcast_primitive_array(struct_array.column(0).as_ref()).unwrap();
        let b: &Float64Array = downcast_primitive_array(struct_array.column(1).as_ref()).unwrap();

        a.iter()
            .zip(b.iter())
            .enumerate()
            .map(|(index, (a, b))| {
                if struct_array.is_null(index) {
                    None
                } else {
                    Some(TestStruct { a, b })
                }
            })
            .collect()
    }

    fn assert_round_trips(spread: &Spread) {
        let serialized = postcard::to_stdvec(spread).unwrap();
        let deserialized: Spread = postcard::from_bytes(&serialized).unwrap();
        let reserialized = postcard::to_stdvec(&deserialized).unwrap();
        assert_eq!(serialized, reserialized)
    }

    #[test]
    fn test_bool_latched_serde() {
        let spread = Spread::try_new(true, &DataType::Boolean).unwrap();
        assert_round_trips(&spread);
    }

    #[test]
    fn test_i64_latched_serde() {
        let spread = Spread::try_new(true, &DataType::Int64).unwrap();
        assert_round_trips(&spread);
    }

    #[test]
    fn test_i64_unlatched_serde() {
        let spread = Spread::try_new(false, &DataType::Int64).unwrap();
        assert_round_trips(&spread);
    }

    #[test]
    fn test_i16_latched_serde() {
        let spread = Spread::try_new(true, &DataType::Int16).unwrap();
        assert_round_trips(&spread);
    }

    #[test]
    fn test_i16_unlatched_serde() {
        let spread = Spread::try_new(false, &DataType::Int16).unwrap();
        assert_round_trips(&spread);
    }

    #[test]
    fn test_u16_latched_serde() {
        let spread = Spread::try_new(true, &DataType::UInt16).unwrap();
        assert_round_trips(&spread);
    }

    #[test]
    fn test_u16_unlatched_serde() {
        let spread = Spread::try_new(false, &DataType::UInt16).unwrap();
        assert_round_trips(&spread);
    }
}
