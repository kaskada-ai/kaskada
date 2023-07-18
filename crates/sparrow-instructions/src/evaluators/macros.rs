/// Type level function used to pass through the Arrow Type.
///
/// For example, we can use `Identify` for the following
/// ```no_run
/// $aggf == Identify
/// $evaluator::<$aggf<$type>> == $evaluator::<$type>
/// ```
pub(crate) type Identity<T> = T;

/// Create a `Box<dyn Evaluator>` for a number instruction.
///
/// The `$input_type` must be a `&DataType` corresponding to the number input
/// to the instruction.
///
/// The `$evaluator` must be a `struct` that takes a single `T:
/// ArrowPrimitiveType` generic and which implements `Evaluator`.
///
/// The `$info` should be a `StaticInfo`, which contains information relevant
/// to creating a new evaluator.
///
/// # Example
///
/// ```no_run
/// struct AddEvaluator<T: ArrowPrimitiveType>;
/// impl<T: ArrowPrimitiveType> Evaluator for AddEvaluator<T> { ... }
///
/// fn create_evaluator(args: Vec<StaticArg>, result_type: &DataType) -> anyhow::Result<Self> {
///   create_number_evaluator!(&args[0].data_type, AddEvaluator, args, result_type)
/// }
/// ```
macro_rules! create_number_evaluator {
    ($input_type:expr, $evaluator:ident, $info:expr) => {{
        use $crate::evaluators::macros::Identity;
        create_number_evaluator! {$input_type, $evaluator, Identity, $info}
    }};
    ($input_type:expr, $evaluator:ident, $aggf:ident, $info:expr) => {{
        use arrow::datatypes::*;
        match $input_type {
            DataType::Int32 => $evaluator::<$aggf<Int32Type>>::try_new($info),
            DataType::Int64 => $evaluator::<$aggf<Int64Type>>::try_new($info),
            DataType::UInt32 => $evaluator::<$aggf<UInt32Type>>::try_new($info),
            DataType::UInt64 => $evaluator::<$aggf<UInt64Type>>::try_new($info),
            DataType::Float32 => $evaluator::<$aggf<Float32Type>>::try_new($info),
            DataType::Float64 => $evaluator::<$aggf<Float64Type>>::try_new($info),
            unsupported_type => {
                // This macro should only be used on `signed` numeric types.
                Err(anyhow::anyhow!(format!(
                    "Unsupported non-number input type {:?} for {}",
                    unsupported_type,
                    stringify!($evaluator)
                )))
            }
        }
    }};
}

/// Create a `Box<dyn Evaluator>` for a signed instruction.
///
/// The `$input_type` must be a `&DataType` corresponding to the signed input
/// to the instruction.
///
/// The `$evaluator` must be a `struct` that takes a single `T:
/// ArrowPrimitiveType` generic and which implements `Evaluator`.
///
/// The `$info` should be a `StaticInfo`, which contains information relevant
/// to creating a new evaluator.
///
/// # Example
///
/// ```no_run
/// struct NegEvaluator<T: ArrowPrimitiveType>;
/// impl<T: ArrowPrimitiveType> Evaluator for NegEvaluator<T> { ... }
///
/// fn create_evaluator(args: Vec<StaticArg>, result_type: &DataType) -> anyhow::Result<Self> {
///   create_signed_evaluator!(&args[0].data_type, NegEvaluator, args, result_type)
/// }
/// ```
macro_rules! create_signed_evaluator {
    ($input_type:expr, $evaluator:ident, $info:expr) => {{
        use arrow::datatypes::*;
        match $input_type {
            DataType::Int32 => $evaluator::<Int32Type>::try_new($info),
            DataType::Int64 => $evaluator::<Int64Type>::try_new($info),
            DataType::Float32 => $evaluator::<Float32Type>::try_new($info),
            DataType::Float64 => $evaluator::<Float64Type>::try_new($info),
            unsupported_type => {
                // This macro should only be used on `signed` numeric types.
                Err(anyhow::anyhow!(format!(
                    "Unsupported non-signed input type {:?} for {}",
                    unsupported_type,
                    stringify!($evaluator)
                )))
            }
        }
    }};
}

/// Create a `Box<dyn Evaluator>` for a float instructions.
///
/// The `$input_type` must be a `&DataType` corresponding to the float input
/// to the instruction.
///
/// The `$evaluator` must be a `struct` that takes a single `T:
/// ArrowPrimitiveType` generic and which implements `Evaluator`.
///
/// The `$info` should be a `StaticInfo`, which contains information relevant
/// to creating a new evaluator.
///
/// # Example
///
/// ```no_run
/// struct PowfEvaluator<T: ArrowPrimitiveType>;
/// impl<T: ArrowFloatNumericType> Evaluator for PowfEvaluator<T> { ... }
///
/// fn create_evaluator(args: Vec<StaticArg>, result_type: &DataType) -> anyhow::Result<Self> {
///   create_float_evaluator!(&args[0].data_type, PowfEvaluator, args, result_type)
/// }
/// ```
macro_rules! create_float_evaluator {
    ($input_type:expr, $evaluator:ident, $info:expr) => {{
        use arrow::datatypes::*;
        match $input_type {
            DataType::Float32 => $evaluator::<Float32Type>::try_new($info),
            DataType::Float64 => $evaluator::<Float64Type>::try_new($info),
            unsupported_type => {
                // This macro should only be used on `float` numeric types.
                Err(anyhow::anyhow!(format!(
                    "Unsupported non-float input type {:?} for {}",
                    unsupported_type,
                    stringify!($evaluator)
                )))
            }
        }
    }};
}

/// Create a `Box<dyn Evaluator>` for an ordered instruction.
///
/// The `$input_type` must be a `&DataType` corresponding to the ordered input
/// to the instruction.
///
/// The `$evaluator` must be a `struct` that takes a single `T:
/// ArrowPrimitiveType` generic and which implements `Evaluator`.
///
/// The `$info` should be a `StaticInfo`, which contains information relevant
/// to creating a new evaluator.
/// # Example
///
/// ```no_run
/// struct LtEvaluator<T: ArrowPrimitiveType>;
/// impl<T: ArrowNumericType> Evaluator for LtEvaluator<T> { ... }
///
/// fn create_evaluator(args: Vec<StaticArg>, result_type: &DataType) -> anyhow::Result<Self> {
///   create_ordered_evaluator!(&args[0].data_type, LtEvaluator, args, result_type)
/// }
/// ```
macro_rules! create_ordered_evaluator {
    ($input_type:expr, $evaluator:ident, $info:expr) => {{
        use $crate::evaluators::macros::Identity;
        create_ordered_evaluator! {$input_type, $evaluator, Identity, $info}
    }};
    ($input_type:expr, $evaluator:ident, $aggf:ident, $info:expr) => {{
        use arrow::datatypes::*;
        match $input_type {
            DataType::Int32 => $evaluator::<$aggf<Int32Type>>::try_new($info),
            DataType::Int64 => $evaluator::<$aggf<Int64Type>>::try_new($info),
            DataType::UInt32 => $evaluator::<$aggf<UInt32Type>>::try_new($info),
            DataType::UInt64 => $evaluator::<$aggf<UInt64Type>>::try_new($info),
            DataType::Float32 => $evaluator::<$aggf<Float32Type>>::try_new($info),
            DataType::Float64 => $evaluator::<$aggf<Float64Type>>::try_new($info),
            DataType::Timestamp(TimeUnit::Second, None) => {
                $evaluator::<$aggf<TimestampSecondType>>::try_new($info)
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                $evaluator::<$aggf<TimestampMillisecondType>>::try_new($info)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                $evaluator::<$aggf<TimestampMicrosecondType>>::try_new($info)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                $evaluator::<$aggf<TimestampNanosecondType>>::try_new($info)
            }
            unsupported_type => {
                // This macro should only be used on `ordered` numeric types.
                Err(anyhow::anyhow!(format!(
                    "Unsupported non-ordered input type {:?} for {}",
                    unsupported_type,
                    stringify!($evaluator)
                )))
            }
        }
    }};
}

/// Create a `Box<dyn Evaluator>` for a generic instruction.
///
/// The `$input_type` must be a `&DataType` corresponding to the signed input
/// to the instruction.
///
/// The `$primitive_evaluator` must be a struct that takes a single `T:
/// ArrowPrimitiveType` generic and which implements `Evaluator`.
///
/// The `$bool_evaluator` must be a struct that implements `Evaluator` and
/// expects the input to be a `BooleanArray`.
///
/// The `$string_evaluator` must be a struct that implements `Evaluator` and
/// expects the input to be a `BooleanArray`.
///
/// The `$info` should be a `StaticInfo`, which contains information relevant
/// to creating a new evaluator.
///
/// See `Eq`, `Neq`, `First`, `Last` for examples.
macro_rules! create_typed_evaluator {
    ($input_type:expr,
        $primitive_evaluator:ident,
        $bool_evaluator:ident,
        $string_evaluator:ident,
        $info:expr) => {{
        use $crate::evaluators::macros::Identity;
        create_typed_evaluator! {$input_type, $primitive_evaluator, $bool_evaluator, $string_evaluator, Identity, $info}
    }};

    ($input_type:expr,
        $primitive_evaluator:ident,
        $bool_evaluator:ident,
        $string_evaluator:ident,
        $aggf:ident,
        $info:expr) => {{
        use arrow::datatypes::*;
        use DataType::*;

        match $input_type {
            Int8 => $primitive_evaluator::<$aggf<Int8Type>>::try_new($info),
            Int16 => $primitive_evaluator::<$aggf<Int16Type>>::try_new($info),
            Int32 => $primitive_evaluator::<$aggf<Int32Type>>::try_new($info),
            Int64 => $primitive_evaluator::<$aggf<Int64Type>>::try_new($info),
            UInt8 => $primitive_evaluator::<$aggf<UInt8Type>>::try_new($info),
            UInt16 => $primitive_evaluator::<$aggf<UInt16Type>>::try_new($info),
            UInt32 => $primitive_evaluator::<$aggf<UInt32Type>>::try_new($info),
            UInt64 => $primitive_evaluator::<$aggf<UInt64Type>>::try_new($info),
            Float32 => $primitive_evaluator::<$aggf<Float32Type>>::try_new($info),
            Float64 => $primitive_evaluator::<$aggf<Float64Type>>::try_new($info),
            Timestamp(TimeUnit::Microsecond, None) => $primitive_evaluator::<$aggf<
                TimestampMicrosecondType,
            >>::try_new($info),
            Timestamp(TimeUnit::Millisecond, None) => $primitive_evaluator::<$aggf<
                TimestampMillisecondType,
            >>::try_new($info),
            Timestamp(TimeUnit::Nanosecond, None) => $primitive_evaluator::<$aggf<
                TimestampNanosecondType,
            >>::try_new($info),
            Timestamp(TimeUnit::Second, None) => {
                $primitive_evaluator::<$aggf<TimestampSecondType>>::try_new($info)
            }
            Date32 => $primitive_evaluator::<$aggf<Date32Type>>::try_new($info),
            Date64 => $primitive_evaluator::<$aggf<Date64Type>>::try_new($info),
            Time32(TimeUnit::Millisecond) => $primitive_evaluator::<$aggf<
                Time32MillisecondType,
            >>::try_new($info),
            Time32(TimeUnit::Second) => {
                $primitive_evaluator::<$aggf<Time32SecondType>>::try_new($info)
            }
            Time64(TimeUnit::Microsecond) => $primitive_evaluator::<$aggf<
                Time64MicrosecondType,
            >>::try_new($info),
            Time64(TimeUnit::Nanosecond) => {
                $primitive_evaluator::<$aggf<Time64NanosecondType>>::try_new($info)
            }
            Duration(TimeUnit::Second) => {
                $primitive_evaluator::<$aggf<DurationSecondType>>::try_new($info)
            }
            Duration(TimeUnit::Millisecond) => $primitive_evaluator::<$aggf<
                DurationMillisecondType,
            >>::try_new($info),
            Duration(TimeUnit::Microsecond) => $primitive_evaluator::<$aggf<
                DurationMicrosecondType,
            >>::try_new($info),
            Duration(TimeUnit::Nanosecond) => $primitive_evaluator::<$aggf<
                DurationNanosecondType,
            >>::try_new($info),
            Interval(IntervalUnit::DayTime) => {
                $primitive_evaluator::<$aggf<IntervalDayTimeType>>::try_new($info)
            }
            Interval(IntervalUnit::YearMonth) => $primitive_evaluator::<$aggf<
                IntervalYearMonthType,
            >>::try_new($info),
            Boolean => $bool_evaluator::try_new($info),
            Utf8 => $string_evaluator::try_new($info),
            unsupported => {
                Err(anyhow::anyhow!(format!(
                    "Unsupported type {:?} for {}",
                    unsupported,
                    stringify!($primitive_evaluator)
                )))
            }
        }
    }};
}

/// Create a `Box<dyn Evaluator>` for a map instruction.
///
/// The `$evaluator` must be a `struct` that takes a generic datatype T
/// and Offset O and which implements `Evaluator`.
///
/// The `$info` should be a `StaticInfo`, which contains information relevant
/// to creating a new evaluator.
/// # Example
///
/// ```no_run
/// struct GetStringToPrimitiveEvaluator<O: OffsetTrait, T: ArrowPrimitiveType>;
/// impl<O: OffsetTrait, T: ArrowPrimitiveType> Evaluator for GetStringToPrimitiveEvaluator<O, T> { ... }
///
/// fn create_evaluator(args: Vec<StaticArg>, result_type: &DataType) -> anyhow::Result<Self> {
///   create_map_evaluator!(key_type, map_value_type, GetStringToPrimitiveEvaluator, info)
/// }
/// ```
macro_rules! create_map_evaluator {
    ($key_type:expr, $value_type:expr, $string_to_primitive_evaluator:ident, $primitive_to_primitive_evaluator:ident, $primitive_to_string_evaluator:ident, $info:expr) => {{
        use arrow::datatypes::*;
        use create_primitive_map_evaluator;
        use create_string_map_evaluator;
        // Only types that are hashable (`key` type_class) can be used as keys are maps.
        match $key_type {
            DataType::Utf8 => {
                create_string_map_evaluator!(
                    i32,
                    $value_type,
                    $string_to_primitive_evaluator,
                    $info
                )
            }
            DataType::LargeUtf8 => {
                create_string_map_evaluator!(
                    i64,
                    $value_type,
                    $string_to_primitive_evaluator,
                    $info
                )
            }
            DataType::Int8 => {
                create_primitive_map_evaluator!(
                    Int8Type,
                    $value_type,
                    $primitive_to_primitive_evaluator,
                    $primitive_to_string_evaluator,
                    $info
                )
            }
            DataType::Int16 => {
                create_primitive_map_evaluator!(
                    Int16Type,
                    $value_type,
                    $primitive_to_primitive_evaluator,
                    $primitive_to_string_evaluator,
                    $info
                )
            }
            DataType::Int32 => {
                create_primitive_map_evaluator!(
                    Int32Type,
                    $value_type,
                    $primitive_to_primitive_evaluator,
                    $primitive_to_string_evaluator,
                    $info
                )
            }
            DataType::Int64 => {
                create_primitive_map_evaluator!(
                    Int64Type,
                    $value_type,
                    $primitive_to_primitive_evaluator,
                    $primitive_to_string_evaluator,
                    $info
                )
            }
            DataType::UInt16 => {
                create_primitive_map_evaluator!(
                    UInt16Type,
                    $value_type,
                    $primitive_to_primitive_evaluator,
                    $primitive_to_string_evaluator,
                    $info
                )
            }
            DataType::UInt32 => {
                create_primitive_map_evaluator!(
                    UInt32Type,
                    $value_type,
                    $primitive_to_primitive_evaluator,
                    $primitive_to_string_evaluator,
                    $info
                )
            }
            DataType::UInt64 => {
                create_primitive_map_evaluator!(
                    UInt64Type,
                    $value_type,
                    $primitive_to_primitive_evaluator,
                    $primitive_to_string_evaluator,
                    $info
                )
            }
            // TODO: Bool
            unsupported_type => Err(anyhow::anyhow!(format!(
                "unsupported key type {:?} for map evaluator",
                unsupported_type
            ))),
        }
    }};
}

macro_rules! create_primitive_map_evaluator {
    ($key_type:ident, $value_type:expr, $primitive_evaluator:ident, $string_evaluator:ident, $info:expr) => {{
        use arrow::datatypes::*;
        match $value_type {
            DataType::Int32 => $primitive_evaluator::<$key_type, Int32Type>::try_new($info),
            DataType::Int64 => $primitive_evaluator::<$key_type, Int64Type>::try_new($info),
            DataType::UInt32 => $primitive_evaluator::<$key_type, UInt32Type>::try_new($info),
            DataType::UInt64 => $primitive_evaluator::<$key_type, UInt64Type>::try_new($info),
            DataType::Float32 => $primitive_evaluator::<$key_type, Float32Type>::try_new($info),
            DataType::Float64 => $primitive_evaluator::<$key_type, Float64Type>::try_new($info),
            DataType::Timestamp(TimeUnit::Second, None) => {
                $primitive_evaluator::<$key_type, TimestampSecondType>::try_new($info)
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                $primitive_evaluator::<$key_type, TimestampMillisecondType>::try_new($info)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                $primitive_evaluator::<$key_type, TimestampMicrosecondType>::try_new($info)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                $primitive_evaluator::<$key_type, TimestampNanosecondType>::try_new($info)
            }
            DataType::Duration(TimeUnit::Second) => {
                $primitive_evaluator::<$key_type, DurationSecondType>::try_new($info)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                $primitive_evaluator::<$key_type, DurationMillisecondType>::try_new($info)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                $primitive_evaluator::<$key_type, DurationMicrosecondType>::try_new($info)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                $primitive_evaluator::<$key_type, DurationNanosecondType>::try_new($info)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                $primitive_evaluator::<$key_type, IntervalDayTimeType>::try_new($info)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                $primitive_evaluator::<$key_type, IntervalYearMonthType>::try_new($info)
            }
            DataType::Utf8 => $string_evaluator::<$key_type, i32>::try_new($info),
            DataType::LargeUtf8 => $string_evaluator::<$key_type, i64>::try_new($info),
            unsupported_type => Err(anyhow::anyhow!(format!(
                "unsupported value type {:?} for primitive map evaluator",
                unsupported_type
            ))),
        }
    }};
}

macro_rules! create_string_map_evaluator {
    ($offset_size:ident, $input_type:expr, $evaluator:ident, $info:expr) => {{
        use arrow::datatypes::*;
        match $input_type {
            DataType::Int32 => $evaluator::<$offset_size, Int32Type>::try_new($info),
            DataType::Int64 => $evaluator::<$offset_size, Int64Type>::try_new($info),
            DataType::UInt32 => $evaluator::<$offset_size, UInt32Type>::try_new($info),
            DataType::UInt64 => $evaluator::<$offset_size, UInt64Type>::try_new($info),
            DataType::Float32 => $evaluator::<$offset_size, Float32Type>::try_new($info),
            DataType::Float64 => $evaluator::<$offset_size, Float64Type>::try_new($info),
            DataType::Timestamp(TimeUnit::Second, None) => {
                $evaluator::<$offset_size, TimestampSecondType>::try_new($info)
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                $evaluator::<$offset_size, TimestampMillisecondType>::try_new($info)
            }
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                $evaluator::<$offset_size, TimestampMicrosecondType>::try_new($info)
            }
            DataType::Timestamp(TimeUnit::Nanosecond, None) => {
                $evaluator::<$offset_size, TimestampNanosecondType>::try_new($info)
            }
            DataType::Duration(TimeUnit::Second) => {
                $evaluator::<$offset_size, DurationSecondType>::try_new($info)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                $evaluator::<$offset_size, DurationMillisecondType>::try_new($info)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                $evaluator::<$offset_size, DurationMicrosecondType>::try_new($info)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                $evaluator::<$offset_size, DurationNanosecondType>::try_new($info)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                $evaluator::<$offset_size, IntervalDayTimeType>::try_new($info)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                $evaluator::<$offset_size, IntervalYearMonthType>::try_new($info)
            }
            unsupported_type => Err(anyhow::anyhow!(format!(
                "Unsupported non-primitive value type {:?} for {}",
                unsupported_type,
                stringify!($evaluator)
            ))),
        }
    }};
}

pub(super) use {
    create_float_evaluator, create_map_evaluator, create_number_evaluator,
    create_ordered_evaluator, create_primitive_map_evaluator, create_signed_evaluator,
    create_string_map_evaluator, create_typed_evaluator,
};
