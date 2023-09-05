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
            DataType::Duration(TimeUnit::Second) => {
                $evaluator::<$aggf<DurationSecondType>>::try_new($info)
            }
            DataType::Duration(TimeUnit::Millisecond) => {
                $evaluator::<$aggf<DurationMillisecondType>>::try_new($info)
            }
            DataType::Duration(TimeUnit::Microsecond) => {
                $evaluator::<$aggf<DurationMicrosecondType>>::try_new($info)
            }
            DataType::Duration(TimeUnit::Nanosecond) => {
                $evaluator::<$aggf<DurationNanosecondType>>::try_new($info)
            }
            DataType::Interval(IntervalUnit::DayTime) => {
                $evaluator::<$aggf<IntervalDayTimeType>>::try_new($info)
            }
            DataType::Interval(IntervalUnit::YearMonth) => {
                $evaluator::<$aggf<IntervalYearMonthType>>::try_new($info)
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
        $struct_evaluator:ident,
        $list_evaluator:ident,
        $map_evaluator:ident,
        $bool_evaluator:ident,
        $string_evaluator:ident,
        $info:expr) => {{
        use $crate::evaluators::macros::Identity;
        create_typed_evaluator! {$input_type, $primitive_evaluator, $struct_evaluator, $list_evaluator, $map_evaluator, $bool_evaluator, $string_evaluator, Identity, $info}
    }};

    ($input_type:expr,
        $primitive_evaluator:ident,
        $struct_evaluator:ident,
        $list_evaluator:ident,
        $map_evaluator:ident,
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
            Map(..) => $map_evaluator::try_new($info),
            List(..) => $list_evaluator::try_new($info),
            Struct(..) => $struct_evaluator::try_new($info),
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

pub(super) use {
    create_float_evaluator, create_number_evaluator, create_ordered_evaluator,
    create_signed_evaluator, create_typed_evaluator,
};
