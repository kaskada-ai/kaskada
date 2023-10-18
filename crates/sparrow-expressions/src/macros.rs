/// Create a primitive evaluator.
///
/// Invoked as `create_primitive_evaluator(arg_index, evaluator[, supported_types])`.
///
/// Arguments:
///
/// - `arg_index` is the index of the argument to use to determine the primitive type.
///   Generally, this should be the first argument associated with the primitive type.
/// - `evaluator` is the name of the evalutaor to instantiate.
/// - A list of `(ty_case, ty_name)` pairs, where `ty_case` is the data type case
///   (eg., `Int32`) and `ty_name` is the corresponding array (eg., `Int32Type`).
///
/// The third argument can also be one of the special types -- `number`, `float`, `ordered`
/// or `signed`, which expands to the corresponding list of types.
///
/// Omitting the third argument creates an evaluator for any datatype that is supported
/// by `PrimitiveArray`.
macro_rules! create_primitive_evaluator {
    ($type_index:expr, $evaluator:ident, number) => {
        crate::macros::create_primitive_evaluator! {$type_index, $evaluator,
          (arrow_schema::DataType::Int32, Int32Type),
          (arrow_schema::DataType::Int64, Int64Type),
          (arrow_schema::DataType::UInt32, UInt32Type),
          (arrow_schema::DataType::UInt64, UInt64Type),
          (arrow_schema::DataType::Float32, Float32Type),
          (arrow_schema::DataType::Float64, Float64Type)
        }
    };
    ($type_index:expr, $evaluator:ident, signed) => {
        crate::macros::create_primitive_evaluator! {$type_index, $evaluator,
          (arrow_schema::DataType::Int32, Int32Type),
          (arrow_schema::DataType::Int64, Int64Type),
          (arrow_schema::DataType::Float32, Float32Type),
          (arrow_schema::DataType::Float64, Float64Type)
        }
    };
    ($type_index:expr, $evaluator:ident, float) => {
        crate::macros::create_primitive_evaluator! {$type_index, $evaluator,
          (arrow_schema::DataType::Float32, Float32Type),
          (arrow_schema::DataType::Float64, Float64Type)
        }
    };
    ($type_index:expr, $evaluator:ident, ordered) => {
        crate::macros::create_primitive_evaluator! {$type_index, $evaluator,
            (arrow_schema::DataType::Int32, Int32Type),
            (arrow_schema::DataType::Int64, Int64Type),
            (arrow_schema::DataType::UInt32, UInt32Type),
            (arrow_schema::DataType::UInt64, UInt64Type),
            (arrow_schema::DataType::Float32, Float32Type),
            (arrow_schema::DataType::Float64, Float64Type),
            (arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Second, None), TimestampSecondType),
            (arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Millisecond, None), TimestampMillisecondType),
            (arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None), TimestampMicrosecondType),
            (arrow_schema::DataType::Timestamp(arrow_schema::TimeUnit::Nanosecond, None), TimestampNanosecondType)
        }
    };
    ($type_index:expr, $evaluator:ident, $(($ty_case:pat, $ty_name:ident)),+) => {
        |info: sparrow_interfaces::expression::StaticInfo<'_>| -> error_stack::Result<Box<dyn sparrow_interfaces::expression::Evaluator>, sparrow_interfaces::expression::Error> {
            use arrow_array::types::*;
            match info.args[$type_index].data_type {
                $($ty_case => $evaluator::<$ty_name>(info),)*
                unsupported => {
                    error_stack::bail!(Error::UnsupportedArgumentType {
                        name: info.name.clone(),
                        actual: unsupported.clone(),
                    })
                }
            }
    }};
}

pub(super) use create_primitive_evaluator;
