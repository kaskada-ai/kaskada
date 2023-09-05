use std::str::FromStr;

use anyhow::{anyhow, Context};
use arrow::datatypes::*;
use serde::Serialize;
use sparrow_arrow::scalar_value::ScalarValue;

/// Identifies a literal value. For general value manipulation we should prefer
/// to operate on Arrow value arrays.
///
/// This is most often used in cases where one argument to an operation is a
/// constant. In such cases, a different implementation of the
/// sparrow-runtime may be used to avoid materializing the literal.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize)]
pub enum LiteralValue {
    Null,
    True,
    False,
    /// Numeric literal of arbitrary type.
    ///
    /// The literal is only converted to a specific type when needed. To avoid
    /// loss of precision we retain the original string.
    Number(String),
    String(String),
    Timedelta {
        seconds: i64,
        nanos: i64,
    },
}

impl std::fmt::Display for LiteralValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LiteralValue::Null => write!(f, "null"),
            LiteralValue::True => write!(f, "true"),
            LiteralValue::False => write!(f, "false"),
            LiteralValue::Number(n) => write!(f, "{n}"),
            LiteralValue::String(s) => write!(f, "{s}"),
            LiteralValue::Timedelta { seconds, nanos } => write!(f, "s:{seconds},nanos:{nanos}"),
        }
    }
}

impl LiteralValue {
    /// Convert a parsed literal to a `ScalarValue` guessing the type.
    pub fn to_scalar(&self) -> anyhow::Result<ScalarValue> {
        match self {
            LiteralValue::Null => Ok(ScalarValue::Null),
            LiteralValue::True => Ok(ScalarValue::Boolean(Some(true))),
            LiteralValue::False => Ok(ScalarValue::Boolean(Some(false))),
            LiteralValue::Number(number) => {
                // We currently treat literals as either Float64 or Int64 and introduce
                // explicit widening of other values to match. Other options to consider
                // include
                // (1) using the prefix `-` to decide between `i64` and `u64`
                // (2) allowing implicit conversion between different types of literals
                //     as long as the literal value "fits" in the result type
                if number
                    .chars()
                    .rev()
                    .any(|c| c == 'i' || c == 'f' || c == 'u')
                {
                    ScalarValue::from_str(number)
                } else if number.contains('.') {
                    to_scalar_primitive(number, &DataType::Float64, ScalarValue::Float64)
                } else {
                    to_scalar_primitive(number, &DataType::Int64, ScalarValue::Int64)
                }
            }
            LiteralValue::String(s) => Ok(ScalarValue::Utf8(Some(s.clone()))),
            LiteralValue::Timedelta { seconds, nanos } => {
                // Timedeltas are going to be represented as `DataType::Duration(ns)`
                // so manually convert the seconds to nanos then add the remainder
                let (s_to_ns, overflow_mul) = (*seconds).overflowing_mul(1e9 as i64);
                let (duration_nanos, overflow_add) = s_to_ns.overflowing_add(*nanos);
                if overflow_mul || overflow_add {
                    anyhow::bail!("Overflow converting seconds to nanoseconds")
                }

                Ok(ScalarValue::Duration(
                    Some(duration_nanos),
                    TimeUnit::Nanosecond,
                ))
            }
        }
    }

    /// Convert a parsed literal to a `ScalarValue` of a specific type.
    pub fn to_specific_scalar(&self, data_type: &DataType) -> anyhow::Result<ScalarValue> {
        match (self, data_type) {
            (Self::Null, _) => Ok(ScalarValue::try_new_null(data_type)?),
            (Self::True, DataType::Boolean) => Ok(ScalarValue::Boolean(Some(true))),
            (Self::False, DataType::Boolean) => Ok(ScalarValue::Boolean(Some(false))),
            (Self::Number(n), DataType::Int8) => {
                to_scalar_primitive(n, data_type, ScalarValue::Int8)
            }
            (Self::Number(n), DataType::Int16) => {
                to_scalar_primitive(n, data_type, ScalarValue::Int16)
            }
            (Self::Number(n), DataType::Int32) => {
                to_scalar_primitive(n, data_type, ScalarValue::Int32)
            }
            (Self::Number(n), DataType::Int64) => {
                to_scalar_primitive(n, data_type, ScalarValue::Int64)
            }
            (Self::Number(n), DataType::UInt8) => {
                to_scalar_primitive(n, data_type, ScalarValue::UInt8)
            }
            (Self::Number(n), DataType::UInt16) => {
                to_scalar_primitive(n, data_type, ScalarValue::UInt16)
            }
            (Self::Number(n), DataType::UInt32) => {
                to_scalar_primitive(n, data_type, ScalarValue::UInt32)
            }
            (Self::Number(n), DataType::UInt64) => {
                to_scalar_primitive(n, data_type, ScalarValue::UInt64)
            }
            (Self::Number(n), DataType::Float32) => {
                to_scalar_primitive(n, data_type, ScalarValue::Float32)
            }
            (Self::Number(n), DataType::Float64) => {
                to_scalar_primitive(n, data_type, ScalarValue::Float64)
            }
            (Self::String(s), DataType::Utf8) => Ok(ScalarValue::Utf8(Some(s.clone()))),
            _ => Err(anyhow!(
                "Unable to convert literal {:?} to scalar of type {:?}",
                self,
                data_type
            )),
        }
    }
}

fn to_scalar_primitive<T>(
    n: &str,
    data_type: &DataType,
    to_scalar: impl FnOnce(Option<T>) -> ScalarValue,
) -> anyhow::Result<ScalarValue>
where
    T: FromStr,
    T::Err: std::error::Error + Sync + Send + 'static,
{
    let primitive = T::from_str(n)
        .with_context(|| format!("Unable to convert literal '{n}' to {data_type:?}"))?;

    Ok(to_scalar(Some(primitive)))
}
