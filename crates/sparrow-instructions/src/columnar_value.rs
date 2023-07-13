use anyhow::anyhow;
use arrow::array::{
    Array, ArrayRef, BooleanArray, GenericStringArray, MapArray, OffsetSizeTrait, PrimitiveArray,
    StructArray,
};
use arrow::datatypes::*;
use owning_ref::ArcRef;
use sparrow_arrow::downcast::{
    downcast_boolean_array, downcast_map_array, downcast_primitive_array, downcast_string_array,
    downcast_struct_array,
};
use sparrow_arrow::scalar_value::{NativeFromScalar, ScalarValue};

/// The input to an instruction.
#[derive(Debug, Clone)]
pub enum ColumnarValue {
    /// Indicates the input is stored in the given `ArrayRef`.
    Array(ArrayRef),
    /// Indicates the input is a literal.
    ///
    /// We may be able to avoid creating an array containing the literal by
    /// using specialized kernels operating on scalar values.
    Literal { rows: usize, literal: ScalarValue },
}

impl ColumnarValue {
    /// Retrieve a generic `ArrayRef` corresponding to this `ColumnarValue`.
    pub fn array_ref(&self) -> anyhow::Result<ArrayRef> {
        match &self {
            Self::Array(array_ref) => Ok(array_ref.clone()),
            Self::Literal { rows, literal } => {
                // TODO: Implementing an operation such as `x + 1` is best done by
                // specialization. A pass over the `x` array adding `1` to each
                // item and producing a new array. In some cases, we won't
                // detect / specialize these cases, so we fallback
                // to materializing an array of 1s. We log this situation because it indicates
                // a potentially easy performance improvement.
                Ok(literal.to_array(*rows))
            }
        }
    }

    /// Determine the `DataType` of this value.
    pub fn data_type(&self) -> DataType {
        match &self {
            Self::Array(array_ref) => array_ref.data_type().clone(),
            Self::Literal { literal, .. } => literal.data_type(),
        }
    }

    /// Specialized version of `array_ref` that downcasts the array to a
    /// struct array.
    pub fn struct_array(&self) -> anyhow::Result<ArcRef<dyn Array, StructArray>> {
        let array = self.array_ref()?;
        ArcRef::new(array).try_map(|a| downcast_struct_array(a))
    }

    /// Specialized version of `array_ref` that downcasts the array to a
    /// map array.
    pub fn map_array(&self) -> anyhow::Result<ArcRef<dyn Array, MapArray>> {
        let array = self.array_ref()?;
        ArcRef::new(array).try_map(|a| downcast_map_array(a))
    }

    /// Specialized version of `array_ref` that downcasts the array to a
    /// string array.
    pub fn string_array<T>(&self) -> anyhow::Result<ArcRef<dyn Array, GenericStringArray<T>>>
    where
        T: OffsetSizeTrait,
    {
        let array = self.array_ref()?;
        ArcRef::new(array).try_map(|a| downcast_string_array(a))
    }

    /// Specialized version of `array_ref` that downcasts the array to a
    /// primitive. This method will fail if the array is not of the correct
    /// type.
    pub fn primitive_array<T: ArrowPrimitiveType>(
        &self,
    ) -> anyhow::Result<ArcRef<dyn Array, PrimitiveArray<T>>> {
        let array = self.array_ref()?;
        ArcRef::new(array).try_map(|a| downcast_primitive_array(a))
    }

    /// Specialized version of `array_ref` that downcasts the array to a
    /// boolean array.
    pub fn boolean_array(&self) -> anyhow::Result<ArcRef<dyn Array, BooleanArray>> {
        let array = self.array_ref()?;
        ArcRef::new(array).try_map(|a| downcast_boolean_array(a))
    }

    /// Attempt to get the native value of a literal from this `ColumnarValue`.
    ///
    /// Returns `Ok(None)` if this is a literal null.
    /// Returns `Err` if this not a literal.
    pub fn try_primitive_literal<T>(&self) -> anyhow::Result<Option<T::Native>>
    where
        T: NativeFromScalar,
    {
        match self {
            Self::Literal { literal, .. } => T::native_from_scalar(literal),
            _ => Err(anyhow!("{:?} is not a literal", self)),
        }
    }

    /// Attempt to get the boolean value of a literal from this `ColumnarValue`.
    ///
    /// Returns `Ok(None)` if this is a literal null.
    /// Returns `Err` if this not a literal.
    pub fn try_boolean_literal(&self) -> anyhow::Result<Option<bool>> {
        match self {
            Self::Literal {
                literal: ScalarValue::Boolean(b),
                ..
            } => Ok(*b),
            _ => Err(anyhow!(
                "{:?} is not a literal or not of type {:?}",
                self,
                DataType::Boolean
            )),
        }
    }

    /// Attempt to get the string value of a literal from this `ColumnarValue`.
    ///
    /// Returns `None` if this is not a literal.
    pub fn try_string_literal(&self) -> anyhow::Result<Option<&str>> {
        match self {
            Self::Literal {
                literal: ScalarValue::Utf8(str),
                ..
            } => Ok(str.as_deref()),
            Self::Literal {
                literal: ScalarValue::LargeUtf8(str),
                ..
            } => Ok(str.as_deref()),
            _ => Err(anyhow!(
                "Literal {:?} is not of type {:?}",
                self,
                DataType::Utf8
            )),
        }
    }

    pub fn is_null_literal(&self) -> bool {
        matches!(
            self,
            Self::Literal {
                literal: ScalarValue::Null,
                ..
            }
        )
    }
}
