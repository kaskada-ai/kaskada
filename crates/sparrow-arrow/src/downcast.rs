//! Utilities for downcasting Arrow arrays.

use anyhow::Context;
use arrow::array::{
    Array, BooleanArray, GenericStringArray, OffsetSizeTrait, PrimitiveArray, StructArray,
};
use arrow::datatypes::ArrowPrimitiveType;
use arrow_array::MapArray;

/// Downcast an `ArrayRef` to a `PrimitiveArray<T>`.
pub fn downcast_primitive_array<T: ArrowPrimitiveType>(
    array: &dyn Array,
) -> anyhow::Result<&PrimitiveArray<T>> {
    array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .with_context(|| {
            format!(
                "Unable to downcast {:?} to {:?}",
                array.data_type(),
                T::DATA_TYPE
            )
        })
}

/// Downcast an array into a string array.
pub fn downcast_string_array<T>(array: &dyn Array) -> anyhow::Result<&GenericStringArray<T>>
where
    T: OffsetSizeTrait,
{
    array
        .as_any()
        .downcast_ref::<GenericStringArray<T>>()
        .with_context(|| {
            format!(
                "Unable to downcast {:?} to {:?}",
                array.data_type(),
                GenericStringArray::<T>::DATA_TYPE,
            )
        })
}

/// Downcast an `ArrayRef` to a `StructArray`.
pub fn downcast_struct_array(array: &dyn Array) -> anyhow::Result<&StructArray> {
    array
        .as_any()
        .downcast_ref::<StructArray>()
        .with_context(|| format!("Unable to downcast {:?} to struct array", array.data_type()))
}

/// Downcast an `ArrayRef` to a `MapArray`.
pub fn downcast_map_array(array: &dyn Array) -> anyhow::Result<&MapArray> {
    array
        .as_any()
        .downcast_ref::<MapArray>()
        .with_context(|| format!("Unable to downcast {:?} to map array", array.data_type()))
}

/// Downcast an `ArrayRef` to a `BooleanArray`.
pub fn downcast_boolean_array(array: &dyn Array) -> anyhow::Result<&BooleanArray> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .with_context(|| {
            format!(
                "Unable to downcast {:?} to boolean array",
                array.data_type(),
            )
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float64Array};
    use arrow::datatypes::{Float64Type, Int64Type};

    use super::*;

    #[test]
    fn test_downcast_primitive_array() {
        let f64_array = Float64Array::from(vec![0.0, 1.0, 2.0]);
        let f64_array_ref: ArrayRef = Arc::new(f64_array);

        let downcast_ok = downcast_primitive_array::<Float64Type>(f64_array_ref.as_ref());
        assert!(downcast_ok.is_ok());
        assert_eq!(
            downcast_ok.unwrap(),
            &Float64Array::from(vec![0.0, 1.0, 2.0])
        );

        let downcast_err = downcast_primitive_array::<Int64Type>(f64_array_ref.as_ref());
        assert!(downcast_err.is_err());
        assert_eq!(
            downcast_err.unwrap_err().to_string(),
            "Unable to downcast Float64 to Int64"
        );
    }
}
