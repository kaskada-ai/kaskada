//! Provides a kernel for hashing an arbitrary Arrow array to a UInt64Array.

use std::hash::{BuildHasher, Hash, Hasher};

use anyhow::anyhow;
use arrow::array::{Array, OffsetSizeTrait, UInt64Array};
use arrow::datatypes::{
    ArrowPrimitiveType, DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type,
    UInt32Type, UInt64Type,
};
use sparrow_core::{downcast_boolean_array, downcast_primitive_array, downcast_string_array};

pub fn can_hash(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Null
            | DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Utf8
            | DataType::LargeUtf8
    )
}

/// Return an `ArrayRef` to a `UInt64Array` containing the hash of each row of
/// the array.
pub fn hash(array: &dyn Array) -> anyhow::Result<UInt64Array> {
    match array.data_type() {
        DataType::Null => {
            // The null array contains only null values. Hash that to an array of 0.
            Ok(UInt64Array::from(vec![0; array.len()]))
        }
        DataType::Boolean => hash_boolean(array),
        DataType::Int8 => hash_primitive::<Int8Type>(array),
        DataType::Int16 => hash_primitive::<Int16Type>(array),
        DataType::Int32 => hash_primitive::<Int32Type>(array),
        DataType::Int64 => hash_primitive::<Int64Type>(array),
        DataType::UInt16 => hash_primitive::<UInt16Type>(array),
        DataType::UInt32 => hash_primitive::<UInt32Type>(array),
        DataType::UInt64 => hash_primitive::<UInt64Type>(array),
        DataType::Utf8 => hash_string::<i32>(array),
        DataType::LargeUtf8 => hash_string::<i64>(array),
        todo => Err(anyhow!("Hashing of type {:?}", todo)),
    }
}

fn fixed_seed_hasher() -> ahash::AHasher {
    ahash::random_state::RandomState::with_seeds(1234, 5678, 9012, 3456).build_hasher()
}

fn hash_string<T>(array: &dyn Array) -> anyhow::Result<UInt64Array>
where
    T: OffsetSizeTrait,
{
    let string_array = downcast_string_array::<T>(array)?;

    let mut builder = UInt64Array::builder(array.len());

    for string in string_array {
        let mut hasher = fixed_seed_hasher();
        string.hash(&mut hasher);
        builder.append_value(hasher.finish());
    }

    Ok(builder.finish())
}

fn hash_primitive<T>(array: &dyn Array) -> anyhow::Result<UInt64Array>
where
    T: ArrowPrimitiveType,
    T::Native: std::hash::Hash,
{
    let primitive_array = downcast_primitive_array::<T>(array)?;

    let mut builder = UInt64Array::builder(array.len());

    for primitive in primitive_array {
        let mut hasher = fixed_seed_hasher();
        primitive.hash(&mut hasher);
        builder.append_value(hasher.finish());
    }

    Ok(builder.finish())
}

fn hash_boolean(array: &dyn Array) -> anyhow::Result<UInt64Array> {
    let boolean_array = downcast_boolean_array(array)?;

    let mut builder = UInt64Array::builder(array.len());

    for boolean in boolean_array {
        let mut hasher = fixed_seed_hasher();
        boolean.hash(&mut hasher);
        builder.append_value(hasher.finish());
    }

    Ok(builder.finish())
}

#[cfg(test)]
mod tests {
    use arrow::array::{StringArray, UInt64Array};

    use super::*;

    #[test]
    fn test_hash_uint64() {
        let array = UInt64Array::from(vec![Some(5), None, Some(8), Some(5), None]);

        let hashes = hash(&array).unwrap();
        assert_eq!(hashes.value(0), hashes.value(3));
        assert_eq!(hashes.value(1), hashes.value(4));
        assert_ne!(hashes.value(0), hashes.value(1));
        assert_ne!(hashes.value(0), hashes.value(2));
    }

    #[test]
    fn test_hash_string() {
        let array = StringArray::from(vec![
            Some("hello"),
            None,
            Some("world"),
            Some("hello"),
            None,
        ]);

        let hashes = hash(&array).unwrap();
        assert_eq!(hashes.value(0), hashes.value(3));
        assert_eq!(hashes.value(1), hashes.value(4));
        assert_ne!(hashes.value(0), hashes.value(1));
        assert_ne!(hashes.value(0), hashes.value(2));
    }

    #[test]
    fn test_hash_string_stability() {
        let array = StringArray::from(vec![Some("hello"), None, Some("world")]);

        let hashes = hash(&array).unwrap();
        assert_eq!(
            hashes.values(),
            &[
                13572866306152653102,
                11832085162654999889,
                16979493163667785006
            ]
        );
    }
}
