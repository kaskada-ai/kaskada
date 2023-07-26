//! Provides a kernel for hashing an arbitrary Arrow array to a UInt64Array.
use std::cell::RefCell;

use crate::hasher::Hasher;
use arrow::array::{Array, UInt64Array};
use arrow::datatypes::DataType;

pub fn can_hash(data_type: &DataType) -> bool {
    match data_type {
        primitive if primitive.is_primitive() => true,
        DataType::Null
        | DataType::Boolean
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => true,
        DataType::Dictionary(_, value) => can_hash(value),
        DataType::Struct(fields) => fields.iter().all(|f| can_hash(f.data_type())),
        _ => false,
    }
}

thread_local! {
    /// Thread-local hasher.
    ///
    /// TODO: Move this to the hasher and make it easy to automatically
    /// use this instance.
    static HASHER: RefCell<Hasher> = RefCell::new(Hasher::default());
}

/// Return an `ArrayRef` to a `UInt64Array` containing the hash of each row of
/// the array.
pub fn hash(array: &dyn Array) -> error_stack::Result<UInt64Array, crate::hasher::Error> {
    HASHER.with(|hasher| {
        let mut hasher = hasher.borrow_mut();
        hasher.hash_to_uint64(array)
    })
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
            &[1472103086483932002, 0, 8057155968893317866]
        );
    }
}
