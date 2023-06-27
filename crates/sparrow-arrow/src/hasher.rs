// Based on hashing code from datafusion.

use ahash::RandomState;
use arrow_array::cast::{
    as_boolean_array, as_generic_binary_array, as_largestring_array, as_primitive_array,
    as_string_array, as_struct_array,
};
use arrow_array::types::{ArrowDictionaryKeyType, Decimal128Type, Decimal256Type};
use arrow_array::{
    downcast_dictionary_array, downcast_primitive_array, Array, ArrayAccessor, DictionaryArray,
    FixedSizeBinaryArray,
};
use arrow_buffer::{i256, ArrowNativeType};
use arrow_schema::DataType;

pub struct Hasher {
    random_state: RandomState,
    hash_buffer: Vec<u64>,
}

impl Default for Hasher {
    fn default() -> Self {
        let random_state = ahash::random_state::RandomState::with_seeds(1234, 5678, 9012, 3456);
        Self {
            random_state,
            hash_buffer: Default::default(),
        }
    }
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "must have at least one array to hash")]
    NoArraysToHash,
    #[display(fmt = "hash of '{_0:?}' unsupported")]
    UnsupportedType(DataType),
}

impl error_stack::Context for Error {}

impl Hasher {
    pub fn hash_array(
        &mut self,
        array: impl std::borrow::Borrow<dyn Array>,
    ) -> error_stack::Result<&[u64], Error> {
        self.hash_arrays(&[array.borrow()])
    }

    pub fn hash_arrays(
        &mut self,
        arrays: &[impl std::borrow::Borrow<dyn Array>],
    ) -> error_stack::Result<&[u64], Error> {
        error_stack::ensure!(!arrays.is_empty(), Error::NoArraysToHash);

        let num_rows = arrays[0].borrow().len();

        self.hash_buffer.clear();
        self.hash_buffer.resize(num_rows, 0);
        create_hashes(arrays, &self.random_state, &mut self.hash_buffer, false)?;

        Ok(&self.hash_buffer)
    }
}

/// Creates hash values for every row, based on the values in the
/// columns.
///
/// The number of rows to hash is determined by `hashes_buffer.len()`.
/// `hashes_buffer` should be pre-sized appropriately
#[allow(clippy::ptr_arg)]
fn create_hashes(
    arrays: &[impl std::borrow::Borrow<dyn Array>],
    random_state: &RandomState,
    hashes_buffer: &mut Vec<u64>,
    mut multi_col: bool,
) -> error_stack::Result<(), Error> {
    for col in arrays {
        let array = col.borrow();
        downcast_primitive_array! {
            array => hash_array(array, random_state, hashes_buffer, multi_col),
            DataType::Null => hash_null(random_state, hashes_buffer, multi_col),
            DataType::Boolean => hash_array(as_boolean_array(array), random_state, hashes_buffer, multi_col),
            DataType::Utf8 => hash_array(as_string_array(array), random_state, hashes_buffer, multi_col),
            DataType::LargeUtf8 => hash_array(as_largestring_array(array), random_state, hashes_buffer, multi_col),
            DataType::Binary => hash_array(as_generic_binary_array::<i32>(array), random_state, hashes_buffer, multi_col),
            DataType::LargeBinary => hash_array(as_generic_binary_array::<i64>(array), random_state, hashes_buffer, multi_col),
            DataType::FixedSizeBinary(_) => {
                let array: &FixedSizeBinaryArray = array.as_any().downcast_ref().unwrap();
                hash_array(array, random_state, hashes_buffer, multi_col)
            }
            DataType::Decimal128(_, _) => {
                let array = as_primitive_array::<Decimal128Type>(array);
                hash_array(array, random_state, hashes_buffer, multi_col)
            }
            DataType::Decimal256(_, _) => {
                let array = as_primitive_array::<Decimal256Type>(array);
                hash_array(array, random_state, hashes_buffer, multi_col)
            }
            DataType::Dictionary(_, _) => downcast_dictionary_array! {
                array => hash_dictionary(array, random_state, hashes_buffer, multi_col)?,
                _ => unreachable!()
            }
            DataType::Struct(_) => {
                let array = as_struct_array(array);
                create_hashes(array.columns(), random_state, hashes_buffer, multi_col)?;
            }
            unsupported => {
                // This is internal because we should have caught this before.
                error_stack::bail!(Error::UnsupportedType(unsupported.clone()))
            }
        }

        multi_col = true;
    }

    Ok(())
}

#[inline]
fn combine_hashes(l: u64, r: u64) -> u64 {
    let hash = (17 * 37u64).wrapping_add(l);
    hash.wrapping_mul(37).wrapping_add(r)
}

fn hash_null(random_state: &RandomState, hashes_buffer: &'_ mut [u64], mul_col: bool) {
    if mul_col {
        hashes_buffer.iter_mut().for_each(|hash| {
            // stable hash for null value
            *hash = combine_hashes(random_state.hash_one(1), *hash);
        })
    } else {
        hashes_buffer.iter_mut().for_each(|hash| {
            *hash = random_state.hash_one(1);
        })
    }
}

trait HashValue {
    fn hash_one(&self, state: &RandomState) -> u64;
}

impl<'a, T: HashValue + ?Sized> HashValue for &'a T {
    fn hash_one(&self, state: &RandomState) -> u64 {
        T::hash_one(self, state)
    }
}

macro_rules! hash_value {
    ($($t:ty),+) => {
        $(impl HashValue for $t {
            fn hash_one(&self, state: &RandomState) -> u64 {
                state.hash_one(self)
            }
        })+
    };
}
hash_value!(i8, i16, i32, i64, i128, i256, u8, u16, u32, u64);
hash_value!(bool, str, [u8]);

macro_rules! hash_float_value {
    ($(($t:ty, $i:ty)),+) => {
        $(impl HashValue for $t {
            fn hash_one(&self, state: &RandomState) -> u64 {
                state.hash_one(<$i>::from_ne_bytes(self.to_ne_bytes()))
            }
        })+
    };
}
hash_float_value!((half::f16, u16), (f32, u32), (f64, u64));

fn hash_array<T>(array: T, random_state: &RandomState, hashes_buffer: &mut [u64], multi_col: bool)
where
    T: ArrayAccessor,
    T::Item: HashValue,
{
    if array.null_count() == 0 {
        if multi_col {
            for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                *hash = combine_hashes(array.value(i).hash_one(random_state), *hash);
            }
        } else {
            for (i, hash) in hashes_buffer.iter_mut().enumerate() {
                *hash = array.value(i).hash_one(random_state);
            }
        }
    } else if multi_col {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                *hash = combine_hashes(array.value(i).hash_one(random_state), *hash);
            }
        }
    } else {
        for (i, hash) in hashes_buffer.iter_mut().enumerate() {
            if !array.is_null(i) {
                *hash = array.value(i).hash_one(random_state);
            }
        }
    }
}

/// Hash the values in a dictionary array
fn hash_dictionary<K: ArrowDictionaryKeyType>(
    array: &DictionaryArray<K>,
    random_state: &RandomState,
    hashes_buffer: &mut [u64],
    multi_col: bool,
) -> error_stack::Result<(), Error> {
    // Hash each dictionary value once, and then use that computed
    // hash for each key value to avoid a potentially expensive
    // redundant hashing for large dictionary elements (e.g. strings)
    let values = array.values().clone();
    let mut dict_hashes = vec![0; values.len()];
    create_hashes(&[values], random_state, &mut dict_hashes, false)?;

    // combine hash for each index in values
    if multi_col {
        for (hash, key) in hashes_buffer.iter_mut().zip(array.keys().iter()) {
            if let Some(key) = key {
                *hash = combine_hashes(dict_hashes[key.as_usize()], *hash)
            } // no update for Null, consistent with other hashes
        }
    } else {
        for (hash, key) in hashes_buffer.iter_mut().zip(array.keys().iter()) {
            if let Some(key) = key {
                *hash = dict_hashes[key.as_usize()]
            } // no update for Null, consistent with other hashes
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{StringArray, UInt64Array};
    use arrow_array::StructArray;
    use arrow_schema::{Field, Fields};

    use super::*;

    #[test]
    fn test_hash_uint64() {
        let mut hasher = Hasher::default();
        let array = UInt64Array::from(vec![Some(5), None, Some(8), Some(5), None]);
        let array: &dyn Array = &array;

        let hashes = hasher.hash_array(array).unwrap();
        assert_eq!(hashes[0], hashes[3]);
        assert_eq!(hashes[1], hashes[4]);
        assert_ne!(hashes[0], hashes[1]);
        assert_ne!(hashes[0], hashes[2]);
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
        let mut hasher = Hasher::default();
        let array: &dyn Array = &array;

        let hashes = hasher.hash_array(array).unwrap();
        assert_eq!(hashes[0], hashes[3]);
        assert_eq!(hashes[1], hashes[4]);
        assert_ne!(hashes[0], hashes[1]);
        assert_ne!(hashes[0], hashes[2]);
    }

    #[test]
    fn test_hash_string_stability() {
        let mut hasher = Hasher::default();
        let array = StringArray::from(vec![Some("hello"), None, Some("world")]);
        let array: &dyn Array = &array;

        let hashes = hasher.hash_array(array).unwrap();
        assert_eq!(hashes, &[1472103086483932002, 0, 8057155968893317866]);
    }

    #[test]
    fn test_hash_struct() {
        let mut hasher = Hasher::default();
        let n_array = UInt64Array::from(vec![Some(5), None, Some(8), Some(5), Some(5), None]);
        let s_array = StringArray::from(vec![
            Some("hello"),
            None,
            Some("world"),
            Some("hello"),
            Some("world"),
            None,
        ]);
        let array = StructArray::new(
            Fields::from(vec![
                Field::new("n", n_array.data_type().clone(), true),
                Field::new("s", s_array.data_type().clone(), true),
            ]),
            vec![Arc::new(n_array), Arc::new(s_array)],
            None,
        );
        let array: &dyn Array = &array;

        let hashes = hasher.hash_array(array).unwrap();
        // 0: ("hello", 5)
        // 1: (null, null)
        // 2: ("world", 8)
        // 3: ("hello", 5)
        // 4: ("world", 5)
        // 5: (null, null)

        assert_eq!(hashes[0], hashes[3]);
        assert_eq!(hashes[1], hashes[5]);
        assert_ne!(hashes[0], hashes[1]);
        assert_ne!(hashes[0], hashes[2]);
        assert_ne!(hashes[2], hashes[4]);
        assert_ne!(hashes[3], hashes[4]);
        assert_ne!(hashes[4], hashes[5]);
    }
}
