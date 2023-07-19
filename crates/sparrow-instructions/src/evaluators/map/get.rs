use anyhow::Context;
use arrow::array::{
    as_boolean_array, as_largestring_array, as_primitive_array, as_string_array, Array,
    ArrayAccessor, ArrayRef, Int32Array, MapArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::downcast_primitive_array;
use arrow_schema::DataType;
use itertools::Itertools;

/// Given a `MapArray` and `keys` array of the same length return an array of the values.
fn map_get(map: &MapArray, keys: &dyn Array) -> anyhow::Result<ArrayRef> {
    anyhow::ensure!(map.len() == keys.len());
    let indices = map_indices(map, keys)?;
    arrow::compute::take(map.values(), &indices, None).context("take in get_map")
}

fn map_indices(map: &MapArray, keys: &dyn Array) -> anyhow::Result<Int32Array> {
    anyhow::ensure!(
        map.key_type() == keys.data_type(),
        "Expected map keys {} to be same type as keys {}",
        map.key_type(),
        keys.data_type()
    );

    let offsets = map.offsets();

    downcast_primitive_array!(
        keys => {
            let map_keys = as_primitive_array(map.keys());
            Ok(accessible_array_map_indices(offsets, map_keys, keys))
        }
        DataType::Utf8 => {
            let keys = as_string_array(keys);
            let map_keys = as_string_array(map.keys());
            Ok(accessible_array_map_indices(offsets, map_keys, keys))
        }
        DataType::LargeUtf8 => {
            let keys = as_largestring_array(keys);
            let map_keys = as_largestring_array(map.keys());
            Ok(accessible_array_map_indices(offsets, map_keys, keys))
        }
        DataType::Boolean => {
            let keys = as_boolean_array(keys);
            let map_keys = as_boolean_array(map.keys());
            Ok(accessible_array_map_indices(offsets, map_keys, keys))
        }
        unsupported => {
            anyhow::bail!("Unsupported key type {:?} for map get", unsupported);
        }
    )
}

/// Generic implementation of `map_indices` for arrays impelmenting `ArrayAccessor`.
fn accessible_array_map_indices<T: PartialEq, A: ArrayAccessor<Item = T>>(
    offsets: &OffsetBuffer<i32>,
    map_keys: A,
    keys: A,
) -> Int32Array {
    let mut result = Int32Array::builder(keys.len());
    let offsets = offsets.iter().map(|n| *n as usize).tuple_windows();
    'outer: for (index, (start, next)) in offsets.enumerate() {
        if keys.is_valid(index) {
            let key = keys.value(index);
            for index in start..next {
                if map_keys.value(index) == key && map_keys.is_valid(index) {
                    result.append_value(index as i32);
                    continue 'outer;
                }
            }
        }
        result.append_null();
    }
    result.finish()
}

#[cfg(test)]
mod tests {

    use arrow::array::{as_string_array, MapBuilder, StringArray, StringBuilder};

    use crate::evaluators::map::get::map_get;

    #[test]
    fn test_get_string_key_string_value() {
        let mut map = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());

        // 0: { "hello": "world" }
        map.keys().append_value("hello");
        map.values().append_value("world");
        map.append(true).unwrap();

        // 1: null
        map.append(false).unwrap();

        // 2: {}
        map.append(true).unwrap();

        // 3: { "hi": "earth" }
        map.keys().append_value("hi");
        map.values().append_value("earth");
        map.append(true).unwrap();

        // 4: { "hello": "world", "hi": "earth" }
        map.keys().append_value("hello");
        map.values().append_value("world");
        map.keys().append_value("hi");
        map.values().append_value("earth");
        map.append(true).unwrap();

        let map = map.finish();

        let keys = StringArray::from(vec![
            Some("hello"),
            None,
            Some("hello"),
            Some("hello"),
            Some("hi"),
        ]);
        let actual = map_get(&map, &keys).unwrap();
        let expected = StringArray::from(vec![Some("world"), None, None, None, Some("earth")]);
        assert_eq!(as_string_array(actual.as_ref()), &expected);
    }

    #[test]
    fn test_get_bool_key_string_value() {
        todo!()
    }

    #[test]
    fn test_get_bool_key_struct_value() {
        todo!()
    }
}
