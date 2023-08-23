use crate::ValueRef;
use anyhow::Context;
use arrow::array::{
    as_boolean_array, as_largestring_array, as_primitive_array, as_string_array, Array,
    ArrayAccessor, ArrayRef, Int32Array, MapArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::downcast_primitive_array;
use arrow_schema::DataType;
use itertools::Itertools;
use std::sync::Arc;

use crate::{Evaluator, EvaluatorFactory, StaticInfo};

/// Evaluator for `get` on maps.
#[derive(Debug)]
pub(in crate::evaluators) struct GetEvaluator {
    map: ValueRef,
    key: ValueRef,
}

impl EvaluatorFactory for GetEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let key_type = info.args[0].data_type.clone();
        let map_type = &info.args[1].data_type;
        match map_type {
            DataType::Map(s, _) => match s.data_type() {
                DataType::Struct(fields) => {
                    anyhow::ensure!(
                        fields.len() == 2,
                        "expected 2 fields in map, saw {:?}",
                        fields
                    );
                    anyhow::ensure!(
                        fields[0].data_type() == &key_type,
                        "expected key type {:?}, saw {:?}",
                        key_type,
                        map_type
                    );
                    fields[1].data_type().clone()
                }
                other => anyhow::bail!("expected struct type in map, saw {:?}", other),
            },
            other => anyhow::bail!("expected map type, saw {:?}", other),
        };

        let (key, map) = info.unpack_arguments()?;
        Ok(Box::new(Self { map, key }))
    }
}

impl Evaluator for GetEvaluator {
    fn evaluate(&mut self, info: &dyn crate::RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let map_input = info.value(&self.map)?.map_array()?;
        let key_input = info.value(&self.key)?.array_ref()?;

        let result = map_get(&map_input, &key_input)?;
        Ok(Arc::new(result))
    }
}

/// Given a `MapArray` and `keys` array of the same length return an array of the values.
fn map_get(map: &MapArray, keys: &dyn Array) -> anyhow::Result<ArrayRef> {
    anyhow::ensure!(map.len() == keys.len());
    let indices = map_indices(map, keys)?;
    arrow::compute::take(map.values(), &indices, None).context("take in get_map")
}

/// Gets the indices in the map where the map keys match the next key in `keys`.
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

/// Generic implementation of `map_indices` for arrays implementing `ArrayAccessor`.
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

    use std::sync::Arc;

    use arrow::{
        array::{
            as_string_array, as_struct_array, BooleanArray, BooleanBuilder, Int64Array,
            Int64Builder, MapBuilder, StringArray, StringBuilder, StructArray, StructBuilder,
        },
        buffer::{BooleanBuffer, NullBuffer},
    };
    use arrow_schema::{DataType, Field, Fields};

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
        let mut map = MapBuilder::new(None, BooleanBuilder::new(), StringBuilder::new());

        // 0: { true: "world" }
        map.keys().append_value(true);
        map.values().append_value("world");
        map.append(true).unwrap();

        // 1: null
        map.append(false).unwrap();

        // 2: {}
        map.append(true).unwrap();

        // 3: { true: "earth" }
        map.keys().append_value(true);
        map.values().append_value("earth");
        map.append(true).unwrap();

        // 4: { true: "world", false: "earth" }
        map.keys().append_value(true);
        map.values().append_value("world");
        map.keys().append_value(false);
        map.values().append_value("earth");
        map.append(true).unwrap();

        let map = map.finish();

        let keys = BooleanArray::from(vec![true, true, true, false, false]);
        let actual = map_get(&map, &keys).unwrap();
        let expected = StringArray::from(vec![Some("world"), None, None, None, Some("earth")]);
        assert_eq!(as_string_array(actual.as_ref()), &expected);
    }

    #[test]
    fn test_get_string_key_struct_value() {
        let f1 = Field::new("hello", DataType::Utf8, false);
        let f2 = Field::new("hello", DataType::Int64, false);
        let fields = Fields::from(vec![f1, f2]);

        let struct_builder = StructBuilder::from_fields(fields.clone(), 5);
        let mut map = MapBuilder::new(None, StringBuilder::new(), struct_builder);

        // 0: { "hello": {"world", 1} }
        map.keys().append_value("hello");
        map.values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("world");
        map.values()
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(1);
        map.values().append(true);
        map.append(true).unwrap();

        // 1: null
        map.append(false).unwrap();

        // 2: {}
        map.append(true).unwrap();

        // 3: { "hi": {"earth", 2} }
        map.keys().append_value("hi");
        map.values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("earth");
        map.values()
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(2);
        map.values().append(true);
        map.append(true).unwrap();

        // 4: { "hello": {"world", 8}, "hi": {"earth", 10} }
        map.keys().append_value("hello");
        map.values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("world");
        map.values()
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(8);
        map.values().append(true);
        map.keys().append_value("hi");
        map.values()
            .field_builder::<StringBuilder>(0)
            .unwrap()
            .append_value("earth");
        map.values()
            .field_builder::<Int64Builder>(1)
            .unwrap()
            .append_value(10);
        map.values().append(true);
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
        let f1_expected = StringArray::from(vec![Some("world"), None, None, None, Some("earth")]);
        let f2_expected = Int64Array::from(vec![Some(1), None, None, None, Some(10)]);
        let null_buffer =
            NullBuffer::new(BooleanBuffer::from(vec![true, false, false, false, true]));
        let expected = StructArray::new(
            fields,
            vec![Arc::new(f1_expected), Arc::new(f2_expected)],
            Some(null_buffer),
        );
        assert_eq!(as_struct_array(actual.as_ref()), &expected);
    }
}
