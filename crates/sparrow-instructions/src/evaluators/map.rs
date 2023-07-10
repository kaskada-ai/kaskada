use std::sync::Arc;

use anyhow::{anyhow, Context};
use arrow::array::{Array, ArrayRef, PrimitiveArray, StringArray};
use arrow::datatypes::{DataType, Int64Type};
use owning_ref::ArcRef;
use sparrow_arrow::downcast::{downcast_primitive_array, downcast_string_array};
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_plan::ValueRef;

use crate::{Evaluator, EvaluatorFactory, StaticInfo};

/// Evaluator for `get` on maps.
#[derive(Debug)]
pub struct GetEvaluator {
    map: ValueRef,
    key: ValueRef,
    key_type: DataType,
    value_type: DataType,
}

impl EvaluatorFactory for GetEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let map_type = &info.args[0].data_type;
        let key_type = info.args[1].data_type.clone();
        let value_type = match map_type {
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

        let (map, key) = info.unpack_arguments()?;
        Ok(Box::new(Self {
            map,
            key,
            key_type,
            value_type,
        }))
    }
}

impl Evaluator for GetEvaluator {
    fn evaluate(&mut self, info: &dyn crate::RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let map_input = info.value(&self.map)?.map_array()?;
        let key_input = info.value(&self.key)?;

        // TODO: must specialize for ALL TYPES instead of this match.
        let results: PrimitiveArray<Int64Type> = match &self.key_type {
            DataType::Utf8 => {
                let key_input: ArcRef<dyn Array, StringArray> =
                    info.value(&self.key)?.string_array()?;
                // Keys in a map ~should~ be unique..but I'm not sure arrow enforces this. Test?
                // does the map array have a different length because all the rows are represented
                // as structs {key,value}, with no regard to whether they're in the same row?
                anyhow::ensure!(
                    key_input.len() == map_input.len(),
                    "key and map lengths don't match"
                );

                // TODO: Can probably more efficiently iterate by using the `value_offsets`

                let mut results = vec![];
                for i in 0..key_input.len() {
                    let cur_key = key_input.value(i);
                    let cur_map = map_input.value(i);
                    debug_assert!(cur_map.fields().len() == 2);

                    // Iterate through map_entry, match on key, return value.
                    let map_keys: &StringArray = downcast_string_array(cur_map.column(0).as_ref())?;

                    let value_pos = map_keys.iter().position(|x| x == Some(cur_key));
                    let values: &PrimitiveArray<Int64Type> =
                        downcast_primitive_array(cur_map.column(1).as_ref())?;

                    if let Some(value_pos) = value_pos {
                        if values.is_null(value_pos) {
                            results.push(None);
                        } else {
                            results.push(Some(values.value(value_pos)));
                        }
                    } else {
                        results.push(None)
                    }
                }

                unsafe { PrimitiveArray::from_trusted_len_iter(results.iter()) }
            }
            _ => panic!("asdf"),
        };

        Ok(Arc::new(results))
    }
}

mod tests {
    use arrow::array::{Int32Array, Int32Builder, Int64Array, MapBuilder, StringBuilder};
    use sparrow_arrow::downcast::downcast_primitive_array;

    use super::*;

    #[test]
    fn test() {
        let string_builder = StringBuilder::new();
        let int_builder = Int32Builder::with_capacity(4);

        let mut builder = MapBuilder::new(None, string_builder, int_builder);

        builder.keys().append_value("joe");
        builder.values().append_value(1);
        builder.append(true).unwrap();

        builder.append(false).unwrap();

        builder.keys().append_value("joe");
        builder.values().append_value(2);
        builder.keys().append_value("bob");
        builder.values().append_value(4);
        builder.keys().append_value("joe");
        builder.values().append_value(1);
        builder.append(true).unwrap();
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        let array = builder.finish();
        println!("Array: {:?}", array);
        println!("len: {}", array.len());
        println!("entries: {:?}", array.entries());
        println!("entries: {:?}", array.entries().len());
        println!("keys: {:?}", array.keys());
        println!("values: {:?}", array.values());
        println!("offsets: {:?}", array.offsets());
        println!("value offsets: {:?}", array.value_offsets());
        for i in 0..array.len() {
            println!("Value at {}: {:?}", i, array.value(i));
        }
        let v: &Int32Array = downcast_primitive_array(array.values().as_ref()).unwrap();
        println!("Value at 2: {:?}", v.value(2));
    }
}
