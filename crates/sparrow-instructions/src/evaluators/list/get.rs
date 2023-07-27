use anyhow::Context;
use arrow::array::{
    as_boolean_array, as_largestring_array, as_primitive_array, as_string_array, Array,
    ArrayAccessor, ArrayRef, Int32Array, Int64Array, ListArray, MapArray,
};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::Int64Type;
use arrow::downcast_primitive_array;
use arrow_schema::DataType;
use itertools::Itertools;
use sparrow_plan::ValueRef;
use std::sync::Arc;

use crate::{Evaluator, EvaluatorFactory, StaticInfo};

/// Evaluator for `get` on lists.
///
/// Retrieves the value at the given index.
#[derive(Debug)]
pub(in crate::evaluators) struct ListGetEvaluator {
    index: ValueRef,
    list: ValueRef,
}

impl EvaluatorFactory for ListGetEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input_type = info.args[1].data_type.clone();
        match input_type {
            DataType::List(t) => anyhow::ensure!(t.data_type() == info.result_type),
            other => anyhow::bail!("expected list type, saw {:?}", other),
        };

        println!("info: {:?}", info);

        let (index, list) = info.unpack_arguments()?;
        Ok(Box::new(Self { index, list }))
    }
}

impl Evaluator for ListGetEvaluator {
    fn evaluate(&mut self, info: &dyn crate::RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let list_input = info.value(&self.list)?.list_array()?;
        let index_input = info.value(&self.index)?.primitive_array()?;

        let result = list_get(&list_input, &index_input)?;
        Ok(Arc::new(result))
    }
}

/// Given a `ListArray` and `index` array of the same length return an array of the values.
fn list_get(list: &ListArray, indices: &Int64Array) -> anyhow::Result<ArrayRef> {
    anyhow::ensure!(list.len() == indices.len());
    let take_indices = list_indices(list, indices)?;
    arrow::compute::take(list.values(), &take_indices, None).context("take in get_map")
}

/// Gets the indices in the list where the values are at the index within each list.
fn list_indices(list: &ListArray, indices: &Int64Array) -> anyhow::Result<Int32Array> {
    let offsets = list.offsets();
    let x = accessible_array_list_indices(offsets, indices);
    println!("Take results: {:?}", x);
    Ok(x)
    // Ok(accessible_array_list_indices(offsets, indices))
}

/// Generic implementation of `map_indices` for arrays implementing `ArrayAccessor`.
fn accessible_array_list_indices(offsets: &OffsetBuffer<i32>, indices: &Int64Array) -> Int32Array {
    let mut result = Int32Array::builder(indices.len());
    let offsets = offsets.iter().map(|n| *n as usize).tuple_windows();

    println!("Indices: {:?}", indices);
    println!("Offsets: {:?}", offsets);
    'outer: for (index, (start, next)) in offsets.enumerate() {
        let list_start = 0;
        let list_end = next - start;
        // TODO: Verify the value is valid
        // send values back in and make sure it's valid at index + start
        if indices.is_valid(index) {
            println!("Index: {:?}", index);
            println!("Start: {:?}", start);
            println!("Next: {:?}", next);
            println!("List start: {:?}", list_start);
            println!("List end: {:?}", list_end);
            let index = indices.value(index) as usize;
            if index >= list_start && index < list_end {
                result.append_value((start + index) as i32);
                continue 'outer;
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
            as_string_array, as_struct_array, BooleanArray, BooleanBuilder, Int32Builder,
            Int64Array, Int64Builder, ListBuilder, MapBuilder, StringArray, StringBuilder,
            StructArray, StructBuilder,
        },
        buffer::{BooleanBuffer, NullBuffer, OffsetBuffer},
    };
    use arrow_schema::{DataType, Field, Fields};
    use itertools::Itertools;

    #[test]
    fn test_get_string_key_string_value() {
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.append_value([Some(1), Some(2), Some(3)]);
        builder.append_value([]);
        builder.append_value([None]);
        builder.append_value([Some(10), Some(8), Some(4)]);
        builder.append_value([Some(10), Some(15), Some(19), Some(123)]);

        let array = builder.finish();
        let array = Arc::new(array);
        println!("offsets: {:?}", array.offsets());
        let offsets: &OffsetBuffer<i32> = array.offsets();
        let tuples = offsets.iter().map(|n| *n as usize).tuple_windows();
        for (index, (start, end)) in tuples.enumerate() {
            println!("index: {}, start: {}, end: {}", index, start, end);
        }
        // println!("tuple windows: {:?}", tuples);
    }
}
