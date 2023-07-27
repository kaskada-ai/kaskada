use anyhow::Context;
use arrow::array::{
    as_boolean_array, as_largestring_array, as_string_array, Array, ArrayAccessor, ArrayRef,
    Int32Array, Int64Array, ListArray,
};
use arrow::buffer::OffsetBuffer;

use arrow::downcast_primitive_array;
use arrow_schema::DataType;
use itertools::Itertools;
use sparrow_plan::ValueRef;
use std::sync::Arc;

use crate::{Evaluator, EvaluatorFactory, StaticInfo};

/// Evaluator for `index` on lists.
///
/// Retrieves the value at the given index.
#[derive(Debug)]
pub(in crate::evaluators) struct IndexEvaluator {
    index: ValueRef,
    list: ValueRef,
}

impl EvaluatorFactory for IndexEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input_type = info.args[1].data_type.clone();
        match input_type {
            DataType::List(t) => anyhow::ensure!(t.data_type() == info.result_type),
            other => anyhow::bail!("expected list type, saw {:?}", other),
        };

        let (index, list) = info.unpack_arguments()?;
        Ok(Box::new(Self { index, list }))
    }
}

impl Evaluator for IndexEvaluator {
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
    let values = list.values();
    downcast_primitive_array!(
        values => {
               Ok(accessible_array_list_indices(
                offsets,
                values,
                indices
            ))
        }
        DataType::Utf8 => {
            let values = as_string_array(values);
            Ok(accessible_array_list_indices(
                offsets,
                values,
                indices
            ))
        }
        DataType::LargeUtf8 => {
            let values = as_largestring_array(values);
            Ok(accessible_array_list_indices(
                offsets,
                values,
                indices
            ))
        }
        DataType::Boolean => {
            let values = as_boolean_array(values);
            Ok(accessible_array_list_indices(
                offsets,
                values,
                indices
            ))
        }
        unsupported => {
            anyhow::bail!("unsupported list type: {:?}", unsupported)
        }
    )
}

/// Generic implementation of `list_indices` for arrays implementing `ArrayAccessor`.
fn accessible_array_list_indices<T: Send + Sync, A: ArrayAccessor<Item = T>>(
    offsets: &OffsetBuffer<i32>,
    values: A,
    indices: &Int64Array,
) -> Int32Array {
    let mut result = Int32Array::builder(indices.len());
    let offsets = offsets.iter().map(|n| *n as usize).tuple_windows();

    'outer: for (index, (start, next)) in offsets.enumerate() {
        let list_start = 0;
        let list_end = next - start;
        if indices.is_valid(index) {
            // The inner index corresponds to the index within each list.
            let inner_index = indices.value(index) as usize;
            // The outer index corresponds to the index with the flattened array.
            let outer_index = start + inner_index;
            if inner_index >= list_start && inner_index < list_end && values.is_valid(outer_index) {
                result.append_value(outer_index as i32);
                continue 'outer;
            }
        }
        result.append_null();
    }
    result.finish()
}

#[cfg(test)]
mod tests {
    use crate::evaluators::list::index::list_get;
    use arrow::array::{
        as_boolean_array, as_primitive_array, as_string_array, BooleanArray, BooleanBuilder,
        Int32Array, Int32Builder, Int64Array, ListBuilder, StringArray, StringBuilder,
    };
    use std::sync::Arc;

    #[test]
    fn test_index_primitive() {
        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.append_value([Some(1), Some(2), Some(3)]);
        builder.append_value([]);
        builder.append_value([None]);
        builder.append_value([Some(10), Some(8), Some(4)]);
        builder.append_value([Some(10), Some(15), Some(19), Some(123)]);

        let array = builder.finish();
        let array = Arc::new(array);

        let indices = Int64Array::from(vec![0, 1, 2, 0, 1]);
        let actual = list_get(&array, &indices).unwrap();
        let actual: &Int32Array = as_primitive_array(actual.as_ref());
        let expected = Int32Array::from(vec![Some(1), None, None, Some(10), Some(15)]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_index_string() {
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.append_value([Some("hello"), None, Some("world")]);
        builder.append_value([Some("apple")]);
        builder.append_value([None, Some("carrot")]);
        builder.append_value([None, Some("dog"), Some("cat")]);
        builder.append_value([Some("bird"), Some("fish")]);

        let array = builder.finish();
        let array = Arc::new(array);

        let indices = Int64Array::from(vec![0, 1, 2, 0, 1]);
        let actual = list_get(&array, &indices).unwrap();
        let actual: &StringArray = as_string_array(actual.as_ref());
        let expected = StringArray::from(vec![Some("hello"), None, None, None, Some("fish")]);
        assert_eq!(actual, &expected);
    }

    #[test]
    fn test_index_boolean() {
        let mut builder = ListBuilder::new(BooleanBuilder::new());
        builder.append_value([Some(true), None, Some(false)]);
        builder.append_value([Some(false)]);
        builder.append_value([None, Some(false)]);
        builder.append_value([None, Some(true), Some(false)]);
        builder.append_value([Some(true), Some(false)]);

        let array = builder.finish();
        let array = Arc::new(array);

        let indices = Int64Array::from(vec![0, 1, 2, 0, 1]);
        let actual = list_get(&array, &indices).unwrap();
        let actual: &BooleanArray = as_boolean_array(actual.as_ref());
        let expected = BooleanArray::from(vec![Some(true), None, None, None, Some(false)]);
        assert_eq!(actual, &expected);
    }
}
