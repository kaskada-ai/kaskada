use std::sync::Arc;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

use crate::ValueRef;
use arrow::array::{Array, ArrayRef, AsArray, BufferBuilder, ListArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::row::{RowConverter, SortField};
use arrow_schema::{DataType, FieldRef};
use hashbrown::HashSet;
use itertools::Itertools;

/// Evaluator for the `union` instruction.
#[derive(Debug)]
pub struct UnionEvaluator {
    a: ValueRef,
    b: ValueRef,
    field: FieldRef,
    row_converter: RowConverter,
}

impl EvaluatorFactory for UnionEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        anyhow::ensure!(info.args[0].data_type() == info.args[1].data_type());
        let DataType::List(field) = info.args[0].data_type() else {
            anyhow::bail!(
                "Unable to union non-list type {:?}",
                info.args[0].data_type()
            )
        };
        let field = field.clone();
        let row_converter = RowConverter::new(vec![SortField::new(field.data_type().clone())])?;
        let (a, b) = info.unpack_arguments()?;

        Ok(Box::new(Self {
            a,
            b,
            field,
            row_converter,
        }))
    }
}

impl Evaluator for UnionEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let a = info.value(&self.a)?.array_ref()?;
        let b = info.value(&self.b)?.array_ref()?;
        assert_eq!(a.len(), b.len());

        union(
            self.field.clone(),
            &mut self.row_converter,
            a.as_ref(),
            b.as_ref(),
        )
    }
}

fn union(
    field: FieldRef,
    row_converter: &mut RowConverter,
    a: &dyn Array,
    b: &dyn Array,
) -> anyhow::Result<ArrayRef> {
    let a_list: &ListArray = a.as_list();
    let b_list: &ListArray = b.as_list();

    let mut offsets = BufferBuilder::new(a_list.len() + 1);

    let mut indices = Vec::with_capacity(a_list.values().len() + b_list.values().len());

    let mut offset = 0u32;
    offsets.append(offset);

    let mut included = HashSet::new();
    let a_offsets = a_list
        .value_offsets()
        .iter()
        .map(|n| *n as usize)
        .tuple_windows();
    let b_offsets = b_list
        .value_offsets()
        .iter()
        .map(|n| *n as usize)
        .tuple_windows();
    for (index, ((a_start, a_end), (b_start, b_end))) in a_offsets.zip(b_offsets).enumerate() {
        let a_len = a_end - a_start;
        let b_len = b_end - b_start;

        if a_len == 0 && b_len == 0 {
            // Nothing to do
        } else if a_len == 0 {
            // Only need to take from b.
            offset += b_len as u32;
            indices.extend((b_start..b_end).map(|n| (1, n)));
        } else if b_len == 0 {
            // Only need to take from a.
            offset += a_len as u32;
            indices.extend((a_start..a_end).map(|n| (0, n)));
        } else {
            let a_rows = row_converter.convert_columns(&[a_list.value(index)])?;
            let b_rows = row_converter.convert_columns(&[b_list.value(index)])?;

            // INEFFICIENT: This currently copies the row into an owned vec to put
            // in the hash set for deduplication. We'd likely be better off
            // keeping the rows and usin their identity to do the comparison.
            // This would require figuring out a way to setup a map that *didn't*
            // try to drop the rows (eg., just stored references).
            for (a_index, a_row) in a_rows.iter().enumerate() {
                if included.insert(a_row.owned()) {
                    offset += 1;
                    indices.push((0, a_start + a_index));
                }
            }
            for (b_index, b_row) in b_rows.iter().enumerate() {
                if included.insert(b_row.owned()) {
                    offset += 1;
                    indices.push((1, b_start + b_index));
                }
            }
            included.clear();
        }
        offsets.append(offset);
    }

    let values = arrow::compute::interleave(
        &[a_list.values().as_ref(), b_list.values().as_ref()],
        &indices,
    )?;

    let offsets = offsets.finish();
    let offsets = ScalarBuffer::new(offsets, 0, a_list.len() + 1);
    let offsets = OffsetBuffer::new(offsets);
    let result = ListArray::new(field, offsets, values, None);

    Ok(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int32Array, ListBuilder};
    use arrow::row::{RowConverter, SortField};
    use arrow_schema::{DataType, Field};

    #[test]
    fn test_union_list() {
        let field = Arc::new(Field::new("item", DataType::Int32, true));

        let mut a = ListBuilder::new(Int32Array::builder(8));
        let mut b = ListBuilder::new(Int32Array::builder(8));
        let mut expected = ListBuilder::new(Int32Array::builder(12));

        //0: [ 5 ] + [] = [5]
        a.append_value([Some(5)]);
        b.append_value([]);
        expected.append_value([Some(5)]);

        // 1: [ ] + [5, 6] = [5, 6]
        a.append_value([]);
        b.append_value([Some(5), Some(6)]);
        expected.append_value([Some(5), Some(6)]);

        // 2: [] + null = []
        a.append_value([]);
        b.append_null();
        expected.append_value([]);

        // 3: [6, 7] + [6, 7, 8] = [6, 7, 8]
        a.append_value([Some(6), Some(7)]);
        b.append_value([Some(6), Some(7), Some(8)]);
        expected.append_value([Some(6), Some(7), Some(8)]);

        // 4: [7, null, 7, 9] + [8, 9, 10, 8] = [7, null, 9, 8, 10]
        a.append_value([Some(7), None, Some(7), Some(9)]);
        b.append_value([Some(8), Some(9), Some(10), Some(8)]);
        expected.append_value([Some(7), None, Some(9), Some(8), Some(10)]);

        let mut row_convertor = RowConverter::new(vec![SortField::new(DataType::Int32)]).unwrap();
        let actual = super::union(field, &mut row_convertor, &a.finish(), &b.finish()).unwrap();

        let expected: ArrayRef = Arc::new(expected.finish());
        assert_eq!(&actual, &expected);
    }
}
