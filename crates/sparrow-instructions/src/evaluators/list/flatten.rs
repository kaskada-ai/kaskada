use std::sync::Arc;

use crate::{Evaluator, EvaluatorFactory, RuntimeInfo, StaticInfo};

use crate::ValueRef;
use arrow::array::{Array, ArrayRef, AsArray, BufferBuilder, ListArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};

/// Evaluator for the `flatten` instruction.
#[derive(Debug)]
pub struct FlattenEvaluator {
    input: ValueRef,
    field: Arc<Field>,
}

impl EvaluatorFactory for FlattenEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let outer = info.args[0].data_type();
        let inner = match outer {
            DataType::List(field) => field.data_type(),
            _ => {
                anyhow::bail!("Input to flatten must be a list of lists, was {outer:?}.")
            }
        };
        let field = match inner {
            DataType::List(field) => field.clone(),
            _ => {
                anyhow::bail!("Input to flatten must be a list of lists, was {outer:?}.")
            }
        };

        let input = info.unpack_argument()?;
        Ok(Box::new(Self { input, field }))
    }
}

impl Evaluator for FlattenEvaluator {
    fn evaluate(&mut self, info: &dyn RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.input)?.array_ref()?;

        let outer_list: &ListArray = input.as_list();
        let inner_list: &ListArray = outer_list.values().as_list();

        let len = outer_list.len();
        let mut offsets = BufferBuilder::new(len + 1);

        let mut outer_offsets = outer_list.offsets().iter().copied();
        let inner_offsets = inner_list.offsets();
        offsets.append(inner_offsets[outer_offsets.next().unwrap() as usize]);
        for offset in outer_offsets {
            // we know that the outer list contains the inner lists from [start..end).
            // we also know that each inner list contains elements
            //  [inner_offset(i)..inner_offset(i + 1))
            // this we want elements
            offsets.append(inner_offsets[offset as usize]);
        }
        let offsets = offsets.finish();
        let offsets = ScalarBuffer::new(offsets, 0, len + 1);
        let offsets = OffsetBuffer::new(offsets);

        let result = ListArray::new(
            self.field.clone(),
            offsets,
            inner_list.values().clone(),
            outer_list.nulls().cloned(),
        );
        Ok(Arc::new(result))
    }
}
