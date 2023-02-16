//! The field ref instruction isn't a "normal" instruction since the result
//! type depends on the type of input.

use anyhow::{anyhow, Context};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use sparrow_core::ScalarValue;
use sparrow_plan::ValueRef;

use crate::{Evaluator, EvaluatorFactory, StaticInfo};

#[derive(Debug)]
pub struct FieldRefEvaluator {
    base: ValueRef,
    field_index: usize,
}

impl EvaluatorFactory for FieldRefEvaluator {
    fn try_new(info: StaticInfo<'_>) -> anyhow::Result<Box<dyn Evaluator>> {
        let input_type = &info.args[0].data_type;

        let field_name = &info.args[1].value_ref;
        let field_name =
            if let Some(ScalarValue::Utf8(Some(field_name))) = field_name.literal_value() {
                field_name
            } else {
                anyhow::bail!(
                    "Unable to create FieldRefEvaluator with unexpected field name {:?}",
                    field_name
                );
            };

        let base_fields = if let DataType::Struct(fields) = input_type {
            fields
        } else {
            anyhow::bail!(
                "Unable to create FieldRefEvaluator for input type {:?}",
                &info.args[0].data_type
            );
        };

        let field_index = base_fields
            .iter()
            .position(|field| field.name() == field_name)
            .ok_or_else(|| anyhow!("No field named '{}' in struct {:?}", field_name, input_type))?;

        let (base, _) = info.unpack_arguments()?;
        Ok(Box::new(Self { base, field_index }))
    }
}

impl Evaluator for FieldRefEvaluator {
    fn evaluate(&mut self, info: &dyn crate::RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.base)?.struct_array()?;

        let field_array = input.column(self.field_index);

        // Arrow field ref ignores the null-ness of the outer struct.
        // To handle this, we null the field out if the struct was null.
        // TODO: Express in the DFG so we can avoid computing `is_not_null` repeatedly?
        let is_struct_null = arrow::compute::kernels::boolean::is_null(input.as_ref())?;
        // We need to null out rows that are null in the struct, since
        // they may not be properly nulled-out in the field column.
        arrow::compute::nullif(field_array.as_ref(), &is_struct_null)
            .context("null_if for field ref")
    }
}
