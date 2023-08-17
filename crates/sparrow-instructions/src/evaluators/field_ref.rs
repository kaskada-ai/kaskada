//! The field ref instruction isn't a "normal" instruction since the result
//! type depends on the type of input.

use std::sync::Arc;

use crate::ValueRef;
use anyhow::anyhow;
use arrow::array::{make_array, Array, ArrayRef, AsArray, ListArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType;
use arrow_schema::{Field, FieldRef};
use sparrow_arrow::scalar_value::ScalarValue;

use crate::{Evaluator, EvaluatorFactory, StaticInfo};

#[derive(Debug)]
pub struct FieldRefEvaluator {
    base: ValueRef,
    field_index: usize,
    field: FieldRef,
}

fn get_field_index(field_name: &str, base: &DataType) -> anyhow::Result<(usize, FieldRef)> {
    let (base_fields, name) = if let DataType::Struct(fields) = base {
        (fields, None)
    } else if let DataType::List(field) = base {
        if let DataType::Struct(fields) = field.data_type() {
            (fields, Some("item"))
        } else {
            anyhow::bail!("Field-ref only works on lists of records, but was {base:?}");
        }
    } else {
        anyhow::bail!(
            "Unable to create FieldRefEvaluator for input type {:?}",
            base
        );
    };

    let (index, field) = base_fields
        .find(field_name)
        .ok_or_else(|| anyhow!("No field named '{}' in struct {:?}", field_name, base))?;

    // We can't re-use the field if it is part of a collection, which needs special names.
    let field = if let Some(name) = name {
        Arc::new(Field::new(name, field.data_type().clone(), true))
    } else {
        field.clone()
    };
    Ok((index, field))
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

        let (field_index, field) = get_field_index(field_name, input_type)?;
        let (base, _) = info.unpack_arguments()?;
        Ok(Box::new(Self {
            base,
            field_index,
            field,
        }))
    }
}

impl Evaluator for FieldRefEvaluator {
    fn evaluate(&mut self, info: &dyn crate::RuntimeInfo) -> anyhow::Result<ArrayRef> {
        let input = info.value(&self.base)?.array_ref()?;

        match input.data_type() {
            DataType::Struct(_) => {
                let input = input.as_struct();
                let field = input.column(self.field_index);

                // Arrow field ref ignores the null-ness of the outer struct.
                // To handle this, we null the field out if the struct was null.
                let nulls = NullBuffer::union(input.nulls(), field.nulls());

                let data = field.to_data().into_builder().nulls(nulls).build()?;
                Ok(make_array(data))
            }
            DataType::List(_) => {
                let list = input.as_list();
                let structs = list.values().as_struct();
                let fields = structs.column(self.field_index);

                // Arrow field ref ignores the null-ness of the outer struct.
                // To handle this, we null the field out if the struct was null.
                let nulls = NullBuffer::union(structs.nulls(), fields.nulls());

                let values = make_array(fields.to_data().into_builder().nulls(nulls).build()?);
                let list = ListArray::new(
                    self.field.clone(),
                    list.offsets().clone(),
                    values,
                    list.nulls().cloned(),
                );
                Ok(Arc::new(list))
            }
            unsupported => anyhow::bail!("Unsupported input for field ref: {unsupported:?}"),
        }
    }
}
