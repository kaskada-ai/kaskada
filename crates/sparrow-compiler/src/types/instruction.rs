use std::borrow::Cow;

use anyhow::{anyhow, Context};
use arrow::datatypes::{DataType, Field};
use itertools::{izip, Itertools};
use sparrow_arrow::scalar_value::ScalarValue;
use sparrow_instructions::CastEvaluator;
use sparrow_plan::{InstKind, Mode};
use sparrow_syntax::{ArgVec, FenlType, Resolved};

use crate::types::inference::validate_instantiation;

/// Typecheck the given instruction against the argument types.
///
/// This doesn't perform type inference, instead it just validates
/// that the argument types are a valid instantiation of the instruction.
///
/// This returns the (instantiated) result type.
pub(crate) fn typecheck_inst(
    inst: &InstKind,
    argument_types: ArgVec<FenlType>,
    argument_literals: &[Option<ScalarValue>],
    mode: Mode,
) -> anyhow::Result<FenlType> {
    match inst {
        InstKind::Simple(instruction) => {
            let signature = instruction.signature(mode);
            let argument_types = Resolved::new(
                Cow::Borrowed(signature.parameters().names()),
                argument_types,
                signature.parameters().has_vararg,
            );

            validate_instantiation(&argument_types, signature)
        }
        InstKind::FieldRef => {
            anyhow::ensure!(
                argument_types.len() == 2,
                "Expected 2 arguments for field-ref, but was {:?}",
                argument_types
            );

            if argument_types[0].is_error() || argument_types[1].is_error() {
                return Ok(FenlType::Error);
            };

            let field_name =
                if let Some(ScalarValue::Utf8(Some(field_name))) = &argument_literals[1] {
                    field_name
                } else {
                    anyhow::bail!(
                        "Unable to get literal string for field name {:?}",
                        argument_literals[1]
                    );
                };

            if let FenlType::Concrete(DataType::Struct(fields)) = &argument_types[0] {
                // TODO: Handle nullability?
                let result_type = fields
                    .iter()
                    .find_map(|field| {
                        if field.name() == field_name {
                            Some(FenlType::Concrete(field.data_type().clone()))
                        } else {
                            None
                        }
                    })
                    .with_context(|| {
                        format!(
                            "No field named '{}' found in struct {:?}",
                            field_name, argument_types[0]
                        )
                    })?;

                Ok(result_type)
            } else {
                Err(anyhow!(
                    "Unable to access field {} of type {:?}",
                    field_name,
                    argument_types[0]
                ))
            }
        }
        InstKind::Cast(data_type) => {
            anyhow::ensure!(
                argument_types.len() == 1,
                "Expected 1 argument for field-ref, but was {:?}",
                argument_types
            );

            anyhow::ensure!(
                CastEvaluator::is_supported_fenl(
                    &argument_types[0],
                    &FenlType::Concrete(data_type.clone()),
                ),
                "Unable to cast from {:?} to {:?}",
                &argument_types[0],
                data_type
            );

            Ok(FenlType::Concrete(data_type.clone()))
        }
        InstKind::Record => {
            anyhow::ensure!(
                argument_types.len() % 2 == 0,
                "Expected even number of arguments (field name and field value), but was {:?}",
                argument_types.len(),
            );

            if argument_types.iter().contains(&FenlType::Error) {
                return Ok(FenlType::Error);
            }

            // 0, 2, 4, 6, ... = the field names. We need their literals.
            let field_names = argument_literals.iter().step_by(2);
            // 1, 3, 5, 7, ... = the field values. We need their types.
            let field_values = argument_types.iter().skip(1).step_by(2);

            let mut fields = Vec::with_capacity(argument_types.len() / 2);
            for (field_name, field_type) in izip!(field_names, field_values) {
                let field_name = if let Some(ScalarValue::Utf8(Some(field_name))) = field_name {
                    field_name
                } else {
                    anyhow::bail!(
                        "Unable to get literal string for field name {:?}",
                        field_name
                    );
                };

                let field_type = field_type.arrow_type().with_context(|| {
                    format!("Expected field '{field_name}' to have concrete type")
                })?;

                fields.push(Field::new(field_name, field_type.clone(), true))
            }

            let result_type = DataType::Struct(fields);
            Ok(FenlType::Concrete(result_type))
        }
    }
}
