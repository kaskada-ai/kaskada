use crate::{Error, ExprRef};
use arrow_schema::DataType;
use error_stack::ResultExt;
use hashbrown::HashMap;
use itertools::Itertools;
use sparrow_types::{Signature, Types};

/// Type-check the given function name.
pub(crate) fn typecheck(name: &str, args: &[ExprRef]) -> error_stack::Result<Types, Error> {
    match name {
        "fieldref" => {
            error_stack::ensure!(
                args.len() == 2,
                Error::internal(format!("invalid arguments ({}) for fieldref", args.len()))
            );

            let DataType::Struct(fields) = &args[0].result_type else {
                error_stack::bail!(Error::InvalidNonStructType(args[0].result_type.clone()))
            };
            let Some(name) = args[1].literal_str_opt() else {
                error_stack::bail!(Error::InvalidNonStringLiteral(args[0].clone()))
            };

            let Some((_, field)) = fields.find(name) else {
                error_stack::bail!(Error::InvalidFieldName {
                    name: name.to_owned(),
                })
            };
            let types = Types {
                arguments: vec![args[0].result_type.clone(), DataType::Utf8],
                result: field.data_type().clone(),
            };
            Ok(types)
        }
        "record" => {
            error_stack::ensure!(
                !args.is_empty() && args.len() % 2 == 0,
                Error::internal(format!(
                    "expected non-zero, even number of arguments, was {}",
                    args.len()
                ))
            );

            let mut arguments = Vec::with_capacity(args.len());
            let mut fields = Vec::with_capacity(args.len() / 2);
            for (name, field) in args.iter().tuples() {
                let Some(name) = name.literal_str_opt() else {
                    error_stack::bail!(Error::InvalidNonStringLiteral(args[0].clone()))
                };

                arguments.push(DataType::Utf8);
                arguments.push(field.result_type.clone());
                fields.push(arrow_schema::Field::new(
                    name,
                    field.result_type.clone(),
                    true,
                ));
            }

            let types = Types {
                arguments,
                result: DataType::Struct(fields.into()),
            };
            Ok(types)
        }
        _ => {
            let signature = get_signature(name)?;
            // TODO: Ideally, instantiate would accept references so we didn't need to clone.
            let arguments = args.iter().map(|arg| arg.result_type.clone()).collect();
            let result_type = signature
                .instantiate(arguments)
                .change_context(Error::InvalidTypes)?;
            Ok(result_type)
        }
    }
}

#[derive(serde::Deserialize, Debug)]
struct Function {
    name: &'static str,
    signature: Signature,
}

#[static_init::dynamic]
static FUNCTION_SIGNATURES: HashMap<&'static str, &'static Function> = {
    let functions: Vec<Function> =
        serde_yaml::from_str(include_str!("functions.yml")).expect("failed to parse functions.yml");

    functions
        .into_iter()
        .map(|function| {
            let function: &'static Function = Box::leak(Box::new(function));
            (function.name, function)
        })
        .collect()
};

fn get_signature(name: &str) -> error_stack::Result<&'static Signature, Error> {
    let function = FUNCTION_SIGNATURES
        .get(name)
        .ok_or_else(|| Error::InvalidFunction(name.to_owned()))?;
    Ok(&function.signature)
}
