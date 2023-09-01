use arrow_schema::{DataType, TimeUnit};
use hashbrown::HashMap;
use itertools::Itertools;

use crate::instantiate::least_upper_bound::LeastUpperBound;
use crate::instantiate::promote_concrete;
use crate::signature::TypeParameter;
use crate::{Error, FenlType, TypeConstructor, TypeVariable};

/// Collects the actual types used with each type variables.
#[derive(Default)]
pub(super) struct TypeConstraints(HashMap<TypeVariable, Vec<FenlType>>);

impl TypeConstraints {
    /// Add a constraint that the type variable includes the given type.
    fn constrain(&mut self, variable: &TypeVariable, ty: FenlType) {
        self.0.entry_ref(variable).or_default().push(ty)
    }

    fn unify_all(
        &mut self,
        parameters: &[FenlType],
        arguments: Vec<FenlType>,
    ) -> Result<(), Error> {
        debug_assert_eq!(parameters.len(), arguments.len());
        for (parameter, argument) in parameters.iter().zip(arguments) {
            self.unify_one(parameter, argument)?;
        }
        Ok(())
    }

    /// Unify the type of one parameter with the corresponding argument.
    pub fn unify_one(&mut self, parameter: &FenlType, argument: FenlType) -> Result<(), Error> {
        match (&parameter.type_constructor, &argument.type_constructor) {
            (TypeConstructor::Generic(variable), _) => {
                self.constrain(variable, argument);
                Ok(())
            }
            (TypeConstructor::Concrete(parameter_dt), TypeConstructor::Concrete(argument_dt)) => {
                if can_implicitly_cast(argument_dt, parameter_dt) {
                    Ok(())
                } else {
                    Err(Error::IncompatibleTypes {
                        expected: parameter.clone(),
                        actual: argument,
                    })
                }
            }
            (p_con, a_con) => {
                if p_con == a_con {
                    self.unify_all(&parameter.type_args, argument.type_args)?;
                    Ok(())
                } else {
                    Err(Error::IncompatibleTypes {
                        expected: parameter.clone(),
                        actual: argument,
                    })
                }
            }
        }
    }

    pub fn solve<'a>(
        self,
        parameters: impl Iterator<Item = &'a TypeParameter>,
    ) -> Result<HashMap<TypeVariable, DataType>, Error> {
        let mut types_for_variable = self.0;
        let solutions = parameters
            .filter_map(|p| {
                // This expect should never fail -- it would mean no arguments used the type variable
                // which would have been an invalid signature.
                let argument_types = types_for_variable
                    .remove(&p.name)
                    .expect("unused type variable");

                if argument_types.is_empty() {
                    None
                } else {
                    // Can't use `?` because we want Option<Result<...>>.
                    // This allows `filter_map` to filter out the `None` before the errors are
                    // collected.
                    Some(
                        solve_type_class(p, &argument_types)
                            .map(|unified| (p.name.clone(), unified)),
                    )
                }
            })
            .try_collect()?;

        // types_for_variables should be empty, since every type variable added to the map
        // should have been used in the signature (the list of parameters).
        debug_assert!(types_for_variable.is_empty(), "unreferenced type variable");
        Ok(solutions)
    }
}

/// Determine the type for a type class based on the associated argument types.
///
/// # Fails
/// If the types are empty. If no parameters use the type class (and there are
/// no associated parameters) we shouldn't try to determine the corresponding
/// type.
fn solve_type_class(parameter: &TypeParameter, types: &[FenlType]) -> Result<DataType, Error> {
    debug_assert!(!types.is_empty());

    debug_assert!(
        parameter.type_classes.len() == 1,
        "only one type class currently supported"
    );
    let type_class = parameter.type_classes[0];

    // Find the minimum type compatible with all the arguments associated with the
    // type class.
    let mut result = types[0].clone();
    for (index, arg_type) in types.iter().enumerate().skip(1) {
        result = result.least_upper_bound(arg_type).ok_or_else(|| {
            Error::IncompatibleTypesForConstraint {
                type_parameter: parameter.name.clone(),
                types: types[..=index].iter().cloned().collect(),
            }
        })?;
    }

    let result = match result.type_constructor {
        TypeConstructor::Concrete(concrete) => concrete,
        _ => panic!("Least upper bound should be concrete, but was {result:?}"),
    };

    let result = promote_concrete(result, type_class).ok_or_else(|| {
        Error::IncompatibleTypesForConstraint {
            type_parameter: parameter.name.clone(),
            types: types.iter().cloned().collect(),
        }
    })?;

    Ok(result)
}

/// Returns whether the `from` datatype can be implicitly cast to `to`.
///
/// For example, while it is possible to cast `i64` to `i32`, we don't
/// want to do that implicitly since it "narrows" the type and discards
/// values.
fn can_implicitly_cast(from: &DataType, to: &DataType) -> bool {
    use DataType::*;
    match (from, to) {
        (_, _) if from == to => true,
        // Null can be cast to a null version of any other type.
        (Null, _) => true,
        // Integers can be widened or promoted to Float64.
        (Int8, Int8 | Int16 | Int32 | Int64 | Float64) => true,
        (Int16, Int16 | Int32 | Int64 | Float64) => true,
        (Int32, Int32 | Int64 | Float64) => true,
        (Int64, Int64 | Float64) => true,
        // Unsigned integers can be widened or promoted to larger Int or Float64
        (UInt8, UInt8 | UInt16 | UInt32 | UInt64 | Int16 | Int32 | Int64 | Float64) => true,
        (UInt16, UInt16 | UInt32 | UInt64 | Int32 | Int64 | Float64) => true,
        (UInt32, UInt32 | UInt64 | Int64 | Float64) => true,
        (UInt64, UInt64 | Float64) => true,
        // Floats can be widened.
        (Float16, Float16 | Float32 | Float64) => true,
        (Float32, Float32 | Float64) => true,
        (Float64, Float64) => true,
        // Other promotions that we allow implicitly.
        (Utf8, Timestamp(TimeUnit::Nanosecond, None)) => true,
        (Utf8, LargeUtf8) => true,
        (Timestamp(_, _), Timestamp(TimeUnit::Nanosecond, None)) => true,
        // Other promotions must be explicitly requested.
        (_, _) => false,
    }
}
