//! Utilities for working with argument and parameter types.

use std::cmp::Ordering;

use arrow::datatypes::{DataType, Field, TimeUnit};
use hashbrown::hash_map::Entry;
use hashbrown::HashMap;
use itertools::{izip, Itertools};
use sparrow_syntax::{FenlType, Located, Resolved, Signature, TypeClass, TypeVariable};

use crate::{DiagnosticBuilder, DiagnosticCode};

/// Instantiate arguments to a (possibly) polymorphic signature.
///
/// Returns the instantiated argument and return types.
pub(crate) fn instantiate(
    call: &Located<String>,
    arguments: &Resolved<Located<FenlType>>,
    signature: &Signature,
) -> Result<(Resolved<FenlType>, FenlType), DiagnosticBuilder> {
    let parameters = signature.parameters();
    debug_assert_eq!(
        arguments.names(),
        parameters.names(),
        "Arguments being instantiated should have been resolved against the signature"
    );

    let mut types_for_variable: HashMap<TypeVariable, Vec<&Located<FenlType>>> = HashMap::new();

    // Make sure the number of arguments are correct.
    match arguments.len().cmp(&parameters.types().len()) {
        Ordering::Less => {
            return Err(DiagnosticCode::InvalidArguments.builder().with_label(
                call.location().primary_label().with_message(format!(
                    "Not enough arguments for call to '{}'. Expected {} but got {}.",
                    call,
                    parameters.types().len(),
                    arguments.len()
                )),
            ));
        }
        Ordering::Greater if !arguments.has_vararg => {
            return Err(DiagnosticCode::InvalidArguments.builder().with_label(
                call.location().primary_label().with_message(format!(
                    "Too many arguments for call to '{}'. Expected {} but got {}.",
                    call,
                    parameters.types().len(),
                    arguments.len()
                )),
            ))
        }
        Ordering::Equal | Ordering::Greater => (),
    }

    // Repeat the last parameter type, in case this is a vararg function.
    // If it isn't a vararg function, then the `izip` will stop after the regular
    // occurrence, so this is effectively a noop.
    //
    // If it is a vararg function, then the extra arguments will be paired
    // up with these repetitions.
    //
    // We need to do it this way since there needs to be a single type (or
    // we'd need to duplicate the code for the iterator with and without th
    // chain).
    let parameter_types = arguments.vec_for_varargs(parameters.types());

    for (index, (argument_type, parameter_type)) in
        izip!(arguments.iter(), &parameter_types).enumerate()
    {
        match (parameter_type.inner(), argument_type.inner()) {
            (FenlType::TypeRef(type_var), _) => {
                types_for_variable
                    .entry(type_var.clone())
                    .or_default()
                    .push(argument_type);
            }
            (_, FenlType::Error) => {
                // No problem here -- the argument is an error, but we already
                // reported it. Don't hide the actual error.
            }
            (FenlType::Concrete(expected), FenlType::Concrete(arg_type))
                if can_implicitly_cast(arg_type, expected) =>
            {
                // No problem -- either same type or can implicitly convert.
            }
            (FenlType::Window, FenlType::Window | FenlType::Concrete(DataType::Null)) => {
                // No problem -- can use `null` as a window.
            }
            _ => {
                return Err(DiagnosticCode::InvalidArgumentType
                    .builder()
                    .with_label(call.location().primary_label().with_message(format!(
                        "Invalid types for parameter '{}' in call to '{}'",
                        arguments.parameter_name(index),
                        call
                    )))
                    .with_label(
                        argument_type
                            .location()
                            .secondary_label()
                            .with_message(format!("Actual type: {argument_type}")),
                    )
                    .with_label(
                        parameter_type
                            .location()
                            .secondary_label()
                            .with_message(format!("Expected type: {parameter_type}")),
                    ));
            }
        }
    }

    let solutions: HashMap<TypeVariable, FenlType> = signature
        .type_parameters
        .iter()
        .filter_map(|p| {
            // This expect should never fail -- it would mean no arguments used the type variable
            // Which would have been an invalid signature.
            let argument_types = types_for_variable
                .remove(&p.name)
                .expect("unused type variable");

            if argument_types.is_empty() {
                None
            } else {
                debug_assert!(
                    p.constraints.len() == 1,
                    "only one constraint currently supported"
                );
                let constraint = p.constraints[0];

                // Can't use `?` because we want Option<Result<...>>.
                // This allows `filter_map` to filter out the `None` before the errors are
                // collected.
                Some(
                    solve_constraint(call, &constraint, &argument_types)
                        .map(|unified| (p.name.clone(), unified)),
                )
            }
        })
        .try_collect()?;

    // types_for_variables should be empty, asserting that each parameter was assigned a defined
    // type variable.
    debug_assert!(types_for_variable.is_empty(), "unassigned type variables");

    let instantiated_arguments = parameter_types
        .iter()
        .map(|parameter_type| instantiate_type(parameter_type, &solutions))
        .collect();
    let instantiated_arguments = arguments.with_values(instantiated_arguments);
    let instantiated_return = if instantiated_arguments.iter().any(|t| t.is_error()) {
        FenlType::Error
    } else {
        instantiate_type(signature.result(), &solutions)
    };

    Ok((instantiated_arguments, instantiated_return))
}

/// Validate an instantiation.
///
/// If the instantiation was valid, returns the argument type.
///
/// This is intended to be simpler to inspect for correctness than
/// [instantiate], allowing it to be used for verification of the
/// results both immediately after instantiation as well as for
/// validating types after various transformations, such as
/// converting to the plan.
pub fn validate_instantiation(
    argument_types: &Resolved<FenlType>,
    signature: &Signature,
) -> anyhow::Result<FenlType> {
    let parameters = signature.parameters();
    debug_assert_eq!(
        argument_types.names(),
        parameters.names(),
        "Arguments being instantiated should have the same name as parameters"
    );

    let mut types_for_variable: HashMap<TypeVariable, FenlType> = HashMap::new();
    for (argument_type, parameter_type) in izip!(argument_types.iter(), parameters.types()) {
        if matches!(argument_type, FenlType::Concrete(DataType::Null)) {
            // Skip -- null arguments satisfy any parameter.
            continue;
        }
        match parameter_type.inner() {
            FenlType::TypeRef(type_var) => {
                match types_for_variable.entry(type_var.clone()) {
                    Entry::Occupied(occupied) => {
                        // When validating, we assume that all uses of a constraint are
                        // the same. This should be the case for the DFG and plan, since
                        // explicit casts have been added.
                        anyhow::ensure!(
                            occupied.get() == argument_type
                                || matches!(occupied.get(), FenlType::Error)
                                || matches!(argument_type, FenlType::Error),
                            "Failed type validation: expected {} but was {}",
                            occupied.get(),
                            argument_type
                        );
                    }
                    Entry::Vacant(vacant) => {
                        vacant.insert(argument_type.clone());
                    }
                }
            }
            FenlType::Error => {
                // Assume the argument matches (since we already reported what
                // caused the error to appear). We may be able
                // to tighten this by *ignoring* the errors during type
                // inference, and just treating empty lists as
                // OK if there was an error (and returning an error). Then
                // `i64 + error` would assume it would be `i64`. But that could
                // be invalidated if after correcting the error
                // it changed to `f64`. For now, we just let the error swallow
                // other errors.
            }
            _ => {
                anyhow::ensure!(
                    parameter_type.inner() == argument_type
                        || matches!(
                            argument_type,
                            FenlType::Error | FenlType::Concrete(DataType::Null)
                        ),
                    "Failed type validation: expected {} but was {}",
                    parameter_type,
                    argument_type
                );
            }
        };
    }

    if argument_types.iter().any(|t| t.is_error()) {
        return Ok(FenlType::Error);
    }

    let instantiated_return = instantiate_type(signature.result(), &types_for_variable);
    Ok(instantiated_return)
}

/// Determine the type for a constraint based on the associated argument types.
///
/// # Fails
/// If the types are empty. If no parameters use the constraint (and there are
/// no associated parameters) we shouldn't try to determine the corresponding
/// type.
fn solve_constraint(
    call: &Located<String>,
    constraint: &TypeClass,
    types: &[&Located<FenlType>],
) -> Result<FenlType, DiagnosticBuilder> {
    debug_assert!(!types.is_empty());

    if types.iter().any(|t| t.inner().is_error()) {
        return Ok(FenlType::Error);
    }

    // Find the minimum type compatible with all the arguments associated with the
    // constraint.
    let mut result = Some(types[0].inner().clone());
    for arg_type in &types[1..] {
        if let Some(prev) = result {
            result = least_upper_bound(prev, arg_type.inner());
        }
    }

    result
        // Promote the minimum type to be compatible with the constraint, if necessary.
        .and_then(|data_type| promote_concrete(data_type, constraint))
        // Return an error if either (a) there wasn't a least-upper bound or
        // (b) it wasn't possible to promote the least-upper bound to be compatible
        // with the type constraint.
        .ok_or_else(|| {
            // Only report each distinct type as a problem once. This reduces clutter in the
            // error. TODO: Attempt to minimize the number of types involved
            // further. Find a subset of types that are compatible, and report
            // the problem with the corresponding least-upper-bound and the
            // remaining type(s).
            let distinct_arg_types: Vec<_> = types
                .iter()
                .unique_by(|l| l.inner())
                .map(|arg_type| {
                    arg_type
                        .location()
                        .secondary_label()
                        .with_message(format!("Type: {arg_type}"))
                })
                .collect();

            DiagnosticCode::InvalidArgumentType
                .builder()
                .with_label(
                    call.location()
                        .primary_label()
                        .with_message(format!("Invalid types for call to '{call}'")),
                )
                .with_labels(distinct_arg_types)
                .with_note(format!("Expected '{constraint}'"))
        })
}

/// Instantiate the (possibly generic) `FenlType` using the computed
/// solutions.
fn instantiate_type(fenl_type: &FenlType, solutions: &HashMap<TypeVariable, FenlType>) -> FenlType {
    match fenl_type {
        FenlType::TypeRef(type_var) => solutions
            .get(type_var)
            .cloned()
            .unwrap_or(FenlType::Concrete(DataType::Null)),
        FenlType::Concrete(_) => fenl_type.clone(),
        FenlType::Window => fenl_type.clone(),
        FenlType::Json => fenl_type.clone(),
        FenlType::Error => fenl_type.clone(),
    }
}

fn least_upper_bound(a: FenlType, b: &FenlType) -> Option<FenlType> {
    match (a, b) {
        (FenlType::Error, _) | (_, FenlType::Error) => Some(FenlType::Error),
        (FenlType::Concrete(dt_a), FenlType::Concrete(dt_b)) => {
            let result = least_upper_bound_data_type(dt_a, dt_b);
            result.map(FenlType::Concrete)
        }
        (FenlType::Concrete(DataType::Null), FenlType::Window)
        | (FenlType::Window, FenlType::Concrete(DataType::Null))
        | (FenlType::Window, FenlType::Window) => Some(FenlType::Window),
        (_, _) => {
            // Any other combination of types cannot produce a least upper bound
            None
        }
    }
}

/// Defines the least-upper-bound of a pair of types.
///
/// This is basically as a 2-d table indicating what happens when two types
/// are "joined". This is similar to the approach taken in Numpy, Julia,
/// SQL, C, etc.
///
/// See also the [table of promotion rules](https://docs.kaskada.com/docs/data-model#type-promotion-rules) for fenl.
fn least_upper_bound_data_type(a: DataType, b: &DataType) -> Option<DataType> {
    use DataType::*;

    match (a, b) {
        ///////////////////////////////////////////////////////////////////
        // Rules from the numeric table:

        // Row 6: A is i8.
        (Int8, Int8) => Some(Int8),
        (Int8, Int16) => Some(Int16),
        (Int8, Int32) => Some(Int32),
        (Int8, Int64) => Some(Int64),
        (Int8, UInt8) => Some(Int16),
        (Int8, UInt16) => Some(Int32),
        (Int8, UInt32) => Some(Int64),
        (Int8, UInt64) => Some(Float64),
        (Int8, Float16) => Some(Float64),
        (Int8, Float32) => Some(Float64),
        (Int8, Float64) => Some(Float64),
        // Row 7: A is i16.
        (Int16, Int8) => Some(Int16),
        (Int16, Int16) => Some(Int16),
        (Int16, Int32) => Some(Int32),
        (Int16, Int64) => Some(Int64),
        (Int16, UInt8) => Some(Int16),
        (Int16, UInt16) => Some(Int32),
        (Int16, UInt32) => Some(Int64),
        (Int16, UInt64) => Some(Float64),
        (Int16, Float16) => Some(Float64),
        (Int16, Float32) => Some(Float64),
        (Int16, Float64) => Some(Float64),
        // Row 8: A is i32.
        (Int32, Int8) => Some(Int32),
        (Int32, Int16) => Some(Int32),
        (Int32, Int32) => Some(Int32),
        (Int32, Int64) => Some(Int64),
        (Int32, UInt8) => Some(Int32),
        (Int32, UInt16) => Some(Int32),
        (Int32, UInt32) => Some(Int64),
        (Int32, UInt64) => Some(Float64),
        (Int32, Float16) => Some(Float64),
        (Int32, Float32) => Some(Float64),
        (Int32, Float64) => Some(Float64),
        // Row 9: A is i64.
        (Int64, Int8) => Some(Int64),
        (Int64, Int16) => Some(Int64),
        (Int64, Int32) => Some(Int64),
        (Int64, Int64) => Some(Int64),
        (Int64, UInt8) => Some(Int64),
        (Int64, UInt16) => Some(Int64),
        (Int64, UInt32) => Some(Int64),
        (Int64, UInt64) => Some(Float64),
        (Int64, Float16) => Some(Float64),
        (Int64, Float32) => Some(Float64),
        (Int64, Float64) => Some(Float64),
        // Row 10: A is u8.
        (UInt8, Int8) => Some(Int16),
        (UInt8, Int16) => Some(Int16),
        (UInt8, Int32) => Some(Int32),
        (UInt8, Int64) => Some(Int64),
        (UInt8, UInt8) => Some(UInt8),
        (UInt8, UInt16) => Some(UInt16),
        (UInt8, UInt32) => Some(UInt32),
        (UInt8, UInt64) => Some(UInt64),
        (UInt8, Float16) => Some(Float64),
        (UInt8, Float32) => Some(Float64),
        (UInt8, Float64) => Some(Float64),
        // Row 11: A is u16.
        (UInt16, Int8) => Some(Int32),
        (UInt16, Int16) => Some(Int32),
        (UInt16, Int32) => Some(Int32),
        (UInt16, Int64) => Some(Int64),
        (UInt16, UInt8) => Some(UInt16),
        (UInt16, UInt16) => Some(UInt16),
        (UInt16, UInt32) => Some(UInt32),
        (UInt16, UInt64) => Some(UInt64),
        (UInt16, Float16) => Some(Float64),
        (UInt16, Float32) => Some(Float64),
        (UInt16, Float64) => Some(Float64),
        // Row 12: A is u32.
        (UInt32, Int8) => Some(Int64),
        (UInt32, Int16) => Some(Int64),
        (UInt32, Int32) => Some(Int64),
        (UInt32, Int64) => Some(Int64),
        (UInt32, UInt8) => Some(UInt32),
        (UInt32, UInt16) => Some(UInt32),
        (UInt32, UInt32) => Some(UInt32),
        (UInt32, UInt64) => Some(UInt64),
        (UInt32, Float16) => Some(Float64),
        (UInt32, Float32) => Some(Float64),
        (UInt32, Float64) => Some(Float64),
        // Row 13: A is u64.
        (UInt64, Int8) => Some(Int64),
        (UInt64, Int16) => Some(Int64),
        (UInt64, Int32) => Some(Int64),
        (UInt64, Int64) => Some(Int64),
        (UInt64, UInt8) => Some(UInt32),
        (UInt64, UInt16) => Some(UInt32),
        (UInt64, UInt32) => Some(UInt32),
        (UInt64, UInt64) => Some(UInt64),
        (UInt64, Float16) => Some(Float64),
        (UInt64, Float32) => Some(Float64),
        (UInt64, Float64) => Some(Float64),
        // Row 14: A is f16.
        (Float16, Int8) => Some(Float64),
        (Float16, Int16) => Some(Float64),
        (Float16, Int32) => Some(Float64),
        (Float16, Int64) => Some(Float64),
        (Float16, UInt8) => Some(Float64),
        (Float16, UInt16) => Some(Float64),
        (Float16, UInt32) => Some(Float64),
        (Float16, UInt64) => Some(Float64),
        (Float16, Float16) => Some(Float16),
        (Float16, Float32) => Some(Float32),
        (Float16, Float64) => Some(Float64),
        // Row 15: A is f32.
        (Float32, Int8) => Some(Float64),
        (Float32, Int16) => Some(Float64),
        (Float32, Int32) => Some(Float64),
        (Float32, Int64) => Some(Float64),
        (Float32, UInt8) => Some(Float64),
        (Float32, UInt16) => Some(Float64),
        (Float32, UInt32) => Some(Float64),
        (Float32, UInt64) => Some(Float64),
        (Float32, Float16) => Some(Float32),
        (Float32, Float32) => Some(Float32),
        (Float32, Float64) => Some(Float64),
        // Row 16: A is f64.
        (Float64, Int8) => Some(Float64),
        (Float64, Int16) => Some(Float64),
        (Float64, Int32) => Some(Float64),
        (Float64, Int64) => Some(Float64),
        (Float64, UInt8) => Some(Float64),
        (Float64, UInt16) => Some(Float64),
        (Float64, UInt32) => Some(Float64),
        (Float64, UInt64) => Some(Float64),
        (Float64, Float16) => Some(Float64),
        (Float64, Float32) => Some(Float64),
        (Float64, Float64) => Some(Float64),

        // Row 17: A is Utf8
        (Utf8, Utf8) => Some(Utf8),
        (Utf8, ts @ Timestamp(_, _)) => Some(ts.clone()),
        (ts @ Timestamp(_, _), Utf8) => Some(ts),

        //
        ///////////////////////////////////////////////////////////////////
        // Other rules

        // Null is promotable to a null version of any other type.
        (Null, other) => Some(other.clone()),
        (other, Null) => Some(other),

        // Catch all
        (lhs, rhs) if &lhs == rhs => Some(lhs),

        // Least Upper bound on records = least upper bound on each field.
        // Currently, this requires the fields to be present (in any order) in both
        // structs. We could relax this and allow absent fields to be treated as null.
        // This could make it easier to make mistakes though -- nulling out one or two
        // fields may be OK, but at some point it may have been intended that the structs
        // were genuinely different.
        (Struct(fields1), Struct(fields2)) => {
            if fields1.len() != fields2.len() {
                // Early check that the fields don't match.
                return None;
            }

            // TODO: Have least_upper_bound return a anyhow::Result. Then this method can
            // indicate *which* fields didn't match.

            // Allow the names to be in any order by creating a map.
            let mut fields2: HashMap<_, _> =
                fields2.iter().map(|field| (field.name(), field)).collect();
            let result_fields: Vec<_> = fields1
                .iter()
                .flat_map(|field1| {
                    if let Some(field2) = fields2.remove(field1.name()) {
                        least_upper_bound_data_type(field1.data_type().clone(), field2.data_type())
                            .map(|data_type| Field::new(field1.name(), data_type, true))
                    } else {
                        None
                    }
                })
                .collect();

            if result_fields.len() != fields1.len() {
                // At least one of the fields didn't line up.
                None
            } else {
                Some(DataType::Struct(result_fields.into()))
            }
        }

        // Other types are unrelated.
        _ => None,
    }
}

/// Promote a concrete type to satisfy this constraint.
///
/// If the `concrete` type already satisfies this constraint, it is
/// returned. If the `concrete` may be promoted to satisfy this
/// constraint, it is. Otherwise, `None` is returned.
fn promote_concrete(concrete: FenlType, constraint: &TypeClass) -> Option<FenlType> {
    use DataType::*;
    match (constraint, &concrete) {
        // Any type may be null.
        (_, FenlType::Concrete(Null)) => Some(concrete),

        // Window behaviors don't fit anything else and nothing else matches window behavior.
        (_, FenlType::Window) => None,

        // Any concrete type satisfies `any`.
        (TypeClass::Any, _) => Some(concrete),

        // Json types should not promote into other constraints.
        (_, FenlType::Json) => None,

        // All numeric types satisfy `number`.
        (
            TypeClass::Number,
            FenlType::Concrete(
                Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float16 | Float32
                | Float64,
            ),
        ) => Some(concrete),
        (TypeClass::Number, FenlType::Concrete(_)) => None,

        // UInt needs to be widened to wider Int or Float64 to be `signed`
        (TypeClass::Signed, FenlType::Concrete(Int8 | Int16 | Int32 | Int64)) => Some(concrete),
        (TypeClass::Signed, FenlType::Concrete(UInt8)) => Some(FenlType::Concrete(Int16)),
        (TypeClass::Signed, FenlType::Concrete(UInt16)) => Some(FenlType::Concrete(Int32)),
        (TypeClass::Signed, FenlType::Concrete(UInt32)) => Some(FenlType::Concrete(Int64)),
        (TypeClass::Signed, FenlType::Concrete(UInt64)) => Some(FenlType::Concrete(Float64)),
        (TypeClass::Signed, FenlType::Concrete(Float16 | Float32 | Float64)) => Some(concrete),
        (TypeClass::Signed, FenlType::Concrete(_)) => None,

        // Int and UInt needs to be widened to Float64 to be `float`
        (
            TypeClass::Float,
            FenlType::Concrete(Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64),
        ) => Some(FenlType::Concrete(Float64)),
        (TypeClass::Float, FenlType::Concrete(Float16 | Float32 | Float64)) => Some(concrete),
        (TypeClass::Float, FenlType::Concrete(_)) => None,

        // Duration and Interval types are time deltas.
        (TypeClass::TimeDelta, FenlType::Concrete(Duration(_) | Interval(_))) => Some(concrete),
        (TypeClass::TimeDelta, FenlType::Concrete(_)) => None,

        // Ordered types include all numbered types and timestamps.
        (
            TypeClass::Ordered,
            FenlType::Concrete(
                Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float16 | Float32
                | Float64,
            ),
        ) => Some(concrete),
        (TypeClass::Ordered, FenlType::Concrete(Timestamp(_, _))) => Some(concrete),
        (TypeClass::Ordered, _) => None,

        // Keys include anything we can currently hash.
        (TypeClass::Key, FenlType::Concrete(actual_type)) => {
            if sparrow_arrow::hash::can_hash(actual_type) {
                Some(concrete)
            } else {
                None
            }
        }

        // errors propagate.
        (TypeClass::Error, _) => Some(FenlType::Error),

        // Generics can never be concrete.
        (_, FenlType::TypeRef(_)) => None,

        // Errors propagate. This ensures we don't report an additional error.
        (_, FenlType::Error) => Some(FenlType::Error),
    }
}

/// Returns a bool indicating whether
///
/// 1) The `actual` datatype can be cast into the `expected` by the arrow
/// cast kernel.
///
/// 2) The cast is desired to happen implicitly.
///
/// For example, while the kernel can cast i64 -> i32, we may not want to
/// implicitly make that decision.
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
        (Timestamp(_, _), Timestamp(TimeUnit::Nanosecond, None)) => true,
        // Other promotions must be explicitly requested.
        (_, _) => false,
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::str::FromStr;

    use sparrow_syntax::{FeatureSetPart, FenlType, Resolved, Signature};

    use super::*;

    /// Helper that runs type inference and returns the result (as a string) for
    /// easier testing.
    fn instantiate_types(
        signature_str: &'static str,
        argument_types: &[&'static str],
    ) -> Result<String, DiagnosticBuilder> {
        let signature = signature(signature_str);
        let argument_types = argument_types
            .iter()
            .map(|s| {
                Located::internal_str(s).transform(|s| {
                    FenlType::from_str(s)
                        .unwrap_or_else(|e| panic!("'{s}' is not valid as a type: {e}"))
                })
            })
            .collect();

        let argument_types = Resolved::new(
            Cow::Owned(signature.parameters().names().to_owned()),
            argument_types,
            signature.parameters().has_vararg,
        );

        let call = Located::internal_string("callsite");
        instantiate(&call, &argument_types, &signature).map(|(args, result)| {
            format!(
                "({}) -> {}",
                args.iter()
                    .enumerate()
                    .format_with(", ", |(index, fenl_type), f| f(&format_args!(
                        "{}: {}",
                        args.parameter_name(index),
                        fenl_type
                    ))),
                result
            )
        })
    }

    fn signature(signature: &'static str) -> Signature {
        Signature::try_from_str(FeatureSetPart::Internal(signature), signature).unwrap()
    }

    const ADD_SIGNATURE: &str = "add<N: number>(lhs: N, rhs: N) -> N";
    const NEG_SIGNATURE: &str = "neg<S: signed>(n: S) -> S";

    #[test]
    fn test_instantiate_add() {
        // i32 should be widened to i64
        assert_eq!(
            instantiate_types(ADD_SIGNATURE, &["i32", "i64"]),
            Ok("(lhs: i64, rhs: i64) -> i64".to_owned())
        );
        // i32 should be widened to i64; u32 should be cast to i64
        assert_eq!(
            instantiate_types(ADD_SIGNATURE, &["i32", "u32"]),
            Ok("(lhs: i64, rhs: i64) -> i64".to_owned())
        );
        // both should be cast to f64
        assert_eq!(
            instantiate_types(ADD_SIGNATURE, &["i32", "f32"]),
            Ok("(lhs: f64, rhs: f64) -> f64".to_owned())
        );
        // i32 should be cast to float (defaulting to f64).
        // This could happen if the right hand side was a floating point literal.
        assert_eq!(
            instantiate_types(ADD_SIGNATURE, &["i32", "f32"]),
            Ok("(lhs: f64, rhs: f64) -> f64".to_owned())
        );
    }

    #[test]
    fn test_instantiate_neg() {
        // i32 is signed
        assert_eq!(
            instantiate_types(NEG_SIGNATURE, &["i32"]),
            Ok("(n: i32) -> i32".to_owned())
        );
        // u32 needs to be widened
        assert_eq!(
            instantiate_types(NEG_SIGNATURE, &["u32"]),
            Ok("(n: i64) -> i64".to_owned())
        );
        // u64 needs to be widened (to float)
        assert_eq!(
            instantiate_types(NEG_SIGNATURE, &["u64"]),
            Ok("(n: f64) -> f64".to_owned())
        );
    }

    // TODO: Test error cases
}
