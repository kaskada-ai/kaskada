//! Utilities for working with argument and parameter types.

use std::cmp::Ordering;

use anyhow::Context;
use arrow::datatypes::{DataType, Field, TimeUnit};
use hashbrown::HashMap;
use itertools::{izip, Itertools};
use sparrow_syntax::{
    Collection, FenlType, Located, Resolved, Signature, TypeClass, TypeParameter, TypeVariable,
};

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

    // Contains the fenl type(s) for each type variable.
    let mut types_for_variable = TypeConstraints::default();

    for (index, (argument_type, parameter_type)) in
        izip!(arguments.iter(), parameter_types.clone()).enumerate()
    {
        types_for_variable
            .unify_one(parameter_type.inner(), argument_type.clone())
            .map_err(|_| {
                DiagnosticCode::InvalidArgumentType
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
                    )
            })?;
    }

    // Solve for each type parameter in the signature.
    let solutions: HashMap<TypeVariable, FenlType> =
        types_for_variable.solutions(call, signature.type_parameters.iter())?;

    // Instantiate the actual types for the signature.
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
        validate_type(
            &mut types_for_variable,
            parameter_type.inner(),
            argument_type,
        )?;
    }

    if argument_types.iter().any(|t| t.is_error()) {
        return Ok(FenlType::Error);
    }

    let instantiated_return = instantiate_type(signature.result(), &types_for_variable);
    Ok(instantiated_return)
}

fn validate_type(
    types_for_variable: &mut HashMap<TypeVariable, FenlType>,
    parameter: &FenlType,
    argument: &FenlType,
) -> anyhow::Result<()> {
    match (parameter, argument) {
        (FenlType::Error, _) | (_, FenlType::Error) => {
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
        (_, FenlType::Concrete(DataType::Null)) => {
            // Null arguments satisfy any parameter.
        }
        (FenlType::TypeRef(type_var), actual) => {
            let expected = types_for_variable
                .entry_ref(type_var)
                .or_insert_with(|| actual.clone());
            anyhow::ensure!(
                expected == actual,
                "Failed type validation: expected {expected} but was {actual}"
            );
        }
        (FenlType::Collection(p_coll, p_args), argument) => {
            let a_args = argument.collection_args(p_coll).with_context(|| {
                format!(
                    "Failed type validation: Expected collection type {p_coll} but got {argument}"
                )
            })?;
            anyhow::ensure!(
                p_args.len() == a_args.len(),
                "Failed type validation: Mismatched number of type arguments {p_args:?} and {a_args:?}"
            );
            for (p_arg, a_arg) in izip!(p_args, a_args) {
                validate_type(types_for_variable, p_arg, &a_arg)?;
            }
        }
        (expected, actual) => {
            anyhow::ensure!(
                actual == expected,
                "Failed type validation: expected {expected} but was {actual}"
            );
        }
    }
    Ok(())
}

/// Determine the type for a type class based on the associated argument types.
///
/// # Fails
/// If the types are empty. If no parameters use the type class (and there are
/// no associated parameters) we shouldn't try to determine the corresponding
/// type.
fn solve_type_class(
    call: &Located<String>,
    type_class: &TypeClass,
    types: &[Located<FenlType>],
) -> Result<FenlType, DiagnosticBuilder> {
    debug_assert!(!types.is_empty());
    if types.iter().any(|t| t.inner().is_error()) {
        return Ok(FenlType::Error);
    }

    // Find the minimum type compatible with all the arguments associated with the
    // type class.
    let mut result = Some(types[0].inner().clone());
    for arg_type in &types[1..] {
        if let Some(prev) = result {
            result = least_upper_bound(prev, arg_type.inner());
        }
    }

    if result.is_none() {
        let distinct_arg_types = distinct_arg_types(types);
        return Err(DiagnosticCode::IncompatibleArgumentTypes
            .builder()
            .with_label(
                call.location()
                    .primary_label()
                    .with_message(format!("Incompatible types for call to '{call}'")),
            )
            .with_labels(distinct_arg_types));
    }

    result
        // Promote the minimum type to be compatible with the type class, if necessary.
        .and_then(|data_type| promote_concrete(data_type, type_class))
        // Return an error if either (a) there wasn't a least-upper bound or
        // (b) it wasn't possible to promote the least-upper bound to be compatible
        // with the type type class.
        .ok_or_else(|| {
            let distinct_arg_types = distinct_arg_types(types);
            DiagnosticCode::InvalidArgumentType
                .builder()
                .with_label(
                    call.location()
                        .primary_label()
                        .with_message(format!("Invalid types for call to '{call}'")),
                )
                .with_labels(distinct_arg_types)
                .with_note(format!("Expected '{type_class}'"))
        })
}

// Only report each distinct type as a problem once. This reduces clutter in the
// error. TODO: Attempt to minimize the number of types involved
// further. Find a subset of types that are compatible, and report
// the problem with the corresponding least-upper-bound and the
// remaining type(s).
fn distinct_arg_types(
    types: &[Located<FenlType>],
) -> Vec<codespan_reporting::diagnostic::Label<sparrow_syntax::FeatureSetPart>> {
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
    distinct_arg_types
}

/// Instantiate the (possibly generic) `FenlType` using the computed
/// solutions.
fn instantiate_type(fenl_type: &FenlType, solutions: &HashMap<TypeVariable, FenlType>) -> FenlType {
    match fenl_type {
        FenlType::TypeRef(type_var) => solutions
            .get(type_var)
            .cloned()
            .unwrap_or_else(|| panic!("missing solution for type variable {:?}", type_var)),
        FenlType::Collection(collection, type_args) => {
            match collection {
                Collection::List => debug_assert!(type_args.len() == 1),
                Collection::Map => debug_assert!(type_args.len() == 2),
            };

            let type_args = type_args
                .iter()
                .map(|t| instantiate_type(t, solutions))
                .collect();
            FenlType::Collection(*collection, type_args).normalize()
        }
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
        (LargeUtf8, Utf8) => Some(LargeUtf8),
        (Utf8, LargeUtf8) => Some(LargeUtf8),
        (Utf8, ts @ Timestamp(_, _)) => Some(ts.clone()),
        (ts @ Timestamp(_, _), Utf8) => Some(ts),

        //
        ///////////////////////////////////////////////////////////////////
        // Other rules

        // Null is promotable to a null version of any other type.
        (Null, other) => Some(other.clone()),
        (other, Null) => Some(other),

        // Duration types cast to the more granular TimeUnit
        (Duration(unit1), Duration(unit2)) => Some(Duration(std::cmp::max(unit1, unit2.clone()))),

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

/// Promote a concrete type to satisfy this type class.
///
/// If the `concrete` type already satisfies this type class, it is
/// returned. If the `concrete` may be promoted to satisfy this
/// type class, it is. Otherwise, `None` is returned.
fn promote_concrete(concrete: FenlType, type_class: &TypeClass) -> Option<FenlType> {
    use DataType::*;
    match (type_class, &concrete) {
        // Any type may be null.
        (_, FenlType::Concrete(Null)) => Some(concrete),

        // Window behaviors don't fit anything else and nothing else matches window behavior.
        (_, FenlType::Window) => None,

        // Any concrete type satisfies `any`.
        (TypeClass::Any, _) => Some(concrete),

        // Json types should not promote into other type class
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

        // Ordered types include all numbered types, timestamps, and timedeltas.
        (
            TypeClass::Ordered,
            FenlType::Concrete(
                Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float16 | Float32
                | Float64,
            ),
        ) => Some(concrete),
        (TypeClass::Ordered, FenlType::Concrete(Timestamp(_, _))) => Some(concrete),
        (TypeClass::Ordered, FenlType::Concrete(Duration(_) | Interval(_))) => Some(concrete),
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

        // Collections can never be concrete as they contain type variables.
        (_, FenlType::Collection(_, _)) => None,

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
        (Utf8, LargeUtf8) => true,
        (Timestamp(_, _), Timestamp(TimeUnit::Nanosecond, None)) => true,
        // More granular time units are "greater than" less granular time units,
        // so we can implicitly cast from a more granular time unit to a lesser one
        // by checking if the latter is greater than the former
        (Duration(tu1), Duration(tu2)) => tu2 > tu1,
        // Other promotions must be explicitly requested.
        (_, _) => false,
    }
}

#[derive(Default)]
struct TypeConstraints(HashMap<TypeVariable, Vec<Located<FenlType>>>);

impl TypeConstraints {
    /// Add a constraint that the type variable includes the given type.
    fn constrain(&mut self, variable: &TypeVariable, ty: Located<FenlType>) {
        self.0.entry_ref(variable).or_default().push(ty)
    }

    fn unify_all(
        &mut self,
        parameters: &[FenlType],
        arguments: Vec<Located<FenlType>>,
    ) -> Result<(), ()> {
        if parameters.len() != arguments.len() {
            return Err(());
        }
        for (parameter, argument) in parameters.iter().zip(arguments) {
            self.unify_one(parameter, argument)?;
        }
        Ok(())
    }

    /// Unify the type of one parameter with the corresponding argument.
    fn unify_one(&mut self, parameter: &FenlType, argument: Located<FenlType>) -> Result<(), ()> {
        match (parameter, argument.inner()) {
            (FenlType::TypeRef(variable), _) => self.constrain(variable, argument),
            (FenlType::Collection(p_collection, p_type_vars), arg_type) => {
                let Some(args) = arg_type.collection_args(p_collection) else {
                    return Err(());
                };

                let arguments = args
                    .into_iter()
                    .map(|arg| argument.with_value(arg))
                    .collect();
                self.unify_all(p_type_vars, arguments)?;
            }
            (_, FenlType::Error) => {
                // The argument is an error, but we already reported it.
                // Don't need to do anything.
            }
            (FenlType::Concrete(parameter), FenlType::Concrete(argument))
                if can_implicitly_cast(argument, parameter) =>
            {
                // Already solved -- the argument is either the same as the parameter
                // or can be cast to match the type of the parameter.
            }
            (FenlType::Window, FenlType::Window | FenlType::Concrete(DataType::Null)) => {
                // No problem -- can use `null` as a window.
            }
            _ => return Err(()),
        };
        Ok(())
    }

    fn solutions<'a>(
        self,
        call: &Located<String>,
        parameters: impl Iterator<Item = &'a TypeParameter>,
    ) -> Result<HashMap<TypeVariable, FenlType>, DiagnosticBuilder> {
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
                    debug_assert!(
                        p.type_classes.len() == 1,
                        "only one type class currently supported"
                    );
                    let type_class = p.type_classes[0];

                    // Can't use `?` because we want Option<Result<...>>.
                    // This allows `filter_map` to filter out the `None` before the errors are
                    // collected.
                    Some(
                        solve_type_class(call, &type_class, &argument_types)
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

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

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
                let part_id = FeatureSetPart::Internal(s);
                Located::internal_str(s).transform(|s| {
                    FenlType::try_from_str(part_id, s)
                        .unwrap_or_else(|e| panic!("'{s}' is not valid as a type: {e:?}"))
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

    #[test]
    fn test_instantiate_add() {
        const ADD_SIGNATURE: &str = "add<N: number>(lhs: N, rhs: N) -> N";

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
        const NEG_SIGNATURE: &str = "neg<S: signed>(n: S) -> S";

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

    #[test]
    fn test_instantiate_get_map() {
        const GET_SIGNATURE: &str = "get<K: key, V: any>(key: K, map: map<K, V>) -> V";

        assert_eq!(
            instantiate_types(GET_SIGNATURE, &["i32", "map<i32, f32>"]),
            Ok("(key: i32, map: map<i32, f32>) -> f32".to_owned())
        );

        assert_eq!(
            instantiate_types(GET_SIGNATURE, &["i64", "map<i64, f32>"]),
            Ok("(key: i64, map: map<i64, f32>) -> f32".to_owned())
        );

        assert_eq!(
            instantiate_types(GET_SIGNATURE, &["i32", "map<i32, i32>"]),
            Ok("(key: i32, map: map<i32, i32>) -> i32".to_owned())
        );
    }

    #[test]
    fn test_instantiate_flatten() {
        const FLATTEN_SIGNATURE: &str = "flatten<T: any>(list: list<list<T>>) -> list<T>";

        assert_eq!(
            instantiate_types(FLATTEN_SIGNATURE, &["list<list<f32>>"]),
            Ok("(list: list<list<f32>>) -> list<f32>".to_owned())
        );
    }

    fn validate_instantiation(
        signature_str: &'static str,
        argument_types: &[&'static str],
    ) -> anyhow::Result<FenlType> {
        let signature = signature(signature_str);
        let argument_types = argument_types
            .iter()
            .map(|s| {
                let part_id = FeatureSetPart::Internal(s);
                FenlType::try_from_str(part_id, s)
                    .unwrap_or_else(|e| panic!("'{s}' is not valid as a type: {e:?}"))
            })
            .collect();

        let argument_types = Resolved::new(
            Cow::Owned(signature.parameters().names().to_owned()),
            argument_types,
            signature.parameters().has_vararg,
        );
        super::validate_instantiation(&argument_types, &signature)
    }

    #[test]
    fn test_length_validation() {
        const LENGTH_SIGNATURE: &str = "length<T: any>(list: list<T>) -> i32";

        assert_eq!(
            validate_instantiation(LENGTH_SIGNATURE, &["list<f32>"]).unwrap(),
            FenlType::Concrete(DataType::Int32)
        )
    }
}
