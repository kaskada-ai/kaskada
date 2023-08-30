mod least_upper_bound;
mod type_constraints;

use arrow_schema::DataType;
use hashbrown::HashMap;
use itertools::{izip, Itertools};

use crate::instantiate::type_constraints::TypeConstraints;
use crate::signature::Signature;
use crate::type_class::TypeClass;
use crate::{DisplayFenlType, Error, TypeVariable};

/// The types inferred for a function call.
#[derive(Debug)]
pub struct Types {
    /// The type of each argument to the function call.
    pub arguments: Vec<DataType>,
    /// The result of the function call.
    pub result: DataType,
}

impl std::fmt::Display for Types {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "({}) -> {}",
            self.arguments.iter().map(|t| t.display()).format(", "),
            self.result.display()
        )
    }
}

/// Instantiate arguments to a (possibly) polymorphic signature.
pub(super) fn instantiate(
    signature: &Signature,
    arguments: Vec<DataType>,
) -> error_stack::Result<Types, Error> {
    let arguments_len = arguments.len();
    let parameters = signature.iter_parameters(arguments.len())?;

    // First pass: Collect arguments associated with generic types.
    let mut type_constraints = TypeConstraints::default();

    for (parameter, argument) in izip!(parameters, arguments) {
        type_constraints.unify_one(&parameter.ty, argument.into())?;
    }

    // Solve for each type parameter in the signature.
    let solutions: HashMap<TypeVariable, DataType> =
        type_constraints.solve(signature.type_parameters.iter())?;

    // Instantiate the actual types for the signature.
    let arguments = signature
        .iter_parameters(arguments_len)?
        .map(|parameter| {
            parameter
                .ty
                .instantiate(Some(&solutions))
                .expect("concrete instantiation")
        })
        .collect();

    let result = signature
        .result
        .instantiate(Some(&solutions))
        .expect("concrete instantiation");

    Ok(Types { arguments, result })
}

/// Promote a concrete type to satisfy this constraint.
///
/// If the `fenl_type` type already satisfies this constraint, it is
/// returned. If the `concrete` may be promoted to satisfy this
/// constraint, it is. Otherwise, `None` is returned.
fn promote_concrete(data_type: DataType, type_class: TypeClass) -> Option<DataType> {
    use DataType::*;
    match (type_class, data_type) {
        // Any type may be null.
        (_, DataType::Null) => Some(DataType::Null),

        // Any concrete type satisfies `any`.
        (TypeClass::Any, data_type) => Some(data_type),

        // All numeric types satisfy `number`.
        (
            TypeClass::Number,
            data_type @ (Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float16
            | Float32 | Float64),
        ) => Some(data_type),
        (TypeClass::Number, _) => None,

        // UInt needs to be widened to wider Int or Float64 to be `signed`
        (TypeClass::Signed, data_type @ (Int8 | Int16 | Int32 | Int64)) => Some(data_type),
        (TypeClass::Signed, UInt8) => Some(Int16),
        (TypeClass::Signed, UInt16) => Some(Int32),
        (TypeClass::Signed, UInt32) => Some(Int64),
        (TypeClass::Signed, UInt64) => Some(Float64),
        (TypeClass::Signed, data_type @ (Float16 | Float32 | Float64)) => Some(data_type),
        (TypeClass::Signed, _) => None,

        // Int and UInt needs to be widened to Float64 to be `float`
        (TypeClass::Float, Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64) => {
            Some(Float64)
        }
        (TypeClass::Float, data_type @ (Float16 | Float32 | Float64)) => Some(data_type),
        (TypeClass::Float, _) => None,

        // Duration and Interval types are time deltas.
        (TypeClass::TimeDelta, data_type @ (Duration(_) | Interval(_))) => Some(data_type),
        (TypeClass::TimeDelta, _) => None,

        // Ordered types include all numbered types and timestamps.
        (
            TypeClass::Ordered,
            data_type @ (Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32 | UInt64 | Float16
            | Float32 | Float64),
        ) => Some(data_type),
        (TypeClass::Ordered, data_type @ Timestamp(_, _)) => Some(data_type),
        (TypeClass::Ordered, _) => None,

        // Keys include anything we can currently hash.
        (TypeClass::Key, data_type) => Some(data_type),
    }
}
