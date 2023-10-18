//! Module for defining expression evaluators.
//!
//! Expression evaluators apply an instruction named in the physical plan to one
//! or more Arrow arrays.
//!
//! # Adding an expression
//!
//! Adding an evaluator for a new kind of expression requires the following steps
//! (more details follow):
//!
//! 1. In a file for the evaluator, create a struct implementing the evaluator.
//!    This should have fields for the information necessary to execute the
//!    expression.
//! 2. Implement [evaluator::Evaluator] for the evaluator struct.
//! 3. Write a method `create` which takes [evaluators::StaticInfo] and returns
//!    an `error_stack::Result<Box<dyn Evaluator>, Error>`.
//! 4. Register the creation method with inventory of supported expressions.
//!
//! ## Fields in the Evaluator
//!
//! For each input column the evaluator operates on, it should have a
//! [work_area::WorkAreaValue]. This represents a type-safe handle for accessing
//! the input column from the work area during runtime. These values are
//! obtained from the `StaticInfo` by calling methods such as
//! [evaluators::StaticArg::primitive].
//!
//! Depending on the evaluator, it may also be necessary to have a primitive type
//! parameter, as can be seen with the various math operations.
//!
//! ## Implementing Create
//!
//! The create method should take the static info, verify that the types are
//! correct and the expression can be executed, and then create the evaluator
//! struct. Doing so also requires populating the values that will be referenced
//! at runtime. Usually, this population does the necessary type checking.
//!
//! ## Register the creation method
//!
//! Registering the evaluator is done using the following block of code.
//! This makes use of the `inventory` crate so evaluators may be registered
//! in other modules and discovered automatically.
//!
//! ```
//! inventory::submit!(crate::evaluators::EvaluatorFactory {
//!     name: "not",
//!     create: &create
//! });
//! ```
//!
//! In cases where the `create` method has a generic parameter, the
//! `create_primitive_evaluator!` macro can be used to create an implementation
//! of `create` for the EvaluatorFactory that matches on the given data type. In
//! the following registration, the first argument (position `0`) is used to
//! determine the type of evaluator to create. The *actual* `create` method
//! is called with the necessary data type.
//!
//! The final argument indicates what types should be supported by the match.
//! It can be one or more pairs of the form `(ty_case, ty_name), ...`, but is
//! more typically one of the following defined types (corresponding to the
//! equivalent Fenl type classes):
//!
//! - `number`: Supports `i32`, `i64`, `u32`, `u64`, `f32` and `f64`.
//! - `signed`: Supports `i32`, `i64`, `f32` and `f64`.
//! - `float`: Supports `f32` and `f64`.
//! - `ordered`: Supports `number` and the timestamp types.
//!
//! ```
//! inventory::submit!(crate::evaluators::EvaluatorFactory {
//!     name: "add",
//!     create: &crate::evaluators::macros::create_primitive_evaluator!(0, create, number)
//! });
//! ```

mod error;
mod static_info;
mod work_area;

use arrow_array::ArrayRef;
use itertools::Itertools;

pub use error::*;
use hashbrown::HashMap;
pub use static_info::*;
pub use work_area::*;

/// Trait for evaluating an individual expression node.
pub trait Evaluator: Send + Sync {
    /// Evaluate the function with the given runtime info.
    fn evaluate(&self, work_area: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error>;
}

/// Type alias for a function used to create an [Evaluator].
///
/// This type is equivalent to dynamic functions with signatures like:
///
/// ```
/// fn f<'a>(info: StaticInfo<'a>) -> error_stack::Result<Box<dyn Evaluator>, Error> + Send + Sync
/// ```
///
/// This corresponds to the functions each evaluator registers for creating
/// an evaluator from the static information (types, constant arguments, and
/// information about the arguments).
type EvaluatorFactoryFn =
    dyn for<'a> Fn(StaticInfo<'a>) -> error_stack::Result<Box<dyn Evaluator>, Error> + Send + Sync;

/// Factory for creating evaluators with a specific name.
pub struct EvaluatorFactory {
    pub name: &'static str,
    pub create: &'static EvaluatorFactoryFn,
}

inventory::collect!(EvaluatorFactory);

// This needs to be marked lazy so it is run after the evaluators
// are submitted to the inventory.
#[static_init::dynamic(lazy)]
static EVALUATORS: HashMap<&'static str, &'static EvaluatorFactoryFn> = {
    let result: HashMap<_, _> = inventory::iter::<EvaluatorFactory>()
        .map(|e| (e.name, e.create))
        .collect();

    debug_assert_eq!(
        result.len(),
        inventory::iter::<EvaluatorFactory>().count(),
        "Expected evaluators to have unique names. Duplicates: {:?}",
        inventory::iter::<EvaluatorFactory>()
            .map(|e| e.name)
            .duplicates()
            .collect::<Vec<_>>()
    );
    result
};

pub fn create_evaluator(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let Some(create) = EVALUATORS.get(info.name.as_ref()) else {
        error_stack::bail!(Error::NoEvaluator(info.name.clone()))
    };
    create(info)
}

/// Use the names of registered evaluators to intern the given name.
///
/// Returns `None` if no evaluator is registered for the given name.
pub fn intern_name(name: &str) -> Option<&'static str> {
    EVALUATORS.get_key_value(name).map(|(k, _)| *k)
}

// Exposed so we can report "nearest" names.
pub fn names() -> impl Iterator<Item = &'static str> {
    EVALUATORS.keys().copied()
}
