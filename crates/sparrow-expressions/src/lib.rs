#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Execution of expressions.
//!
//! Creating an [ExpressionExecutor] takes a sequence of
//! [expressions][sparrow_physical::Expr] and applies them to an input batch to
//! produce a sequence of computed columns. Expressions may reference columns
//! from the input to the expressions (typically the input to the step executing
//! the expressions), create a column from a literal value, or apply an
//! expression to columns computed by earlier expressions.
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
mod evaluator;
mod evaluators;
mod executor;
mod values;
mod work_area;

pub use error::*;
pub use evaluators::{intern_name, names};
pub use executor::*;
