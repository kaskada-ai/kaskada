#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Common type-representation, type-inference and type-checking.
//!
//! These facilities are used by the frontend for displaying type information in
//! errors and instantiating the types of functions applied to specific
//! arguments. They are used in the backend for instantiating the types of
//! instructions. They are used at execution time for checking that a plan has
//! properly instantiated instructions.

mod error;
mod fenl_type;
mod instantiate;
mod signature;
mod type_class;

pub type TypeVariable = String;

pub use error::*;
pub use fenl_type::*;
pub use instantiate::Types;
pub use signature::*;
