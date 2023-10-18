#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Stateless expression evaluators.

mod cast;
mod coalesce;
mod comparison;
mod fieldref;
mod hash;
mod input;
mod is_valid;
mod json_field;
mod literal;
mod logical;
mod macros;
mod math;
mod record;
mod string;
mod time;

/// Call to ensure the expressions in this crate are installed.
///
/// This does two things:
///
/// 1. Avoids the compiler (or linter) removing the dependency on this crate
///    since it is (otherwise) only used via `inventory`.
/// 2. Check early to verify things are present.
pub fn ensure_registered() {
    assert!(sparrow_interfaces::expression::intern_name("add").is_some());
}
