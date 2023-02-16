//! Serializable representation of Sparrow computation plans and helpers.
#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

pub use ids::*;
pub use inst::*;
pub use value::*;

mod ids;
mod inst;
mod value;
