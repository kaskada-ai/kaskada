#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Implementations of the transforms to be executed.

mod error;
mod transform_pipeline;
pub mod transforms;

pub use error::*;
