//! Defines computation kernels operating over Arrow vectors.

#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]
mod ordered_cast;
pub mod string;
pub mod time;

pub use ordered_cast::*;
