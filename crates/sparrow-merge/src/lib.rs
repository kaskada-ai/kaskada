//! Merge related functions and pipelines.
#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

mod in_memory_batches;
pub mod old;

pub use in_memory_batches::*;
