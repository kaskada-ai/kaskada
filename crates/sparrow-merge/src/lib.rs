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

mod gather;
// pub(crate) use gather::*;
// pub mod merge_pipeline;
pub mod old;
