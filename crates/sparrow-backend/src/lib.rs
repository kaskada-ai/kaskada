#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Compiler backend for Kaskada queries.
//!
//! The backend is responsible for converting logical plans to physical plans.
//! It also performs optimizations on both the logical plans and the physical
//! plans.

mod compile;
mod error;
mod exprs;
mod logical_to_physical;
// mod mutable_plan;
mod mutable_plan;
mod pipeline_schedule;

pub use compile::*;
pub use error::*;
pub use pipeline_schedule::*;
