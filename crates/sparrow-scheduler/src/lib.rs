#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

//! Scheduler for local, multi-threaded execution of Sparrow plans.
//!
//! Each plan consists of one or more "pipelines". Each pipeline
//! takes batches and transforms them in some way to produce output
//! batches.

mod error;
mod partition;
mod pipeline;
mod queue;
mod schedule_count;
mod scheduler;
mod sink;
mod task;
mod worker;

pub use error::*;
pub use partition::*;
pub use pipeline::*;
pub use queue::*;
pub use scheduler::*;
pub use sink::*;
pub use task::*;
pub use worker::*;
