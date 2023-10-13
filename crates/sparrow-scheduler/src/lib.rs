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

mod error;
mod idle_workers;
mod input_handles;
mod monitor;
mod partition;
mod pipeline;
mod queue;
mod schedule_count;
mod task;
mod worker;
mod worker_pool;

pub use error::*;
pub use input_handles::*;
pub use partition::*;
pub use pipeline::*;
pub use task::*;
pub use worker::*;
pub use worker_pool::*;
