//! The compiler from Fenl expressions to Sparrow execution plans.
//!
//! Each expression added to the compiler is parsed into an AST and typechecked.
//! It is then converted to a DFG and added to the graph being constructed
//! within the compiler using `ast_to_dfg`.
//!
//! Once all expressions are added the plan is requested. At this point, the
//! compiler performs the following steps:
//!
//!  1. Simplify the DFG by applying all of the rewrite rules to discover
//!     equivalences, and picking the simplest resulting DFG. This is managed by
//!     the E-graph library, with the rules in `simplification.rs`.
//!
//!  2. Schedule the nodes in the DFG to determine which passes they execute in.
//!     See `scheduling` for more information.
//!
//! 3. Produce the execution plan for the scheduled DFG. Buffering (and other
//!    cross-pass operations) added as needed. See `plan_builder.rs` and
//!    `pass_builder.rs`.
//!
//! All of the steps after producing the DFG are part of the `backend`.

#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

mod ast_to_dfg;
mod compile;
mod data_context;
mod dfg;
mod diagnostics;
mod env;
mod error;
mod frontend;
mod functions;
mod nearest_matches;
mod options;
pub mod plan;
mod time_domain;
mod types;

// TODO: Cleanup the top-level modules in the `sparrow-compiler` crate.
pub use ast_to_dfg::*;
pub use compile::*;
pub use data_context::*;
pub use dfg::{remove_useless_transforms, Dfg};
pub use diagnostics::*;
pub use error::*;
pub use frontend::*;
pub use functions::*;
pub use options::*;

pub use nearest_matches::NearestMatches;
