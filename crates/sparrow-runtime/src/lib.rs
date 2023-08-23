//! The runtime used for executing Sparrow plans.
//!
//! This module provides the logic used for executing each pass.
//!
//! Passes start by receiving rows (in order) from each source.
//! The rows are merged using the merging logic from `sparrow-merge`.
//!
//! Each merged batch is put in a `WorkArea`, and instructions are
//! executed to produce intermediate columns.
//!
//! Finally, the sinks in the pass are executed over all of the columns
//! in the work area to produce batches that are output over channels
//! to downstream passes.

#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

mod batch;
pub mod execute;
mod key_hash_index;
pub mod key_hash_inverse;
mod metadata;
mod min_heap;
pub mod prepare;
mod read;
pub mod stores;
mod streams;
mod util;

use std::path::PathBuf;

pub use batch::*;
pub use metadata::*;
pub use prepare::preparer;
use read::*;
use sparrow_api::kaskada::v1alpha::execute_request::Limits;

static DETERMINISTIC_RUNTIME_HASHER: ahash::RandomState =
    ahash::RandomState::with_seeds(8723, 8737, 8736, 9871);

#[derive(Debug, Default, Clone)]
pub(crate) struct RuntimeOptions {
    pub limits: Limits,
    pub max_batch_size: Option<usize>,

    /// Path to store the Query Flight Record to.
    /// Defaults to not storing anything.
    pub flight_record_path: Option<PathBuf>,
}

/// Initial size of the upload buffer.
///
/// This balances size (if we have multiple uploads in parallel) with
/// number of "parts" required to perform an upload.
const UPLOAD_BUFFER_SIZE_IN_BYTES: usize = 5_000_000;
