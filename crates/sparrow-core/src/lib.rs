#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]
use std::pin::Pin;

mod context_code;
mod key_triple;
mod table_schema;

use arrow::record_batch::RecordBatch;
pub use context_code::*;
use futures::Stream;
pub use key_triple::{KeyTriple, KeyTriples};
pub use table_schema::TableSchema;

// A stream of record batches (wrapped in anyhow::Result, boxed and pinned).
pub type BatchStream = Pin<Box<dyn Stream<Item = anyhow::Result<RecordBatch>>>>;

/// A macro that wraps a debug print conditional on some boolean.
///
/// Generally, should use a constant so this is compiled away when disabled.
#[macro_export]
macro_rules! debug_println {
    ($cond:expr, $($x:tt)*) => { if $cond { println!("DBG: {}", format_args!($($x)*)) } }
}
