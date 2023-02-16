mod prepared_metadata;
mod raw_metadata;

use anyhow::Context;
pub use prepared_metadata::*;
pub use raw_metadata::*;

/// Return the file at a given path.
fn file_from_path(path: &std::path::Path) -> anyhow::Result<std::fs::File> {
    std::fs::File::open(path).with_context(|| format!("Unable to open {path:?}"))
}
