pub(crate) use gatherer::*;
pub use homogeneous_merge::*;

mod binary_merge;
mod gatherer;
mod homogeneous_merge;
mod input;

#[cfg(test)]
mod testing;

// Public for benchmarks.
pub use binary_merge::{binary_merge, BinaryMergeInput};
