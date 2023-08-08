mod binary_merge;
mod gatherer;
mod homogeneous_merge;
mod input;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use binary_merge::{binary_merge, BinaryMergeInput};
pub use gatherer::*;
pub use homogeneous_merge::*;
pub use input::*;
