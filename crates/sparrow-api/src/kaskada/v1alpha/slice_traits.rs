use decorum::cmp::FloatOrd;
use decorum::hash::FloatHash;

use crate::kaskada::v1alpha::{slice_plan, SlicePlan};

// `Eq` is not enabled by default to the `f64`, but we need it to be in a
// hash table.
impl Eq for slice_plan::Slice {}

// We need to use decorum to implement the hash (manually) because of `f64`.
#[allow(clippy::derived_hash_with_manual_eq)]
impl std::hash::Hash for slice_plan::Slice {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        core::mem::discriminant(self).hash(state);

        match self {
            slice_plan::Slice::Percent(percent) => {
                percent.percent.float_hash(state);
            }
            slice_plan::Slice::EntityKeys(entity_keys) => {
                entity_keys.entity_keys.hash(state);
            }
        }
    }
}

// We need to use decorum to implement ordering (manually) because of `f64`.
impl PartialOrd for slice_plan::Slice {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for slice_plan::Slice {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (slice_plan::Slice::Percent(p1), slice_plan::Slice::Percent(p2)) => {
                p1.percent.float_cmp(&p2.percent)
            }
            (slice_plan::Slice::Percent(_), slice_plan::Slice::EntityKeys(_)) => {
                std::cmp::Ordering::Greater
            }
            (slice_plan::Slice::EntityKeys(_), slice_plan::Slice::Percent(_)) => {
                std::cmp::Ordering::Less
            }
            (slice_plan::Slice::EntityKeys(e1), slice_plan::Slice::EntityKeys(e2)) => {
                e1.entity_keys.cmp(&e2.entity_keys)
            }
        }
    }
}

impl PartialOrd for SlicePlan {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for SlicePlan {}

impl Ord for SlicePlan {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.table_name
            .cmp(&other.table_name)
            .then_with(|| self.slice.cmp(&other.slice))
    }
}
