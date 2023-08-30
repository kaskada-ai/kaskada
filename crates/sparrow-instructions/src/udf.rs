use sparrow_syntax::Signature;
use std::fmt::Debug;
use std::hash::Hash;

use crate::{Evaluator, StaticInfo};

/// Defines the interface for user-defined functions.
pub trait Udf: Send + Sync + Debug {
    fn signature(&self) -> &Signature;
    fn make_evaluator(&self, static_info: StaticInfo<'_>) -> Box<dyn Evaluator>;
    fn uuid(&self) -> &uuid::Uuid;
}

impl PartialOrd for dyn Udf {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for dyn Udf {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.signature()
            .name()
            .cmp(other.signature().name())
            .then(self.uuid().cmp(other.uuid()))
    }
}

impl Hash for dyn Udf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.uuid().hash(state);
    }
}

impl PartialEq for dyn Udf {
    fn eq(&self, other: &Self) -> bool {
        self.uuid() == other.uuid()
    }
}

impl Eq for dyn Udf {}
