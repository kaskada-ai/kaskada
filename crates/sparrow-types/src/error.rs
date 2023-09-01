use crate::FenlType;
use itertools::Itertools;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "incompatible types for type parameter '{type_parameter}: {types}'")]
    IncompatibleTypesForConstraint {
        type_parameter: String,
        types: DistinctTypes,
    },
    #[display(fmt = "incompatible types: expected {expected} but got {actual}")]
    IncompatibleTypes {
        expected: FenlType,
        actual: FenlType,
    },
    #[display(fmt = "not enough arguments: expected {expected} but got {actual}")]
    NotEnoughArguments { expected: usize, actual: usize },
    #[display(fmt = "incorrect argument count: expected {expected} but got {actual}")]
    IncorrectArgumentCount { expected: usize, actual: usize },
    #[display(fmt = "invalid signature: '{signature}' at position {position}: {reason}")]
    InvalidSignature {
        signature: String,
        position: usize,
        reason: String,
    },
}

impl error_stack::Context for Error {}

#[derive(Debug, derive_more::Display)]
#[display(fmt = "{}", "_0.iter().format(\", \")")]
pub struct DistinctTypes(Vec<FenlType>);

impl DistinctTypes {
    pub fn new(types: impl Iterator<Item = FenlType>) -> Self {
        let types = types.dedup().collect();
        Self(types)
    }
}

impl FromIterator<FenlType> for DistinctTypes {
    fn from_iter<T: IntoIterator<Item = FenlType>>(iter: T) -> Self {
        Self::new(iter.into_iter())
    }
}
