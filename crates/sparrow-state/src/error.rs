use crate::StateKey;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to create state backend")]
    CreateBackend,
    #[display(fmt = "failed to clear all")]
    ClearAll,
    #[display(fmt = "failed to perform '{_0}' on key {_1:?}")]
    Backend(Operation, StateKey),
    #[display(fmt = "failed to serialize value for key {_0:?}")]
    Serialize(StateKey),
    #[display(fmt = "failed to deserialize value for key {_0:?}")]
    Deserialize(StateKey),
}

#[derive(Debug, Clone, Copy)]
pub enum Operation {
    Get,
    Put,
    Clear,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Operation::Get => write!(f, "get"),
            Operation::Put => write!(f, "put"),
            Operation::Clear => write!(f, "clear"),
        }
    }
}

impl error_stack::Context for Error {}

pub struct DbPath(std::path::PathBuf);

impl DbPath {
    pub fn new(path: &std::path::Path) -> Self {
        DbPath(path.to_owned())
    }
}

impl std::fmt::Display for DbPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.display())
    }
}

impl std::fmt::Debug for DbPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}
