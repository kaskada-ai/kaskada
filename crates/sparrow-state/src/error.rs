use crate::StateKey;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to create state backend")]
    CreateBackend,
    #[display(fmt = "failed to clear all")]
    ClearAll,
    #[display(fmt = "failed to perform '{_0}' on backend for key {_1:?}")]
    BackendKey(&'static str, StateKey),
    #[display(fmt = "failed to perform '{_0}' on backend")]
    Backend(&'static str),
    #[display(fmt = "failed to serialize value for key {_0:?}")]
    Serialize(StateKey),
    #[display(fmt = "failed to deserialize value for key {_0:?}")]
    Deserialize(StateKey),
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
