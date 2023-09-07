use crate::{Error, StateKey};

/// The `StateBackend` is responsible for how computation states are persisted.
pub trait StateBackend {
    // fn clear_all(&self) -> error_stack::Result<(), Error>;

    // fn value_get<T: serde::de::DeserializeOwned>(
    //     &self,
    //     key: &StateKey,
    // ) -> error_stack::Result<Option<T>, Error>;
    // fn value_put<T: serde::Serialize>(
    //     &self,
    //     key: &StateKey,
    //     value: Option<&[u8]>,
    // ) -> error_stack::Result<(), Error>;

    // fn list_get<T: serde::de::DeserializeOwned>(
    //     &self,
    //     key: &StateKey,
    // ) -> error_stack::Result<Option<Vec<T>>, Error>;
    // fn list_clear(&self, key: &StateKey) -> error_stack::Result<(), Error>;
    // fn list_push<'a, T: 'a + serde::Serialize>(
    //     &'a self,
    //     key: &StateKey,
    //     items: impl Iterator<Item = &'a T>,
    // ) -> error_stack::Result<(), Error>;
    // fn list_pop_n(&self, key: &StateKey, n: usize) -> error_stack::Result<(), Error>;
    // fn list_shrink(&self, key: &StateKey, len: usize) -> error_stack::Result<(), Error>;
}
