use std::borrow::Cow;

use bytes::Bytes;

use crate::primitive_state::PrimitiveState;
use crate::{Error, Keys, StateKey};

pub trait ReadState<T> {
    fn read(&self, state_index: u16, keys: &Keys) -> error_stack::Result<T, Error>;
}

pub trait WriteState<T> {
    fn write(&mut self, state_index: u16, keys: &Keys, value: T) -> error_stack::Result<(), Error>;
}

/// The `StateBackend` is responsible for how computation states are persisted.
pub trait StateBackend: ReadState<PrimitiveState<i64>> + ReadState<PrimitiveState<f64>> {
    fn clear_all(&self) -> error_stack::Result<(), Error>;
    fn writer(&self) -> error_stack::Result<Box<dyn StateWriter>, Error>;
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

pub trait StateWriter:
    WriteState<PrimitiveState<i64>> + WriteState<PrimitiveState<f64>> + 'static
{
    fn finish(self) -> error_stack::Result<(), Error>;
}
