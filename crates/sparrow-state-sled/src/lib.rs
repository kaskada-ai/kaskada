use std::sync::Arc;

use error_stack::{IntoReport, ResultExt};
use sparrow_state::{DbPath, Error, StateBackend};

pub struct SledStateBackend {
    db: sled::Db,
    /// Sled tree holding list items.
    items: sled::Tree,
}

impl SledStateBackend {
    pub fn open(path: &std::path::Path) -> error_stack::Result<Arc<Self>, Error> {
        let db = sled::open(path)
            .into_report()
            .change_context(Error::CreateBackend)
            .attach_printable_lazy(|| DbPath::new(path))?;
        let items = db
            .open_tree("items")
            .into_report()
            .change_context(Error::CreateBackend)?;
        let backend = SledStateBackend { db, items };

        Ok(Arc::new(backend))
    }

    pub fn flush_batch(&self, batch: sled::Batch) -> error_stack::Result<(), Error> {
        self.db
            .apply_batch(batch)
            .into_report()
            .change_context(Error::Backend("flush"))?;
        Ok(())
    }

    pub fn batch_value_put<T: serde::Serialize>(
        &self,
        key: &sparrow_state::StateKey,
        value: Option<&T>,
        batch: &mut sled::Batch,
    ) -> error_stack::Result<(), Error> {
        match value {
            None => {
                batch.remove(key.as_ref());
            }
            Some(value) => {
                let value = bincode::serialize(value)
                    .into_report()
                    .change_context_lazy(|| Error::Serialize(key.clone()))?;
                batch.insert(key.as_ref(), value);
            }
        }
        Ok(())
    }

    fn clear_all(&self) -> error_stack::Result<(), Error> {
        self.db
            .clear()
            .into_report()
            .change_context(Error::ClearAll)
    }

    fn value_get<T: serde::de::DeserializeOwned>(
        &self,
        key: &sparrow_state::StateKey,
    ) -> error_stack::Result<Option<T>, Error> {
        let value = self
            .db
            .get(key)
            .into_report()
            .change_context_lazy(|| Error::BackendKey("get", key.clone()))?;

        match value {
            Some(value) => {
                let value: T = bincode::deserialize(value.as_ref())
                    .into_report()
                    .change_context_lazy(|| Error::Deserialize(key.clone()))?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn value_put<T: serde::Serialize>(
        &self,
        key: &sparrow_state::StateKey,
        value: Option<&T>,
    ) -> error_stack::Result<(), Error> {
        match value {
            None => {
                self.db
                    .remove(key)
                    .into_report()
                    .change_context_lazy(|| Error::BackendKey("put", key.clone()))?;
            }
            Some(value) => {
                let value = bincode::serialize(value)
                    .into_report()
                    .change_context_lazy(|| Error::Serialize(key.clone()))?;
                self.db
                    .insert(key, value)
                    .into_report()
                    .change_context_lazy(|| Error::BackendKey("put", key.clone()))?;
            }
        }
        Ok(())
    }

    fn list_get<T>(
        &self,
        key: &sparrow_state::StateKey,
    ) -> error_stack::Result<Option<Vec<T>>, Error> {
        todo!()
    }

    fn list_clear(&self, key: &sparrow_state::StateKey) -> error_stack::Result<(), Error> {
        todo!()
    }

    fn list_push<'a, T: 'a>(
        &'a self,
        key: &sparrow_state::StateKey,
        items: impl Iterator<Item = &'a T>,
    ) -> error_stack::Result<(), Error> {
        todo!()
    }

    fn list_pop_n(
        &self,
        key: &sparrow_state::StateKey,
        n: usize,
    ) -> error_stack::Result<(), Error> {
        todo!()
    }

    fn list_shrink(
        &self,
        key: &sparrow_state::StateKey,
        len: usize,
    ) -> error_stack::Result<(), Error> {
        todo!()
    }
}
