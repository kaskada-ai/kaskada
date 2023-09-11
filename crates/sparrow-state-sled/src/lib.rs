use speedy::{Readable, Writable};
use std::borrow::Cow;
use std::cell::RefCell;
use std::sync::Arc;

use error_stack::{IntoReport, ResultExt};
use sparrow_state::{
    DbPath, Error, Keys, PrimitiveState, ReadState, StateBackend, StateKey, StateWriter, WriteState,
};

pub struct SledStateBackend {
    db: sled::Db,
    /// Sled tree holding list items.
    items: sled::Tree,
}

impl StateBackend for SledStateBackend {
    fn clear_all(&self) -> error_stack::Result<(), Error> {
        self.db
            .clear()
            .into_report()
            .change_context(Error::ClearAll)
    }

    fn writer(&self) -> error_stack::Result<Box<dyn sparrow_state::StateWriter>, Error> {
        Ok(Box::new(SledStateBatch {
            db: self.db.clone(),
            batch: Default::default(),
        }))
    }
}

impl WriteState<PrimitiveState<i64>> for SledStateBatch {
    fn write(
        &mut self,
        state_index: u16,
        keys: &Keys,
        value: PrimitiveState<i64>,
    ) -> error_stack::Result<(), Error> {
        debug_assert_eq!(keys.num_key_hashes(), value.len());

        let mut key = StateKey {
            operation_id: keys.operation_id,
            state_index,
            key_hash: 0,
        };

        for (key_hash, (non_null, value)) in keys.unique_key_hashes.iter().zip(value.values2()) {
            key.key_hash = *key_hash;

            if non_null {
                let value = value
                    .write_to_vec()
                    .into_report()
                    .change_context(Error::Serialize)?;
                self.batch.insert(key.as_ref(), value);
            } else {
                self.batch.remove(key.as_ref());
            }
        }

        Ok(())
    }
}

impl ReadState<PrimitiveState<i64>> for SledStateBackend {
    fn read(
        &self,
        state_index: u16,
        keys: &Keys,
    ) -> error_stack::Result<PrimitiveState<i64>, Error> {
        let mut state = PrimitiveState::with_capacity(keys.num_key_hashes());
        for key in keys.state_keys(state_index) {
            let value = self
                .db
                .get(key)
                .into_report()
                .change_context_lazy(|| Error::Backend("get"))?;

            match value {
                Some(value) => {
                    let value: i64 = i64::read_from_buffer(value.as_ref())
                        .into_report()
                        .change_context(Error::Deserialize)?;
                    state.push_some(value);
                }
                None => {
                    state.push_none();
                }
            }
        }

        Ok(state)
    }
}

impl ReadState<PrimitiveState<f64>> for SledStateBackend {
    fn read(
        &self,
        state_index: u16,
        keys: &Keys,
    ) -> error_stack::Result<PrimitiveState<f64>, Error> {
        todo!()
    }
}

impl WriteState<PrimitiveState<f64>> for SledStateBatch {
    fn write(
        &mut self,
        state_index: u16,
        keys: &Keys,
        value: PrimitiveState<f64>,
    ) -> error_stack::Result<(), Error> {
        todo!()
    }
}
pub struct SledStateBatch {
    db: sled::Db,
    batch: sled::Batch,
}

impl StateWriter for SledStateBatch {
    fn finish(self) -> error_stack::Result<(), Error> {
        self.db
            .apply_batch(self.batch)
            .into_report()
            .change_context(Error::Backend("finish batch"))
    }
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
}
