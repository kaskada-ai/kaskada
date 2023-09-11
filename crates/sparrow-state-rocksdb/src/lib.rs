use speedy::{Readable, Writable};
use std::sync::Arc;

use error_stack::{IntoReport, ResultExt};
use sparrow_state::{
    DbPath, Error, Keys, PrimitiveState, ReadState, StateBackend, StateWriter, WriteState,
};

#[derive(Clone)]
pub struct RocksdbStateBackend {
    db: Arc<rocksdb::DB>,
}

impl StateBackend for RocksdbStateBackend {
    fn clear_all(&self) -> error_stack::Result<(), Error> {
        todo!()
    }

    fn writer(&self) -> error_stack::Result<Box<dyn sparrow_state::StateWriter>, Error> {
        Ok(Box::new(RocksdbStateBatch {
            db: self.db.clone(),
            batch: rocksdb::WriteBatch::default(),
        }))
    }
}

impl WriteState<PrimitiveState<i64>> for RocksdbStateBatch {
    fn write(
        &mut self,
        state_index: u16,
        keys: &Keys,
        value: PrimitiveState<i64>,
    ) -> error_stack::Result<(), Error> {
        debug_assert_eq!(keys.num_key_hashes(), value.len());

        for (key, value) in keys.state_keys(state_index).zip(value.values()) {
            match value {
                None => {
                    self.batch.delete(key);
                }
                Some(value) => {
                    let value = value
                        .write_to_vec()
                        .into_report()
                        .change_context(Error::Serialize)?;
                    self.batch.put(key, value);
                }
            }
        }

        Ok(())
    }
}

impl ReadState<PrimitiveState<i64>> for RocksdbStateBackend {
    fn read(
        &self,
        state_index: u16,
        keys: &Keys,
    ) -> error_stack::Result<PrimitiveState<i64>, Error> {
        let mut state = PrimitiveState::with_capacity(keys.num_key_hashes());

        let values = self.db.multi_get(keys.state_keys(state_index));
        for value in values {
            match value.into_report().change_context(Error::Backend("get"))? {
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

impl ReadState<PrimitiveState<f64>> for RocksdbStateBackend {
    fn read(
        &self,
        state_index: u16,
        keys: &Keys,
    ) -> error_stack::Result<PrimitiveState<f64>, Error> {
        todo!()
    }
}

impl WriteState<PrimitiveState<f64>> for RocksdbStateBatch {
    fn write(
        &mut self,
        state_index: u16,
        keys: &Keys,
        value: PrimitiveState<f64>,
    ) -> error_stack::Result<(), Error> {
        todo!()
    }
}
pub struct RocksdbStateBatch {
    db: Arc<rocksdb::DB>,
    batch: rocksdb::WriteBatch,
}

impl StateWriter for RocksdbStateBatch {
    fn finish(self) -> error_stack::Result<(), Error> {
        self.db
            .write(self.batch)
            .into_report()
            .change_context_lazy(|| Error::Backend("finish batch"))?;
        Ok(())
    }
}

impl RocksdbStateBackend {
    pub fn open(path: &std::path::Path) -> error_stack::Result<Self, Error> {
        let mut options = rocksdb::Options::default();
        options.create_if_missing(true);

        let db = rocksdb::DB::open(&options, path)
            .into_report()
            .change_context(Error::CreateBackend)
            .attach_printable_lazy(|| DbPath::new(path))?;
        let backend = RocksdbStateBackend { db: Arc::new(db) };

        Ok(backend)
    }
}
