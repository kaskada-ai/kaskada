use crate::{ComputeStore, StateToken, StoreKey};
/// Token used for string accumulators.
///
/// String accumulators are stored as `[passId, instId, entity_index] ->
/// Option<String>`. Values are updated entity-by-entity.
#[derive(Default)]
pub struct StringAccumToken {
    /// Stores the state for in-memory usage.
    accum: Vec<Option<String>>,
}

impl StateToken for StringAccumToken {
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.get_to_vec(key, &mut self.accum)
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self.accum)
    }
}

impl StringAccumToken {
    pub fn resize(&mut self, len: usize) {
        self.accum.resize(len, None);
    }

    pub fn get_value(&mut self, key: u32) -> anyhow::Result<Option<String>> {
        Ok(self.accum[key as usize].clone())
    }

    pub fn put_value(&mut self, key: u32, value: Option<String>) -> anyhow::Result<()> {
        self.accum[key as usize] = value;
        Ok(())
    }
}
