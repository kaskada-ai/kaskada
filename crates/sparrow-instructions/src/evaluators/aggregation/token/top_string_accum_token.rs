use hashbrown::HashMap;

use crate::{ComputeStore, StateToken, StoreKey};

/// Token used for top string accumulators.
///
/// Top String accumulators are stored as `[passId, instId, entity_index] ->
/// HashMap<String, i64>`. Values are updated entity-by-entity.
#[derive(Default)]
pub struct TopStringAccumToken {
    // Stores the occurrences of each value for each entity.
    accum: Vec<HashMap<String, i64>>,
}

impl StateToken for TopStringAccumToken {
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.get_to_vec(key, &mut self.accum)
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self.accum)
    }
}

impl TopStringAccumToken {
    pub fn resize(&mut self, len: usize) {
        self.accum.resize(len, HashMap::new());
    }

    pub fn get_value(&mut self, key: u32) -> anyhow::Result<Option<String>> {
        // This is extremely inefficient. A better strategy might be to
        // store the current max separately, and update that on each `put_value`
        // as needed.
        //
        // A cleaner solution would be to use a priority queue/max heap.
        let max_item = self.accum[key as usize].iter().max_by(|a, b| {
            if a.1 != b.1 {
                a.1.cmp(b.1)
            } else {
                b.0.cmp(a.0)
            }
        });
        if let Some(max) = max_item {
            Ok(Some(max.0.clone()))
        } else {
            Ok(None)
        }
    }

    pub fn put_value(&mut self, key: u32, value: Option<String>) -> anyhow::Result<()> {
        if let Some(value) = value {
            self.accum[key as usize]
                .entry(value)
                .and_modify(|v| *v += 1)
                .or_insert(1);
        }
        Ok(())
    }

    /// Used to clear the set of values for an entity.
    ///
    /// When a window closes, all values for that entities should be forgotten.
    pub fn reset_value_for_entity(&mut self, key: u32) -> anyhow::Result<()> {
        self.accum[key as usize].clear();
        Ok(())
    }
}
