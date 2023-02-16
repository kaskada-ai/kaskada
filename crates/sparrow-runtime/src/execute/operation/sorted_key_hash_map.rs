use std::collections::BTreeMap;

use arrow::array::{UInt32Array, UInt64Array};
use sparrow_instructions::{ComputeStore, GroupingIndices, StoreKey};

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SortedKeyHashMap(BTreeMap<u64, u32>);

impl SortedKeyHashMap {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn len(&self) -> usize {
        self.0.keys().len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn keys(&self) -> impl Iterator<Item = u64> + '_ {
        self.0.keys().copied()
    }

    pub fn values(&self) -> impl Iterator<Item = u32> + '_ {
        self.0.values().copied()
    }

    pub fn contains_key(&self, key: u64) -> bool {
        self.0.contains_key(&key)
    }

    pub fn extend(&mut self, key_hashes: impl Iterator<Item = u64>) {
        let mut next_index = self.0.len();
        for key_hash in key_hashes {
            self.0.entry(key_hash).or_insert_with(|| {
                let index = next_index as u32;
                next_index += 1;
                index
            });
        }
    }

    /// Return the index corresponding to each key hash.
    pub fn get_or_update_indices(
        &mut self,
        key_hashes: &UInt64Array,
    ) -> anyhow::Result<GroupingIndices> {
        // This is a bit weird. We can't mutate both the size and the map at the same
        // time, so create a local value to track "new" keys, and then update
        // later.
        let mut next_index = self.len();
        let entity_indices = key_hashes.values().iter().map(|key_hash| {
            *self.0.entry(*key_hash).or_insert_with(|| {
                let index = next_index as u32;
                next_index += 1;
                index
            })
        });

        // This would be cleaner with `from_trusted_len_iter_values` which doesn't (yet)
        // exist.
        let mut entity_indices_array = UInt32Array::builder(key_hashes.len());
        // SAFETY: Vector iterator + map has a trusted length.
        unsafe { entity_indices_array.append_trusted_len_iter(entity_indices) };

        Ok(GroupingIndices::new(
            self.len(),
            entity_indices_array.finish(),
        ))
    }

    pub fn restore_from(
        &mut self,
        operation_index: u8,
        store: &ComputeStore,
    ) -> anyhow::Result<()> {
        if let Some(key_hash_to_index) = store.get(&StoreKey::new_key_hash_set(operation_index))? {
            self.0 = key_hash_to_index
        } else {
            self.0.clear()
        }

        Ok(())
    }

    pub fn store_to(
        &self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()> {
        compute_store.put(&StoreKey::new_key_hash_set(operation_index), &self.0)?;
        Ok(())
    }
}
