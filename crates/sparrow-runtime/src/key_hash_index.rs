use arrow::array::{UInt32Array, UInt64Array};
use hashbrown::HashMap;
use sparrow_instructions::{ComputeStore, GroupingIndices, StoreKey};

/// Stores an index mapping key hashes to an index.
///
/// Multiple key hash indices may be needed for the same grouping. Consider
/// a computation such as `TableFoo | when(predicate) | ...` which contains
/// two passes -- one over the contents of `TableFoo` and the other over the
/// contents matching the `predicate`. Even though the grouping is the same,
/// the second pass will only contain entities that have had at least one
/// event matching the predicate.
///
/// Currently, we handle this by having a separate `KeyHashIndex` for each
/// pass. If this becomes too much to store, we could explore strategies
/// that allow sharing them -- when possible. For instance:
///
/// 1. We could map entities to indices in a bit set, and then have each
///    pass maintain a bit set indicating which entities have been seen.
///    We could determine the actual index based on the number of `1s`
///    in the range `[0, index_in_bit_set]`.
/// 2. We could determine cases where the index *should* be the same. For
///    instance, if anything is buffered between the first pass and the
///    second then all entities discovered by the first pass will be
///    discovered by the second.
#[derive(Default)]
pub struct KeyHashIndex {
    /// Map from key hash to dense integers. This allows instruction executors
    /// to use vectors with integer keys for storing per-key values.
    // TODO: Consider an `IntMap` to make the index lookup faster.
    key_hash_to_index: HashMap<u64, u32, ahash::RandomState>,
}

impl std::fmt::Debug for KeyHashIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyHashIndex")
            .field(
                "key_hash_to_index",
                &format!("{} entries", self.key_hash_to_index.len()),
            )
            .finish()
    }
}

impl KeyHashIndex {
    pub fn restore_from(
        &mut self,
        operation_index: u8,
        store: &ComputeStore,
    ) -> anyhow::Result<()> {
        if let Some(key_hash_to_index) =
            store.get(&StoreKey::new_key_hash_to_index(operation_index))?
        {
            self.key_hash_to_index = key_hash_to_index
        } else {
            self.key_hash_to_index.clear()
        }

        Ok(())
    }

    pub fn store_to(
        &self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()> {
        compute_store.put(
            &StoreKey::new_key_hash_to_index(operation_index),
            &self.key_hash_to_index,
        )?;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.key_hash_to_index.len()
    }

    /// Return the index corresponding to each key hash.
    ///
    /// The indices assigned to key hashes will be "dense". The first key hash
    /// encountered will be assigned index 0, the second index 1, etc. Thus
    /// the indices are suitable for storing information about the entities
    /// in a vector.
    pub fn get_or_update_indices(
        &mut self,
        key_hashes: &UInt64Array,
    ) -> anyhow::Result<GroupingIndices> {
        // This is a bit weird. We can't mutate both the size and the map at the same
        // time, so create a local value to track "new" keys, and then update
        // later.
        let mut next_index = self.key_hash_to_index.len();
        let entity_indices = key_hashes.values().iter().map(|key_hash| {
            *self.key_hash_to_index.entry(*key_hash).or_insert_with(|| {
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
}
