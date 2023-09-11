use hashbrown::hash_map::Entry;
use hashbrown::HashMap;

use crate::StateKey;

/// The keys in a given stateful operation.
pub struct Keys {
    pub operation_id: u16,
    /// The unique entity key hashes in the operation.
    pub unique_key_hashes: Vec<u64>,
    /// Indices into `unique_key_hashes` for each entity key in the operation.
    pub key_indices: Vec<usize>,
}

impl Keys {
    pub fn new(operation_id: u16, key_hashes: &[u64]) -> Self {
        let mut keys: HashMap<u64, usize> = HashMap::with_capacity(key_hashes.len());
        let mut key_indices = Vec::with_capacity(key_hashes.len());
        let mut unique_key_hashes = Vec::with_capacity(key_hashes.len());
        for key_hash in key_hashes {
            let key_index = match keys.entry(*key_hash) {
                Entry::Occupied(occupied) => *occupied.get(),
                Entry::Vacant(vacant) => {
                    let key_index = unique_key_hashes.len();
                    unique_key_hashes.push(*key_hash);
                    vacant.insert(key_index);
                    key_index
                }
            };
            key_indices.push(key_index)
        }
        unique_key_hashes.shrink_to_fit();
        Self {
            operation_id,
            unique_key_hashes,
            key_indices,
        }
    }

    /// Return the number of unique key hashes in this `Keys`.
    pub fn num_key_hashes(&self) -> usize {
        self.unique_key_hashes.len()
    }

    /// Return the number of rows represented by this `Keys`.
    pub fn num_rows(&self) -> usize {
        self.key_indices.len()
    }

    pub fn state_keys(&self, state_index: u16) -> impl Iterator<Item = StateKey> + '_ {
        let operation_id = self.operation_id;
        self.unique_key_hashes
            .iter()
            .copied()
            .map(move |key_hash| StateKey {
                operation_id,
                state_index,
                key_hash,
            })
    }
}
