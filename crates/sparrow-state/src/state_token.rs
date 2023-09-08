use hashbrown::hash_map::Entry;
use hashbrown::HashMap;

pub struct Keys {
    pub operation_id: u8,
    pub unique_key_hashes: Vec<u64>,
    pub key_indices: Vec<usize>,
}

impl Keys {
    pub fn new(operation_id: u8, key_hashes: &[u64]) -> Self {
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
}
