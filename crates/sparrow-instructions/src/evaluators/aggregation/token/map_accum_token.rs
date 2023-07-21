use arrow::array::{as_map_array, new_empty_array, new_null_array, Array, ArrayRef, MapArray};

use crate::{ComputeStore, StateToken, StoreKey};
/// Token used for string accumulators.
///
/// String accumulators are stored as `[passId, instId, entity_index] ->
/// Option<String>`. Values are updated entity-by-entity.
// #[derive(serde::Serialize, serde::Deserialize)]
pub struct MapAccumToken {
    /// Stores the state for in-memory usage.
    // #[serde(with = "sparrow_arrow::serde::array_ref")]
    accum: MapArray,
}

impl StateToken for MapAccumToken {
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        // store.get_to_vec(key, &mut self.accum)
        todo!()
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        // store.put(key, &self.accum)
        todo!()
    }
}

impl MapAccumToken {
    pub fn new(accum: MapArray) -> Self {
        Self { accum }
    }

    /// Concat nulls to the end of the current accumulator to grow the size.
    pub fn resize(&mut self, len: usize) -> anyhow::Result<()> {
        let diff = len - self.accum.len();
        assert!(diff >= 0, "expected to grow accumulator");

        let null_array = new_null_array(&self.accum.data_type(), diff);
        let null_array = as_map_array(null_array.as_ref());
        let new_state = arrow::compute::concat(&[&self.accum, null_array])?;
        self.accum = as_map_array(&new_state).clone();
        Ok(())
    }

    pub fn value_is_null(&mut self, key: u32) -> bool {
        self.accum.is_null(key as usize)
    }

    pub fn put_value(&mut self, key: u32, value: Option<String>) -> anyhow::Result<()> {
        // todo!();
        // self.accum[key as usize] = value;
        Ok(())
    }

    pub fn accum(&self) -> &MapArray {
        &self.accum
    }

    pub fn set_state(&mut self, new_state: MapArray) {
        self.accum = new_state;
    }
}
