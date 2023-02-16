use bitvec::prelude::BitVec;
use serde::{Deserialize, Serialize};

use crate::{ComputeStore, StateToken, StoreKey};

/// Token used for boolean accumulators.
///
/// Boolean accumulators are stored in two `BitVecs`, one validity bit
/// and one value bit.
#[derive(Default)]
pub struct BooleanAccumToken {
    /// Serializable state of the accumulator.
    accum: SerializableBooleanAccum,
}

#[derive(Serialize, Deserialize, Default)]
struct SerializableBooleanAccum {
    /// Stores the value.
    accum: BitVec,

    /// Indicates whether the bit at the specified index in `accum` is valid.
    is_valid: BitVec,
}

impl SerializableBooleanAccum {
    fn clear(&mut self) {
        self.accum.clear();
        self.is_valid.clear();
    }

    fn resize(&mut self, len: usize) {
        self.accum.resize(len, false);
        self.is_valid.resize(len, false);
    }

    fn is_set(&self, index: u32) -> bool {
        self.is_valid[index as usize]
    }

    fn set(&mut self, index: u32, value: bool) {
        self.is_valid.set(index as usize, true);
        self.accum.set(index as usize, value);
    }

    fn unset(&mut self, index: u32) {
        // No need to unset the `accum`, as the `is_valid` bit is marked as false.
        self.is_valid.set(index as usize, false);
    }

    pub(crate) fn get_optional_value(&mut self, entity_index: u32) -> anyhow::Result<Option<bool>> {
        if self.is_set(entity_index) {
            Ok(Some(self.accum[entity_index as usize]))
        } else {
            Ok(None)
        }
    }
}

impl StateToken for BooleanAccumToken {
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        if let Some(accum) = store.get(key)? {
            self.accum = accum;
        } else {
            self.accum.clear();
        }

        Ok(())
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self.accum)
    }
}

impl BooleanAccumToken {
    pub(crate) fn resize(&mut self, len: usize) {
        self.accum.resize(len);
    }

    fn set(&mut self, index: u32, value: bool) {
        self.accum.set(index, value);
    }

    fn unset(&mut self, index: u32) {
        self.accum.unset(index);
    }

    pub(crate) fn get_optional_value(&mut self, entity_index: u32) -> anyhow::Result<Option<bool>> {
        if self.accum.is_set(entity_index) {
            self.accum.get_optional_value(entity_index)
        } else {
            Ok(None)
        }
    }

    pub(crate) fn put_optional_value(
        &mut self,
        entity_index: u32,
        value: Option<bool>,
    ) -> anyhow::Result<()> {
        if let Some(v) = value {
            self.set(entity_index, v);
        } else {
            self.unset(entity_index);
        }
        Ok(())
    }
}
