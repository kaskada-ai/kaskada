use crate::{ComputeStore, StoreKey};

/// Trait implemented by tokens for flushing and resuming from a state.
pub trait StateToken {
    fn restore(&mut self, key: &StoreKey, compute_store: &ComputeStore) -> anyhow::Result<()>;

    fn store(&self, key: &StoreKey, compute_store: &ComputeStore) -> anyhow::Result<()>;
}
