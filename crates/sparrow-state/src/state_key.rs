#[derive(Debug, Clone)]
pub struct StateKey {
    /// Operation ID in the execution plan for this state.
    pub operation_id: u16,
    /// Index of the state within the operation.
    pub state_index: u16,
    /// Key hash of the entity associated with this state.
    pub key_hash: u64,
}

impl StateKey {
    pub fn new(operation_id: u16, state_index: u16, key_hash: u64) -> Self {
        Self {
            operation_id,
            state_index,
            key_hash,
        }
    }
}

impl AsRef<[u8]> for StateKey {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: the state key struct contains exactly enough bytes.
        unsafe {
            std::slice::from_raw_parts(
                self as *const StateKey as *const u8,
                std::mem::size_of::<StateKey>(),
            )
        }
    }
}
