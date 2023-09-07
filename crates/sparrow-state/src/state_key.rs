#[derive(Debug, Clone)]
pub struct StateKey {
    /// Key hash of the entity associated with this state.
    pub key_hash: u64,
    /// Operation ID in the execution plan for this state.
    pub operation_id: u8,
    /// Expression ID in the operation for this state.
    pub step_id: u8,
}

impl StateKey {
    pub fn new(key_hash: u64, operation_id: u8, step_id: u8) -> Self {
        StateKey {
            key_hash,
            operation_id,
            step_id,
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
