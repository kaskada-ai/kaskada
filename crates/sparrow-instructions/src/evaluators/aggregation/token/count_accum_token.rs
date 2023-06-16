/// Token used for the count accumulator.
///
/// Values are stored as `[pass_id, instruction_id] -> Vec<u32>`.
#[derive(Default)]
pub struct CountAccumToken {
    /// Stores the state for in-memory usage.
    accum: Vec<u32>,
}

impl CountAccumToken {
    pub(crate) fn resize(&mut self, len: usize) {
        self.accum.resize(len, 0);
    }

    pub(crate) fn get_value(&mut self, entity_index: u32) -> u32 {
        self.accum[entity_index as usize]
    }

    pub(crate) fn increment_value(&mut self, entity_index: u32) {
        self.accum[entity_index as usize] += 1;
    }

    pub(crate) fn reset_value(&mut self, entity_index: u32) {
        self.accum[entity_index as usize] = 0;
    }
}
