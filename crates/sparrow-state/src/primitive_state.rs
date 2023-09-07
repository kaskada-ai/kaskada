pub struct PrimitiveToken<T> {
    state_id: StateId,
    _phantom: PhantomData<fn(T) -> T>,
}

pub struct StateId {
    operation_id: u8,
    step_id: u8,
    index: u8,
}

impl<T: serde::ser::Serialize> PrimitiveState<T> {
    pub fn set()
}

trait InMemoryState {

}

trait StateToken {
    type InMemory: InMemoryState;

    fn read(&self, key: &StateKey, backend: &dyn StateBackend) -> Self::InMemory;
    fn write(&self, key: &StateKey, backend: &dyn StateBackend, value: &Self::InMemory);
}

impl StateToken for PrimitiveToken<i64> {

}