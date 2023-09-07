pub struct StateStore {
    pub(crate) backend: Arc<dyn StateBackend>,
}

impl StateStore {
    pub fn new(backend: Arc<dyn StateBackend>) -> Self {
        StateStore { backend }
    }
}
