/// Wrapper around an ObjectStore from the `object_store` crate.
pub struct ObjectStore(Arc<dyn object_store::ObjectStore>);
