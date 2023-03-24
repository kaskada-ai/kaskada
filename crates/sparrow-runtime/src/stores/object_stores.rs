use std::sync::{Arc, RwLock};

use error_stack::{IntoReport, ResultExt};
use hashbrown::HashMap;
use object_store::ObjectStore;
use url::Url;

use super::object_store_url::ObjectStoreKey;

#[derive(Default)]
pub struct ObjectStoreRegistry {
    /// Map from URL scheme to object store for that prefix.
    ///
    /// Currently, we use a single object store or each scheme. This covers
    /// cases like `file:///` using a local file store and `s3://` using an
    /// S3 file store. We may find that it is useful (or necessary) to register
    /// specific object stores for specific prefixes -- for instance, to use
    /// different credentials for different buckets within S3.
    object_stores: RwLock<HashMap<ObjectStoreKey, Arc<dyn ObjectStore>>>,
}

impl ObjectStoreRegistry {
    // TODO: Remove the public constructor
    pub fn new() -> Self {
        let object_stores = hashbrown::HashMap::new();
        ObjectStoreRegistry {
            object_stores: RwLock::new(object_stores),
        }
    }

    pub fn object_store(
        &self,
        key: ObjectStoreKey,
    ) -> error_stack::Result<Arc<dyn ObjectStore>, Error> {
        if let Some(object_store) = self.get_object_store(&key) {
            Ok(object_store)
        } else {
            let object_store = create_object_store(&key)?;
            self.put_object_store(key, object_store.clone());
            Ok(object_store)
        }
    }

    fn get_object_store(&self, key: &ObjectStoreKey) -> Option<Arc<dyn ObjectStore>> {
        let object_stores = self.object_stores.read().unwrap();
        object_stores.get(key).cloned()
    }

    fn put_object_store(&self, key: ObjectStoreKey, object_store: Arc<dyn ObjectStore>) {
        let mut object_stores = self.object_stores.write().unwrap();
        object_stores.insert(key, object_store);
    }
}

impl std::fmt::Debug for ObjectStoreRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let object_stores = self.object_stores.read().unwrap();
        f.debug_struct("ObjectStoreRegistry")
            .field("object_stores", &*object_stores)
            .finish()
    }
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "object store error")]
    ObjectStore,
    #[display(fmt = "invalid URL '{_0}'")]
    InvalidUrl(String),
    #[display(fmt = "missing host in URL '{_0}'")]
    UrlMissingHost(Url),
    #[display(fmt = "unsupported host '{}' in URL '{_0}", "_0.host().unwrap()")]
    UrlUnsupportedHost(Url),
    #[display(
        fmt = "unsupported scheme '{}' in URL '{_0}'; expected one of 'file' or 's3'",
        "_0.scheme()"
    )]
    UrlUnsupportedScheme(Url),
    #[display(fmt = "invalid path '{}' in URL '{_0}'", "_0.path()")]
    UrlInvalidPath(Url),
    #[display(fmt = "error creating object store for {_0:?}")]
    CreatingObjectStore(ObjectStoreKey),
}

impl error_stack::Context for Error {}

fn create_object_store(key: &ObjectStoreKey) -> error_stack::Result<Arc<dyn ObjectStore>, Error> {
    match key {
        ObjectStoreKey::Local => Ok(Arc::new(object_store::local::LocalFileSystem::new())),
        ObjectStoreKey::Memory => Ok(Arc::new(object_store::memory::InMemory::new())),
        ObjectStoreKey::Aws {
            bucket,
            region,
            virtual_hosted_style_request,
        } => {
            let builder = object_store::aws::AmazonS3Builder::from_env()
                .with_bucket_name(bucket)
                .with_virtual_hosted_style_request(*virtual_hosted_style_request);
            let builder = if let Some(region) = region {
                builder.with_region(region)
            } else {
                builder
            };
            let object_store = builder
                .build()
                .into_report()
                .change_context(Error::CreatingObjectStore(key.clone()))?;
            Ok(Arc::new(object_store))
        },
        ObjectStoreKey::Gcs { bucket } => {
            let builder = object_store::gcp::GoogleCloudStorageBuilder::from_env()
                .with_bucket_name(bucket);
            let object_store = builder.build().into_report().change_context(Error::CreatingObjectStore(key.clone()))?;
            Ok(Arc::new(object_store))
        },
        
    }
}

#[cfg(test)]

mod tests {
    use crate::{
        stores::{object_store_url::ObjectStoreKey, object_stores::create_object_store},
        ObjectStoreRegistry,
    };

    #[test]
    fn test_create_object_store_local() {
        let key = ObjectStoreKey::Local;
        let object_store = create_object_store(&key).unwrap();
        assert_eq!(object_store.to_string(), "LocalFileSystem(file:///)")
    }

    #[test]
    fn test_create_object_store_memory() {
        let key = ObjectStoreKey::Memory;
        let object_store = create_object_store(&key).unwrap();
        assert_eq!(object_store.to_string(), "InMemory")
    }

    #[test]
    fn test_create_object_store_aws_builder() {
        // TODO: Is there a better way to test this? Not sure to test the builder built properly from the key.
        let key = ObjectStoreKey::Aws {
            bucket: "test-bucket".to_string(),
            region: Some("test-region".to_string()),
            virtual_hosted_style_request: true,
        };
        let object_store = create_object_store(&key).unwrap();
        assert_eq!(object_store.to_string(), "AmazonS3(test-bucket)")
    }

    #[test]
    fn test_create_object_store_gcs() {
        let key = ObjectStoreKey::Gcs { bucket: "test-bucket".to_owned() };
        let object_store = create_object_store(&key).unwrap();
        assert_eq!(object_store.to_string(), "GoogleCloudStorage(test-bucket)")
    }

    #[test]
    fn test_object_store_registry_creates_if_not_exists() {
        let object_store_registry = ObjectStoreRegistry::new();
        let key = ObjectStoreKey::Local;
        // Verify there is no object store for local first
        assert!(object_store_registry.get_object_store(&key).is_none());
        // Call the public method
        let object_store = object_store_registry.object_store(key.clone());
        // Verify the result is valid and that
        assert!(object_store.is_ok());
        assert!(object_store_registry.get_object_store(&key).is_some());
    }
}
