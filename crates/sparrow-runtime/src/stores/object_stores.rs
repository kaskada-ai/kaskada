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
        }
    }
}
