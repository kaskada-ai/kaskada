use std::path;
use std::sync::Arc;

use dashmap::DashMap;
use derive_more::Display;
use error_stack::{IntoReport, ResultExt};
use object_store::ObjectStore;
use tokio::io::AsyncWriteExt;

use crate::stores::object_store_key::ObjectStoreKey;
use crate::stores::ObjectStoreUrl;

/// If a file is smaller than this, use upload rather than multipart upload.
const SINGLE_PART_UPLOAD_LIMIT_BYTES: u64 = 5_000_000;

/// Map from URL scheme to object store for that prefix.
///
/// Currently, we use a single object store or each scheme. This covers
/// cases like `file:///` using a local file store and `s3://` using an
/// S3 file store. We may find that it is useful (or necessary) to register
/// specific object stores for specific prefixes -- for instance, to use
/// different credentials for different buckets within S3.
///
/// For now, the registry exists as a cache for the clients due to the overhead
/// required to create the cache. The future goal for the registry is to
/// control the number of possibile open connections.
#[derive(Default, Debug)]
pub struct ObjectStoreRegistry {
    object_stores: DashMap<ObjectStoreKey, Arc<dyn ObjectStore>>,
}

impl ObjectStoreRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn object_store(
        &self,
        url: &ObjectStoreUrl,
    ) -> error_stack::Result<Arc<dyn ObjectStore>, Error> {
        let key = ObjectStoreKey::from_url(url.url()).change_context(Error::InvalidObjectStore)?;
        match self.object_stores.entry(key) {
            dashmap::mapref::entry::Entry::Occupied(entry) => Ok(entry.get().clone()),
            dashmap::mapref::entry::Entry::Vacant(vacant) => {
                let object_store = create_object_store(vacant.key())?;
                Ok(vacant.insert(object_store).value().clone())
            }
        }
    }

    pub async fn upload(
        &self,
        source_path: &path::Path,
        destination_url: ObjectStoreUrl,
    ) -> error_stack::Result<(), Error> {
        let target_path = destination_url.path()?;
        let object_store = self.object_store(&destination_url)?;

        let upload_error = || Error::UploadingObject {
            from: source_path.to_path_buf(),
            to: destination_url.clone(),
        };
        let metadata = tokio::fs::metadata(source_path)
            .await
            .into_report()
            .change_context_lazy(upload_error)?;

        let length = metadata.len();

        tracing::info!(
            "Uploading {length} bytes from {} to {destination_url}",
            source_path.display()
        );

        if length <= SINGLE_PART_UPLOAD_LIMIT_BYTES {
            let bytes = tokio::fs::read(source_path)
                .await
                .into_report()
                .change_context_lazy(upload_error)?;
            let bytes = bytes::Bytes::from(bytes);
            object_store
                .put(&target_path, bytes)
                .await
                .into_report()
                .change_context_lazy(upload_error)?;
        } else {
            let mut source = tokio::fs::File::open(source_path)
                .await
                .into_report()
                .change_context_lazy(upload_error)?;

            let (_id, mut destination) = object_store
                .put_multipart(&target_path)
                .await
                .into_report()
                .change_context_lazy(upload_error)?;
            tokio::io::copy(&mut source, &mut destination)
                .await
                .into_report()
                .change_context_lazy(upload_error)?;
            destination
                .shutdown()
                .await
                .into_report()
                .change_context_lazy(upload_error)?;
        }

        Ok(())
    }

    /// Download the given object to the given local file path.
    pub async fn download(
        &self,
        source_url: ObjectStoreUrl,
        destination_path: &path::Path,
    ) -> error_stack::Result<(), Error> {
        let path = source_url.path()?;
        let object_store = self.object_store(&source_url)?;

        let download_error = || Error::DownloadingObject {
            from: source_url.clone(),
            to: destination_path.to_owned(),
        };
        let stream = object_store
            .get(&path)
            .await
            .into_report()
            .change_context_lazy(download_error)?
            .into_stream();
        let mut destination = tokio::fs::File::create(destination_path)
            .await
            .into_report()
            .change_context_lazy(download_error)?;
        let mut source = tokio_util::io::StreamReader::new(stream);
        let length = tokio::io::copy(&mut source, &mut destination)
            .await
            .into_report()
            .change_context_lazy(download_error)?;

        tracing::info!(
            "Downloaded {length} bytes from {source_url} to {}",
            destination_path.display()
        );
        Ok(())
    }
}

#[derive(Display, Debug)]
pub enum Error {
    #[display(fmt = "read-write lock object store error")]
    ReadWriteObjectStore,
    #[display(fmt = "object store error")]
    ObjectStore,
    #[display(fmt = "invalid URL '{_0}'")]
    InvalidUrl(String),
    #[display(fmt = "invalid object stroe")]
    InvalidObjectStore,
    #[display(fmt = "error creating object store")]
    CreatingObjectStore,
    #[display(fmt = "downloading object from '{from}' to '{}'", "to.display()")]
    DownloadingObject {
        from: ObjectStoreUrl,
        to: path::PathBuf,
    },
    #[display(fmt = "uploading object from '{}' to '{to}'", "from.display()")]
    UploadingObject {
        from: path::PathBuf,
        to: ObjectStoreUrl,
    },
    #[display(fmt = "internal error")]
    Internal,
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
                .change_context(Error::CreatingObjectStore)?;
            Ok(Arc::new(object_store))
        }
        ObjectStoreKey::Gcs { bucket } => {
            let builder =
                object_store::gcp::GoogleCloudStorageBuilder::from_env().with_bucket_name(bucket);
            let object_store = builder
                .build()
                .into_report()
                .change_context(Error::CreatingObjectStore)?;
            Ok(Arc::new(object_store))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use crate::stores::object_store_key::ObjectStoreKey;
    use crate::stores::ObjectStoreUrl;
    use crate::stores::{registry::create_object_store, ObjectStoreRegistry};

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
        let key = ObjectStoreKey::Gcs {
            bucket: "test-bucket".to_owned(),
        };
        let object_store = create_object_store(&key).unwrap();
        assert_eq!(object_store.to_string(), "GoogleCloudStorage(test-bucket)")
    }

    #[test]
    fn test_object_store_registry_creates_if_not_exists() {
        let object_store_registry = ObjectStoreRegistry::new();
        let key = ObjectStoreKey::Local;
        let url = ObjectStoreUrl::from_str("file:///foo").unwrap();
        assert_eq!(key, ObjectStoreKey::from_url(url.url()).unwrap());

        // Verify there is no object store for local first
        assert!(!object_store_registry.object_stores.contains_key(&key));
        // Call the public method
        let object_store = object_store_registry.object_store(&url);
        // Verify the result is valid and that
        assert!(object_store.is_ok());
        assert!(object_store_registry.object_stores.contains_key(&key));
    }
}
