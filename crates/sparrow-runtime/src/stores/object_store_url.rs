use object_store::ObjectStore;
use std::{path::PathBuf, str::FromStr, sync::Arc};

use error_stack::{IntoReport, ResultExt};
use tokio_util::io::StreamReader;
use url::Url;

use itertools::Itertools;

use super::object_stores::Error;

/// A string referring to a file in an object store.
/// TODO: Debug this. It doesn't like the serialize on the URL.
// #[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
// #[serde(transparent)]
#[derive(Debug, Clone)]
#[repr(transparent)]
pub struct ObjectStoreUrl(Url);

impl ObjectStoreUrl {
    pub fn path(&self) -> error_stack::Result<object_store::path::Path, Error> {
        object_store::path::Path::parse(self.0.path())
            .into_report()
            .change_context_lazy(|| Error::UrlInvalidPath(self.0.clone()))
    }

    pub fn key(&self) -> error_stack::Result<ObjectStoreKey, Error> {
        match self.0.scheme() {
            "file" => Ok(ObjectStoreKey::Local),
            "mem" => Ok(ObjectStoreKey::Memory),
            "s3" | "s3a" => {
                let bucket = self
                    .0
                    .host_str()
                    .ok_or_else(|| Error::UrlMissingHost(self.0.clone()))?
                    .to_owned();
                // For traditional S3 paths, the `host` should be just the bucket.
                // We use this as the key. The creation of the S3 object store will
                // parse out the bucket and other parts of the URL as needed.
                Ok(ObjectStoreKey::Aws {
                    bucket,
                    region: None,
                    virtual_hosted_style_request: false,
                })
            }
            "https" => {
                let host = self
                    .0
                    .host_str()
                    .ok_or_else(|| Error::UrlMissingHost(self.0.clone()))?;

                match host.splitn(4, '.').collect_tuple() {
                    Some(("s3", bucket, "amazonaws", "com")) => Ok(ObjectStoreKey::Aws {
                        bucket: bucket.to_owned(),
                        region: None,
                        virtual_hosted_style_request: false,
                    }),
                    Some((bucket, "s3", region, "amazonaws.com")) => Ok(ObjectStoreKey::Aws {
                        bucket: bucket.to_owned(),
                        region: Some(region.to_owned()),
                        virtual_hosted_style_request: true,
                    }),
                    _ => error_stack::bail!(Error::UrlUnsupportedHost(self.0.clone())),
                }
            }
            _ => {
                error_stack::bail!(Error::UrlUnsupportedScheme(self.0.clone()))
            }
        }
    }
    pub async fn download(
        &self,
        object_store: Arc<dyn ObjectStore>,
        file_path: PathBuf,
    ) -> error_stack::Result<(), Error> {
        let path = self.path()?;
        // TODO: Update the unwrap await error. Probably need to cast it to error_stack error.
        let stream = object_store.get(&path).await.unwrap().into_stream();
        let mut file = tokio::fs::File::create(file_path.clone()).await.unwrap();
        let mut body = StreamReader::new(stream);
        let bytes = tokio::io::copy(&mut body, &mut file).await.unwrap();
        println!(
            "Successfully downloaded file: {:?} bytes downloaded to {:?}",
            bytes,
            file_path.clone()
        );
        Ok(())
    }
}

impl FromStr for ObjectStoreUrl {
    type Err = error_stack::Report<<Url as FromStr>::Err>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Url::from_str(s).into_report().map(ObjectStoreUrl)
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
pub enum ObjectStoreKey {
    Local,
    Memory,
    Aws {
        bucket: String,
        region: Option<String>,
        virtual_hosted_style_request: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_urls() {
        let url = ObjectStoreUrl::from_str("file:///foo").unwrap();
        assert_eq!(url.key().unwrap(), ObjectStoreKey::Local);
        assert_eq!(
            url.path().unwrap(),
            object_store::path::Path::parse("/foo").unwrap()
        );

        let url = ObjectStoreUrl::from_str("FILE:///foo").unwrap();
        assert_eq!(url.key().unwrap(), ObjectStoreKey::Local);
        assert_eq!(
            url.path().unwrap(),
            object_store::path::Path::parse("/foo").unwrap()
        );
    }

    #[test]
    fn test_memory_urls() {
        let url = ObjectStoreUrl::from_str("mem:///foo").unwrap();
        assert_eq!(url.key().unwrap(), ObjectStoreKey::Memory);
        assert_eq!(
            url.path().unwrap(),
            object_store::path::Path::parse("foo").unwrap()
        );

        let url = ObjectStoreUrl::from_str("mem:foo").unwrap();
        assert_eq!(url.key().unwrap(), ObjectStoreKey::Memory);
        assert_eq!(
            url.path().unwrap(),
            object_store::path::Path::parse("foo").unwrap()
        );
    }

    #[test]
    fn test_aws_urls() {
        let url = ObjectStoreUrl::from_str("s3://bucket/path").unwrap();
        assert_eq!(
            url.key().unwrap(),
            ObjectStoreKey::Aws {
                bucket: "bucket".to_owned(),
                region: None,
                virtual_hosted_style_request: false,
            }
        );
        assert_eq!(
            url.path().unwrap(),
            object_store::path::Path::parse("path").unwrap()
        );

        let url = ObjectStoreUrl::from_str("s3a://bucket/foo").unwrap();
        assert_eq!(
            url.key().unwrap(),
            ObjectStoreKey::Aws {
                bucket: "bucket".to_owned(),
                region: None,
                virtual_hosted_style_request: false,
            }
        );
        assert_eq!(
            url.path().unwrap(),
            object_store::path::Path::parse("foo").unwrap()
        );

        let url = ObjectStoreUrl::from_str("https://s3.bucket.amazonaws.com/foo").unwrap();
        assert_eq!(
            url.key().unwrap(),
            ObjectStoreKey::Aws {
                bucket: "bucket".to_owned(),
                region: None,
                virtual_hosted_style_request: false,
            }
        );
        assert_eq!(
            url.path().unwrap(),
            object_store::path::Path::parse("foo").unwrap()
        );

        let url = ObjectStoreUrl::from_str("https://bucket.s3.region.amazonaws.com/foo").unwrap();
        assert_eq!(
            url.key().unwrap(),
            ObjectStoreKey::Aws {
                bucket: "bucket".to_owned(),
                region: Some("region".to_owned()),
                virtual_hosted_style_request: true
            }
        );
        assert_eq!(
            url.path().unwrap(),
            object_store::path::Path::parse("foo").unwrap()
        );
    }
}
