use std::{path::Path, str::FromStr};

use error_stack::{IntoReport, ResultExt};
use serde::{Deserialize, Serialize};

use tokio_util::io::StreamReader;
use url::Url;

use super::{object_stores::Error, ObjectStoreRegistry};
use itertools::Itertools;

/// A string referring to a file in an object store.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ObjectStoreUrl {
    url: Url,
}

impl ObjectStoreUrl {
    /// Return the [object_store::path::Path] corresponding to this URL.
    pub fn path(&self) -> error_stack::Result<object_store::path::Path, Error> {
        object_store::path::Path::parse(self.url.path())
            .into_report()
            .change_context_lazy(|| Error::UrlInvalidPath(self.url.clone()))
    }

    pub fn url(&self) -> &Url {
        &self.url
    }

    /// Parse a string as an URL, with this URL as the base URL.
    ///
    /// Note: a trailing slash is significant.
    /// Without it, the last path component is considered to be a “file” name
    /// to be removed to get at the “directory” that is used as the base:
    ///
    /// # Errors
    ///
    /// If the function can not parse an URL from the given string
    /// with this URL as the base URL, an [Error] variant will be returned.
    pub fn join(&self, input: &str) -> error_stack::Result<Self, Error> {
        let url = self
            .url
            .join(input)
            .into_report()
            .change_context(Error::InvalidUrl(input.to_owned()))?;
        Ok(Self { url })
    }

    pub fn is_local(&self) -> bool {
        self.url.scheme() == "file"
    }

    pub fn key(&self) -> error_stack::Result<ObjectStoreKey, Error> {
        match self.url.scheme() {
            "file" => Ok(ObjectStoreKey::Local),
            "mem" => Ok(ObjectStoreKey::Memory),
            // S3 is the traditional S3 prefix for reading from S3.
            // S3a is the protocol designed for scalability with Hadoop reading in mind.
            // See: https://aws.amazon.com/blogs/opensource/community-collaboration-the-s3a-story/
            "s3" | "s3a" => {
                let bucket = self
                    .url
                    .host_str()
                    .ok_or_else(|| Error::UrlMissingHost(self.url.clone()))?
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
                    .url
                    .host_str()
                    .ok_or_else(|| Error::UrlMissingHost(self.url.clone()))?;

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
                    Some((bucket, "storage", "googleapis", "com")) => Ok(ObjectStoreKey::Gcs {
                        bucket: bucket.to_owned(),
                    }),
                    Some(("storage", "cloud", "google", "com")) => {
                        let mut path = self
                            .url
                            .path_segments()
                            .ok_or_else(|| Error::UrlInvalidPath(self.url.clone()))?;
                        let bucket = path
                            .next()
                            .ok_or_else(|| Error::UrlInvalidPath(self.url.clone()))?;
                        Ok(ObjectStoreKey::Gcs {
                            bucket: bucket.to_owned(),
                        })
                    }
                    _ => error_stack::bail!(Error::UrlUnsupportedHost(self.url.clone())),
                }
            }
            "gs" => {
                let bucket = self
                    .url
                    .host_str()
                    .ok_or_else(|| Error::UrlMissingHost(self.url.clone()))?
                    .to_owned();
                Ok(ObjectStoreKey::Gcs { bucket })
            }
            _ => {
                error_stack::bail!(Error::UrlUnsupportedScheme(self.url.clone()))
            }
        }
    }

    /// Download the given object to the given local file path.
    pub async fn download(
        &self,
        object_store_registry: &ObjectStoreRegistry,
        file_path: &Path,
    ) -> error_stack::Result<(), Error> {
        let path = self.path()?;
        let object_store = object_store_registry.object_store(self)?;

        let download_error = || Error::DownloadingObject {
            from: self.clone(),
            to: file_path.to_owned(),
        };
        let stream = object_store
            .get(&path)
            .await
            .into_report()
            .change_context_lazy(download_error)?
            .into_stream();
        let mut file = tokio::fs::File::create(file_path)
            .await
            .into_report()
            .change_context_lazy(download_error)?;
        let mut body = StreamReader::new(stream);
        tokio::io::copy(&mut body, &mut file)
            .await
            .into_report()
            .change_context_lazy(download_error)?;
        Ok(())
    }
}

#[derive(derive_more::Display, Debug)]
#[display(fmt = "failed to parse URL '{_0}'")]
pub struct ParseError(String);

impl error_stack::Context for ParseError {}

impl FromStr for ObjectStoreUrl {
    type Err = error_stack::Report<ParseError>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Url::from_str(s)
            .into_report()
            .change_context(ParseError(s.to_owned()))
            .map(|it| ObjectStoreUrl { url: it })
    }
}

impl std::fmt::Display for ObjectStoreUrl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.url.fmt(f)
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
    Gcs {
        bucket: String,
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

    #[test]
    fn test_gcp_urls() {
        let url = ObjectStoreUrl::from_str("gs://bucket/path").unwrap();
        assert_eq!(
            url.key().unwrap(),
            ObjectStoreKey::Gcs {
                bucket: "bucket".to_owned()
            }
        );

        let url = ObjectStoreUrl::from_str("https://bucket.storage.googleapis.com/path").unwrap();
        assert_eq!(
            url.key().unwrap(),
            ObjectStoreKey::Gcs {
                bucket: "bucket".to_owned()
            }
        );

        let url = ObjectStoreUrl::from_str("https://storage.cloud.google.com/bucket/path").unwrap();
        assert_eq!(
            url.key().unwrap(),
            ObjectStoreKey::Gcs {
                bucket: "bucket".to_owned()
            }
        );
    }
}
