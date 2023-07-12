use itertools::Itertools;
use url::Url;

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "missing host in URL '{_0}'")]
    MissingHost(Url),
    #[display(fmt = "unsupported scheme '{}' in URL '{_0}'", "_0.scheme()")]
    UnsupportedScheme(Url),
    #[display(fmt = "invalid path '{}' in URL '{_0}'", "_0.path()")]
    InvalidPath(Url),
    #[display(fmt = "missing host in URL '{_0}'")]
    UnsupportedHost(Url),
}

impl error_stack::Context for Error {}

#[derive(Debug, Hash, Eq, PartialEq, Clone, serde::Serialize, serde::Deserialize)]
pub(super) enum ObjectStoreKey {
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

impl ObjectStoreKey {
    pub(super) fn from_url(url: &Url) -> error_stack::Result<Self, Error> {
        match url.scheme() {
            "file" => Ok(Self::Local),
            "mem" => Ok(Self::Memory),
            // S3 is the traditional S3 prefix for reading from S3.
            // S3a is the protocol designed for scalability with Hadoop reading in mind.
            // See: https://aws.amazon.com/blogs/opensource/community-collaboration-the-s3a-story/
            "s3" | "s3a" => {
                let bucket = url
                    .host_str()
                    .ok_or_else(|| Error::MissingHost(url.clone()))?
                    .to_owned();
                // For traditional S3 paths, the `host` should be just the bucket.
                // We use this as the key. The creation of the S3 object store will
                // parse out the bucket and other parts of the URL as needed.
                Ok(Self::Aws {
                    bucket,
                    region: None,
                    virtual_hosted_style_request: false,
                })
            }
            "https" => {
                let host = url
                    .host_str()
                    .ok_or_else(|| Error::MissingHost(url.clone()))?;

                match host.splitn(4, '.').collect_tuple() {
                    Some(("s3", bucket, "amazonaws", "com")) => Ok(Self::Aws {
                        bucket: bucket.to_owned(),
                        region: None,
                        virtual_hosted_style_request: false,
                    }),
                    Some((bucket, "s3", region, "amazonaws.com")) => Ok(Self::Aws {
                        bucket: bucket.to_owned(),
                        region: Some(region.to_owned()),
                        virtual_hosted_style_request: true,
                    }),
                    Some((bucket, "storage", "googleapis", "com")) => Ok(Self::Gcs {
                        bucket: bucket.to_owned(),
                    }),
                    Some(("storage", "cloud", "google", "com")) => {
                        let mut path = url
                            .path_segments()
                            .ok_or_else(|| Error::InvalidPath(url.clone()))?;
                        let bucket = path.next().ok_or_else(|| Error::InvalidPath(url.clone()))?;
                        Ok(Self::Gcs {
                            bucket: bucket.to_owned(),
                        })
                    }
                    _ => error_stack::bail!(Error::UnsupportedHost(url.clone())),
                }
            }
            "gs" => {
                let bucket = url
                    .host_str()
                    .ok_or_else(|| Error::MissingHost(url.clone()))?
                    .to_owned();
                Ok(Self::Gcs { bucket })
            }
            _ => {
                error_stack::bail!(Error::UnsupportedScheme(url.clone()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_from_url(url: &str) -> error_stack::Result<ObjectStoreKey, Error> {
        let url = Url::parse(url).expect("valid URL");
        ObjectStoreKey::from_url(&url)
    }

    #[test]
    fn test_local_urls() {
        assert_eq!(key_from_url("file:///foo").unwrap(), ObjectStoreKey::Local);
        assert_eq!(key_from_url("FILE:///foo").unwrap(), ObjectStoreKey::Local);
    }

    #[test]
    fn test_memory_urls() {
        assert_eq!(key_from_url("mem:///foo").unwrap(), ObjectStoreKey::Memory);
        assert_eq!(key_from_url("mem:foo").unwrap(), ObjectStoreKey::Memory);
    }

    #[test]
    fn test_aws_urls() {
        assert_eq!(
            key_from_url("s3://bucket/path").unwrap(),
            ObjectStoreKey::Aws {
                bucket: "bucket".to_owned(),
                region: None,
                virtual_hosted_style_request: false,
            }
        );
        assert_eq!(
            key_from_url("s3a://bucket/foo").unwrap(),
            ObjectStoreKey::Aws {
                bucket: "bucket".to_owned(),
                region: None,
                virtual_hosted_style_request: false,
            }
        );
        assert_eq!(
            key_from_url("https://s3.bucket.amazonaws.com/foo").unwrap(),
            ObjectStoreKey::Aws {
                bucket: "bucket".to_owned(),
                region: None,
                virtual_hosted_style_request: false,
            }
        );
        assert_eq!(
            key_from_url("https://bucket.s3.region.amazonaws.com/foo").unwrap(),
            ObjectStoreKey::Aws {
                bucket: "bucket".to_owned(),
                region: Some("region".to_owned()),
                virtual_hosted_style_request: true
            }
        );
    }

    #[test]
    fn test_gcp_urls() {
        assert_eq!(
            key_from_url("gs://bucket/path").unwrap(),
            ObjectStoreKey::Gcs {
                bucket: "bucket".to_owned()
            }
        );

        assert_eq!(
            key_from_url("https://bucket.storage.googleapis.com/path").unwrap(),
            ObjectStoreKey::Gcs {
                bucket: "bucket".to_owned()
            }
        );

        assert_eq!(
            key_from_url("https://storage.cloud.google.com/bucket/path").unwrap(),
            ObjectStoreKey::Gcs {
                bucket: "bucket".to_owned()
            }
        );
    }
}
