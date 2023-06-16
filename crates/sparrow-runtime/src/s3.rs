use std::path::Path;

use anyhow::{anyhow, Context};
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint};
use futures::stream::StreamExt;
use itertools::Itertools;
use tempfile::TempPath;
use tokio_util::io::StreamReader;
use tonic::transport::Uri;
use tracing::info;

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct S3Object {
    pub bucket: String,
    pub key: String,
}

impl S3Object {
    pub fn try_from_uri(path: &str) -> anyhow::Result<Self> {
        if let Some(s3_path) = path.strip_prefix("s3://") {
            if let Some((bucket, key)) = s3_path.split_once('/') {
                Ok(S3Object {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                })
            } else {
                Err(anyhow!(
                    "invalid path provided: '{}' missing bucket/key format",
                    path
                ))
            }
        } else {
            Err(anyhow!(
                "invalid path provided: '{}' missing s3:// prefix",
                path
            ))
        }
    }

    pub fn get_formatted_key(&self) -> String {
        format!("s3://{}/{}", self.bucket, self.key)
    }

    /// Returns the relative path of the key.
    ///
    /// example.
    ///   prefix:   "compute/snapshot/"
    ///   self.key: "/compute/snapshot/file/data"
    ///   returns:  "file/data"
    pub fn get_relative_key_path(&self, prefix: &str) -> anyhow::Result<&str> {
        anyhow::ensure!(!prefix.is_empty(), "Expected prefix to be non-empty");
        let prefix = if prefix.ends_with('/') {
            &prefix[0..prefix.len() - 1]
        } else {
            prefix
        };
        let res = self.key.strip_prefix(prefix).with_context(|| {
            format!(
                "Expected key '{}' to start with prefix '{}', but did not",
                self.key, prefix
            )
        })?;
        // Strip the preceding '/'
        Ok(&res[1..res.len()])
    }

    /// Joins the suffix onto the key separated by a slash.
    ///
    /// If the key has a trailing slash, or the suffix has a preceding
    /// slash, this method will ensure the result is separated only by a
    /// single slash.
    ///
    /// ex. self.key   = "prefix"
    ///     suffix     = "suffix"
    ///     result.key = "prefix/suffix"
    #[must_use]
    pub fn join_delimited(&self, suffix: &str) -> Self {
        let mut result = self.clone();
        result.push_delimited(suffix);
        result
    }

    /// Pushes the suffix onto the key separated by a slash.
    ///
    /// If the key has a trailing slash, or the suffix has a preceding
    /// slash, this method will ensure the result is separated only by a
    /// single slash.
    ///
    /// ex. self.key   = "prefix"
    ///     suffix     = "suffix"
    ///     result.key = "prefix/suffix"
    pub fn push_delimited(&mut self, suffix: &str) {
        let existing_delimiter = self.key.ends_with('/');
        let new_delimiter = suffix.starts_with('/');

        if existing_delimiter && new_delimiter {
            // Double delimited. Drop one from the suffix.
            self.key.push_str(&suffix[1..]);
        } else if existing_delimiter || new_delimiter {
            // Single delimited. Push the entire suffix.
            self.key.push_str(suffix);
        } else {
            // No delimiter. Add one.
            self.key.push('/');
            self.key.push_str(suffix);
        }
    }
}

pub fn is_s3_path(path: &str) -> bool {
    path.to_lowercase().starts_with("s3://")
}

/// S3Helper is a wrapper around the S3 client providing useful methods.
///
/// The underlying client contains only an `Arc`, so it should be cheap
/// to clone.
#[derive(Clone, Debug)]
pub struct S3Helper {
    client: Client,
}

impl S3Helper {
    pub async fn new() -> Self {
        let shared_config = aws_config::from_env().load().await;

        // Create the client.
        let endpoint = std::env::var("AWS_ENDPOINT");
        let s3_config: aws_sdk_s3::config::Config = if let Ok(endpoint) = endpoint {
            // Oeverride the endpoint if needed
            let uri: Uri = endpoint.parse().unwrap();
            let endpoint = Endpoint::immutable(uri);

            aws_sdk_s3::config::Builder::from(&shared_config)
                .endpoint_resolver(endpoint)
                .build()
        } else {
            (&shared_config).into()
        };
        info!("S3 Config: {:?}", s3_config);
        let client = Client::from_conf(s3_config);

        Self { client }
    }

    pub async fn upload_tempfile_to_s3(
        &self,
        s3_key: S3Object,
        local_path: TempPath,
    ) -> anyhow::Result<()> {
        self.upload_s3(s3_key, &local_path).await?;

        // Close and remove the temporary file. This causes errors.
        local_path.close()?;
        Ok(())
    }

    pub async fn download_s3(
        &self,
        s3_object: S3Object,
        target_local_path: impl AsRef<Path>,
    ) -> anyhow::Result<()> {
        let key = Some(s3_object.key);
        let bucket = Some(s3_object.bucket);
        let download = self
            .client
            .get_object()
            .set_key(key)
            .set_bucket(bucket)
            .send()
            .await
            .context("Unable to download object from s3")?;

        let mut file = tokio::fs::File::create(target_local_path).await?;
        let mut body = StreamReader::new(
            download
                .body
                // Convert S3 download errors into `io:Error` so we can use `tokio::io::copy`
                .map(|item| item.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))),
        );
        // https://docs.rs/tokio/1.14.0/tokio/io/fn.copy.html
        tokio::io::copy(&mut body, &mut file).await?;

        Ok(())
    }

    pub async fn upload_s3(
        &self,
        s3_key: S3Object,
        target_local_path: &Path,
    ) -> anyhow::Result<()> {
        let body = ByteStream::from_path(target_local_path)
            .await
            .with_context(|| {
                format!("unable to convert file at {target_local_path:?} to ByteStream")
            })?;

        let s3_key_clone = s3_key.clone();
        self.client
            .put_object()
            .set_key(Some(s3_key.key))
            .set_bucket(Some(s3_key.bucket))
            .set_body(Some(body))
            .send()
            .await
            .with_context(|| {
                format!("unable to upload file at {target_local_path:?} to S3 at {s3_key_clone:?}")
            })?;

        Ok(())
    }

    /// Lists s3 objects with the given prefix in s3.
    ///
    /// Appends a trailing '/' to the `prefix` if it does not exist.
    pub async fn list_prefix_delimited(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> anyhow::Result<Vec<S3Object>> {
        let prefix = if !prefix.is_empty() {
            let c = &prefix[prefix.len() - 1..prefix.len()];
            if c != "/" {
                let mut res = prefix.to_owned();
                res.push('/');
                res
            } else {
                prefix.to_owned()
            }
        } else {
            String::new()
        };

        let list_obj_res = self
            .client
            .list_objects_v2()
            .set_bucket(Some(bucket.to_owned()))
            .set_prefix(Some(prefix))
            .send()
            .await
            .context("Unable to list objects")?;

        if list_obj_res.is_truncated() {
            anyhow::bail!("Unsupported: did not expect result to be truncated!")
        }

        let res = list_obj_res
            .contents()
            .unwrap_or_default()
            .iter()
            .map(|i| -> anyhow::Result<_> {
                Ok(S3Object {
                    bucket: bucket.to_owned(),
                    key: i.key().context("Could not parse s3 key")?.to_owned(),
                })
            })
            .try_collect()?;

        Ok(res)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_from_uri_valid() {
        let valid_uri = "s3://bucket/some/key/path";
        let result = S3Object::try_from_uri(valid_uri).unwrap();
        assert_eq!(result.key, "some/key/path");
        assert_eq!(result.bucket, "bucket");
    }

    #[test]
    fn test_try_from_uri_invalid() {
        let invalid_uri = "s3:/bucket/some/key/path";
        assert_eq!(
            S3Object::try_from_uri(invalid_uri).unwrap_err().to_string(),
            "invalid path provided: 's3:/bucket/some/key/path' missing s3:// prefix",
        );
    }

    #[test]
    fn test_get_formatted_key() {
        let expected = "s3://bucket/some/key/path";
        let obj = S3Object {
            key: "some/key/path".to_owned(),
            bucket: "bucket".to_owned(),
        };
        assert_eq!(obj.get_formatted_key(), expected);
    }

    #[test]
    fn test_get_relative_key_path() {
        let obj = S3Object {
            key: "some/key/path".to_owned(),
            bucket: "bucket".to_owned(),
        };
        let relative_path = obj.get_relative_key_path("some/key").unwrap();
        assert_eq!(relative_path, "path");

        // With trailing slash
        // Regression test against splitting the key incorrectly.
        let relative_path = obj.get_relative_key_path("some/key/").unwrap();
        assert_eq!(relative_path, "path");
    }

    #[test]
    fn test_get_relative_key_path_fails_invalid_prefix() {
        let obj = S3Object {
            key: "some/key/path".to_owned(),
            bucket: "bucket".to_owned(),
        };

        assert_eq!(
            obj.get_relative_key_path("bogus").unwrap_err().to_string(),
            "Expected key 'some/key/path' to start with prefix 'bogus', but did not"
        );
    }

    #[test]
    fn test_join_delimited_adds_delimiter() {
        let obj = S3Object {
            key: "some/key/path".to_owned(),
            bucket: "bucket".to_owned(),
        };
        let res = obj.join_delimited("suffix");
        assert_eq!(res.key, "some/key/path/suffix");
    }

    #[test]
    fn test_join_delimited_with_existing_delimiters() {
        let obj_key_trailing_slash = S3Object {
            key: "some/key/path/".to_owned(),
            bucket: "bucket".to_owned(),
        };
        let res = obj_key_trailing_slash.join_delimited("suffix");
        assert_eq!(res.key, "some/key/path/suffix");

        let res = obj_key_trailing_slash.join_delimited("/suffix");
        assert_eq!(res.key, "some/key/path/suffix");
    }

    #[test]
    fn test_is_s3_path_valid() {
        assert!(is_s3_path("s3://bucket/key"));
        assert!(!is_s3_path("s3//not/valid"));
        assert!(!is_s3_path("s3:/not/valid"));
        assert!(!is_s3_path("bucket/not/valid"));
    }
}
