use std::{path::Path, str::FromStr};

use error_stack::{IntoReport, ResultExt};
use serde::{Deserialize, Serialize};

use url::Url;

use crate::stores::registry::Error;

/// A string referring to a file in an object store.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ObjectStoreUrl {
    url: Url,
}

impl ObjectStoreUrl {
    /// Return the [object_store::path::Path] corresponding to this URL.
    pub fn path(&self) -> error_stack::Result<object_store::path::Path, Error> {
        object_store::path::Path::parse(self.url.path())
            .into_report()
            .change_context_lazy(|| Error::InvalidUrl(self.url.to_string()))
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
            .change_context_lazy(|| Error::InvalidUrl(input.to_owned()))?;
        Ok(Self { url })
    }

    /// Creates a relative URL if possible, with this URL as the base URL.
    ///
    /// This is the inverse of [`join`].
    pub fn relative_path(&self, url: &Self) -> Option<String> {
        self.url.make_relative(&url.url)
    }

    /// Return the local path, if this is a local file.
    pub fn local_path(&self) -> Option<&Path> {
        if self.url.scheme() == "file" {
            let path = self.url.path();
            Some(Path::new(path))
        } else {
            None
        }
    }

    /// Return true if the URL ends with a delimiter.
    pub fn is_delimited(&self) -> bool {
        self.url.path().ends_with('/')
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join() {
        let directory = ObjectStoreUrl::from_str("s3://bucket/directory/").unwrap();
        assert_eq!(
            ObjectStoreUrl::from_str("s3://bucket/directory/path.foo").unwrap(),
            directory.join("path.foo").unwrap()
        );

        let file = ObjectStoreUrl::from_str("s3://bucket/directory/file").unwrap();
        assert_eq!(
            ObjectStoreUrl::from_str("s3://bucket/directory/path.foo").unwrap(),
            file.join("path.foo").unwrap()
        );
    }

    #[test]
    fn test_local_path() {
        assert_eq!(
            ObjectStoreUrl::from_str("file:///absolute/path")
                .unwrap()
                .local_path(),
            Some(Path::new("/absolute/path"))
        );
        assert_eq!(
            ObjectStoreUrl::from_str("s3://bucket/directory/path.foo")
                .unwrap()
                .local_path(),
            None
        );
    }

    #[test]
    fn test_make_relative() {
        let local_prefix = ObjectStoreUrl::from_str("file:///absolute/path/").unwrap();
        let local_file = ObjectStoreUrl::from_str("file:///absolute/path/some/file").unwrap();
        let s3_prefix = ObjectStoreUrl::from_str("s3://bucket/prefix/").unwrap();
        let s3_file = ObjectStoreUrl::from_str("s3://bucket/prefix/some/object").unwrap();

        assert_eq!(
            local_prefix.relative_path(&local_file),
            Some("some/file".to_owned())
        );
        assert_eq!(local_prefix.relative_path(&s3_file), None);

        assert_eq!(
            s3_prefix.relative_path(&s3_file),
            Some("some/object".to_owned())
        );
        assert_eq!(s3_prefix.relative_path(&local_file), None);
    }
}
