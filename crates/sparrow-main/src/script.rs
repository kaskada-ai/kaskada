use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use error_stack::{IntoReport, ResultExt};
use serde::{Deserialize, Serialize};
use sparrow_api::kaskada::v1alpha::{ComputeTable, Destination, FeatureSet};

/// A serializable description of schema for a set of tables.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Schema {
    /// Tables that are available inside the script.
    pub(crate) tables: Vec<ComputeTable>,
}

/// A serializable description of the details needed to execute a query.
///
/// This allows creating "scripts" that compute specific features.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Script {
    // / The destination to output results to.
    pub(crate) output_to: Destination,
    /// The features to be computed for the query.
    pub(crate) feature_set: FeatureSet,
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to open script file")]
    OpenScript,
    #[display(fmt = "missing script extension")]
    MissingScriptExtension,
    #[display(fmt = "invalid script extension '{_0:?}'")]
    InvalidScriptExtension(String),
    #[display(fmt = "failed to deserialize script file")]
    DeserializeScript,
}

impl error_stack::Context for Error {}

#[derive(derive_more::Display, Debug)]
#[display(fmt = "script path: '{_0:?}'")]
pub(crate) struct ScriptPath(pub PathBuf);

fn deserialize_from_path<T>(path: &Path) -> error_stack::Result<T, Error>
where
    for<'de> T: Deserialize<'de>,
{
    let script_file = File::open(path)
        .into_report()
        .change_context(Error::OpenScript)?;
    let reader = BufReader::new(script_file);

    match &path.extension().and_then(|ext| ext.to_str()) {
        Some("json") => serde_json::from_reader(reader)
            .into_report()
            .change_context(Error::DeserializeScript),
        Some("yaml") => serde_yaml::from_reader(reader)
            .into_report()
            .change_context(Error::DeserializeScript),
        Some(extension) => Err(error_stack::report!(Error::InvalidScriptExtension(
            (*extension).to_owned()
        ))),
        None => Err(error_stack::report!(Error::MissingScriptExtension)),
    }
}

impl Script {
    pub fn try_from(path: &Path) -> error_stack::Result<Self, Error> {
        deserialize_from_path(path)
    }
}

impl Schema {
    pub fn try_from(path: &Path) -> error_stack::Result<Self, Error> {
        deserialize_from_path(path)
    }
}
