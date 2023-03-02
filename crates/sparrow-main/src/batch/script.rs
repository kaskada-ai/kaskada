use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};

use error_stack::{IntoReport, ResultExt};
use serde::{Deserialize, Serialize};
use sparrow_api::kaskada::v1alpha::OutputTo;
use sparrow_api::kaskada::v1alpha::{ComputeTable, FeatureSet};

/// A serializable description of the details needed to execute a query.
///
/// This allows creating "scripts" that compute specific features.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Script {
    // / The destination to output results to.
    pub(crate) output_to: OutputTo,
    /// Tables that are available inside the script.
    pub(crate) tables: Vec<ComputeTable>,
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

impl Script {
    pub fn try_from(path: &Path) -> error_stack::Result<Self, Error> {
        let script_file = File::open(path)
            .into_report()
            .change_context(Error::OpenScript)?;
        let script_reader = BufReader::new(script_file);

        match &path.extension().and_then(|ext| ext.to_str()) {
            None => Err(error_stack::report!(Error::MissingScriptExtension)),
            Some("json") => serde_json::from_reader(script_reader)
                .into_report()
                .change_context(Error::DeserializeScript),
            Some("yaml") => serde_yaml::from_reader(script_reader)
                .into_report()
                .change_context(Error::DeserializeScript),
            Some(extension) => Err(error_stack::report!(Error::InvalidScriptExtension(
                (*extension).to_owned(),
            ))),
        }
    }
}

pub trait MakeAbsolute {
    /// Update all paths to be absolute.
    fn make_absolute(&mut self, working_dir: &Path);
}

impl MakeAbsolute for Script {
    fn make_absolute(&mut self, working_dir: &Path) {
        for table in self.tables.iter_mut() {
            for file_set in table.file_sets.iter_mut() {
                for prepared_file in file_set.prepared_files.iter_mut() {
                    prepared_file.path.make_absolute(working_dir);
                }
            }
        }
    }
}

impl MakeAbsolute for PathBuf {
    fn make_absolute(&mut self, working_dir: &Path) {
        if self.is_relative() {
            let path = std::mem::replace(self, working_dir.to_path_buf());
            self.push(path)
        }
    }
}

impl MakeAbsolute for String {
    fn make_absolute(&mut self, working_dir: &Path) {
        let path = Path::new(self);
        if path.is_relative() {
            let path = working_dir.join(path);
            *self = path.to_str().expect("ascii paths").to_owned();
        }
    }
}
