use std::path::PathBuf;

use error_stack::{IntoReport, ResultExt};
use sparrow_api::kaskada::v1alpha::{file_path, FilePath, PrepareDataRequest, SlicePlan};

use sparrow_runtime::s3::S3Helper;

use crate::batch::{Script, ScriptPath};
use crate::serve;

/// Options for the Prepare command.
#[derive(clap::Args, Debug)]
#[command(version, rename_all = "kebab-case")]
pub struct PrepareCommand {
    /// Input file containing the Parquet file to prepare.
    pub input: PathBuf,

    /// Output path to write the files to.
    #[arg(long, default_value = ".")]
    pub output_path: PathBuf,

    /// Prefix for the output file names.
    #[arg(long)]
    pub file_prefix: Option<String>,

    /// Path to the query that will be executed.
    ///
    /// This path is used to read the actual query and extract the table config.
    #[arg(long)]
    pub query: PathBuf,

    /// The name of the table being prepared.
    ///
    /// This must exist within the tables in the query.
    #[arg(long)]
    pub table: String,
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "invalid script")]
    InvalidScript,
    #[display(fmt = "missing table")]
    MissingTable,
    #[display(fmt = "missing table config")]
    MissingTableConfig,
    #[display(fmt = "preparing")]
    Preparing,
    #[display(fmt = "canonicalize paths")]
    Canonicalize,
}

impl error_stack::Context for Error {}

#[derive(derive_more::Display, Debug)]
#[display(fmt = "table name: '{_0:?}'")]
struct TableName(String);

#[derive(Debug)]
struct LabeledPath {
    label: &'static str,
    path: PathBuf,
}

impl LabeledPath {
    fn new(label: &'static str, path: PathBuf) -> Self {
        Self { label, path }
    }
}

impl std::fmt::Display for LabeledPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: '{}'", self.label, self.path.display())
    }
}

impl PrepareCommand {
    #[allow(clippy::print_stdout)]
    pub async fn execute(self) -> error_stack::Result<(), Error> {
        println!("Preparing files: {self:?}");

        let script = Script::try_from(&self.query)
            .attach_printable_lazy(|| ScriptPath(self.query.clone()))
            .change_context(Error::InvalidScript)?;
        let table = script
            .tables
            .into_iter()
            .find(|table| self.table == table.name())
            .ok_or(error_stack::report!(Error::MissingTable))
            .attach_printable_lazy(|| ScriptPath(self.query.clone()))
            .attach_printable_lazy(|| TableName(self.table.clone()))?;
        let config = table
            .config
            .ok_or(error_stack::report!(Error::MissingTableConfig))
            .attach_printable_lazy(|| ScriptPath(self.query.clone()))
            .attach_printable_lazy(|| TableName(self.table.clone()))?;
        let file_prefix = if let Some(prefix) = &self.file_prefix {
            prefix.to_owned()
        } else {
            let input_stem = self
                .input
                .file_stem()
                .expect("File name")
                .to_str()
                .expect("ascii");
            format!("prepared-{}-{}", self.table, input_stem)
        };

        let input = self
            .input
            .canonicalize()
            .into_report()
            .change_context(Error::Canonicalize)
            .attach_printable_lazy(|| LabeledPath::new("input path", self.input.clone()))?;

        // given the input path, we need to create a file path corresponding to its extension.
        // non-CSV files are assumed to be Parquet.
        let input_string = input.as_os_str().to_string_lossy().to_string();
        let file_path = match std::path::Path::new(&input_string).extension() {
            Some(ext) if ext == "csv" => file_path::Path::CsvPath(input_string),
            _ => file_path::Path::ParquetPath(input_string),
        };

        let sp = SlicePlan {
            table_name: config.name.clone(),
            slice: table.file_sets[0]
                .slice_plan
                .clone()
                .ok_or(Error::MissingTableConfig)?
                .slice,
        };

        let pdr = PrepareDataRequest {
            file_path: Some(FilePath {
                path: Some(file_path),
            }),
            config: Some(config),
            output_path_prefix: self.output_path.to_string_lossy().to_string(),
            file_prefix: file_prefix.to_string(),
            slice_plan: Some(sp),
        };
        serve::preparation_service::prepare_data(S3Helper::new().await, tonic::Request::new(pdr))
            .await
            .change_context(Error::Preparing)
            .attach_printable_lazy(|| ScriptPath(self.query.clone()))
            .attach_printable_lazy(|| TableName(self.table.clone()))?;

        Ok(())
    }
}
