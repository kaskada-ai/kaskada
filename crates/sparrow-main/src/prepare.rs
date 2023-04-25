use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;

use error_stack::{IntoReport, IntoReportCompat, ResultExt};
use sparrow_api::kaskada::v1alpha::{source_data, PrepareDataRequest, PulsarConfig, SlicePlan};
use sparrow_api::kaskada::v1alpha::{PulsarSubscription, SourceData};

use sparrow_runtime::s3::S3Helper;

use crate::batch::{Schema, ScriptPath};
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

    /// Path to the serialized schema.
    #[arg(long)]
    pub schema: PathBuf,

    /// The name of the table being prepared.
    ///
    /// This must be defined in the schema.
    #[arg(long)]
    pub table: String,
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "invalid schema")]
    InvalidSchema,
    #[display(fmt = "missing table")]
    MissingTable,
    #[display(fmt = "missing table config")]
    MissingTableConfig,
    #[display(fmt = "preparing")]
    Preparing,
    #[display(fmt = "canonicalize paths")]
    Canonicalize,
    #[display(fmt = "unrecognized input format")]
    UnrecognizedInputFormat,
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

        let schema = Schema::try_from(&self.schema)
            .attach_printable_lazy(|| ScriptPath(self.schema.clone()))
            .change_context(Error::InvalidSchema)?;
        let table = schema
            .tables
            .into_iter()
            .find(|table| self.table == table.name())
            .ok_or(error_stack::report!(Error::MissingTable))
            .attach_printable_lazy(|| ScriptPath(self.schema.clone()))
            .attach_printable_lazy(|| TableName(self.table.clone()))?;
        let config = table
            .config
            .ok_or(error_stack::report!(Error::MissingTableConfig))
            .attach_printable_lazy(|| ScriptPath(self.schema.clone()))
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

        // if input is "pulsar" then turn it into a PulsarSource
        let source_data = if self
            .input
            .to_str()
            .expect("unable to convert input to str (not utf8)")
            == "pulsar"
        {
            // read webServiceUrl, brokerServiceUrl, authPlugin, authParams from config file
            // given by env var PULSAR_CLIENT_CONF
            let fname = std::env::var("PULSAR_CLIENT_CONF")
                .into_report()
                .change_context(Error::MissingTableConfig)
                .attach_printable("missing env var PULSAR_CLIENT_CONF")?;
            let f = File::open(&fname)
                .into_report()
                .change_context(Error::MissingTableConfig)
                .attach_printable_lazy(|| format!("unable to read file at {:?}", fname))?;
            let mut config = HashMap::new();
            for line in BufReader::new(f).lines() {
                let line = line
                    .into_report()
                    .change_context(Error::MissingTableConfig)
                    .attach_printable_lazy(|| format!("error reading line from {}", fname))?;
                let mut parts = line.splitn(2, '=');
                if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                    config.insert(key.to_string(), value.to_string());
                } else if !line.chars().all(|c| c.is_whitespace()) {
                    tracing::trace!("ignoring config line {:?}", line);
                }
            }
            for key in [
                "webServiceUrl",
                "brokerServiceUrl",
                "authPlugin",
                "authParams",
            ] {
                if !config.contains_key(key) {
                    return Err(error_stack::report!(Error::MissingTableConfig)
                        .attach_printable(format!("missing config key {:?}", key)));
                }
            }

            // fully-qualified topic name
            let pulsar_tenant = std::env::var("PULSAR_TENANT").unwrap_or("public".to_owned());
            let pulsar_namespace =
                std::env::var("PULSAR_NAMESPACE").unwrap_or("default".to_owned());
            let pulsar_subscription =
                std::env::var("PULSAR_SUBSCRIPTION").unwrap_or("subscription-default".to_owned());
            let pulsar_topic = std::env::var("PULSAR_TOPIC")
                .into_report()
                .change_context(Error::MissingTableConfig)
                .attach_printable("missing env var PULSAR_TOPIC")?;

            let pulsar_config = PulsarConfig {
                admin_service_url: config["webServiceUrl"].clone(),
                broker_service_url: config["brokerServiceUrl"].clone(),
                auth_plugin: config["authPlugin"].clone(),
                auth_params: config["authParams"].clone(),
                tenant: pulsar_tenant,
                namespace: pulsar_namespace,
                topic_name: pulsar_topic,
            };
            tracing::debug!("Pulsar config is {:?}", redact_auth_field(&pulsar_config));

            SourceData {
                source: Some(source_data::Source::PulsarSubscription(
                    PulsarSubscription {
                        config: Some(pulsar_config),
                        subscription_id: pulsar_subscription,
                        last_publish_time: 0,
                    },
                )),
            }
        } else {
            let input = self
                .input
                .canonicalize()
                .into_report()
                .change_context(Error::Canonicalize)
                .attach_printable_lazy(|| LabeledPath::new("input path", self.input.clone()))?;

            let file_path = SourceData::try_from_local(input.as_path())
                .into_report()
                .change_context(Error::UnrecognizedInputFormat)?;
            SourceData {
                source: Some(file_path),
            }
        };

        if table.file_sets.is_empty() {
            return Err(error_stack::report!(Error::MissingTableConfig)
                .attach_printable("At least one file_sets is required"));
        }
        let sp = SlicePlan {
            table_name: config.name.clone(),
            slice: table.file_sets[0]
                .slice_plan
                .clone()
                .ok_or(Error::MissingTableConfig)?
                .slice,
        };

        let pdr = PrepareDataRequest {
            source_data: Some(source_data),
            config: Some(config),
            output_path_prefix: self.output_path.to_string_lossy().to_string(),
            file_prefix: file_prefix.to_string(),
            slice_plan: Some(sp),
        };
        serve::preparation_service::prepare_data(S3Helper::new().await, tonic::Request::new(pdr))
            .await
            .change_context(Error::Preparing)
            .attach_printable_lazy(|| ScriptPath(self.schema.clone()))
            .attach_printable_lazy(|| TableName(self.table.clone()))?;

        Ok(())
    }
}

// Hack to remove auth fields from log output.
//
// Ideally, we use tonic or prost reflection to redact all
// fields marked as sensitive.
fn redact_auth_field(pulsar: &PulsarConfig) -> PulsarConfig {
    let mut clone = pulsar.clone();
    clone.auth_params = "...redacted...".to_owned();
    clone
}
