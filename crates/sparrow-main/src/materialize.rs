use std::path::PathBuf;

use error_stack::{IntoReport, ResultExt};
use futures::{StreamExt, TryStreamExt};
use sparrow_api::kaskada::v1alpha::compile_request::ExpressionKind;
use sparrow_api::kaskada::v1alpha::execute_request::Limits;
use sparrow_api::kaskada::v1alpha::{
    self, destination, CompileRequest, ExecuteRequest, FenlDiagnostics,
};
use sparrow_api::kaskada::v1alpha::{Destination, ObjectStoreDestination};
use sparrow_compiler::CompilerOptions;
use sparrow_qfr::kaskada::sparrow::v1alpha::FlightRecordHeader;
use sparrow_runtime::s3::S3Helper;
use tracing::{info, info_span};

use crate::script::{Schema, Script, ScriptPath};

/// Options for the Materialize command.
#[derive(clap::Args, Debug)]
#[command(version, rename_all = "kebab-case")]
pub struct MaterializeCommand {
    #[command(flatten)]
    pub compiler_options: CompilerOptions,

    /// File containing the schema definitions for the script.
    #[arg(long)]
    pub schema: PathBuf,

    /// Input file containing the script to run.
    #[arg(long)]
    pub script: PathBuf,

    /// Path to store the Query Flight Record to.
    /// Defaults to not storing anything.
    #[arg(long)]
    pub flight_record_path: Option<PathBuf>,
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "invalid schema")]
    InvalidSchema,
    #[display(fmt = "invalid script")]
    InvalidScript,
    #[display(fmt = "script must be a file")]
    ScriptIsNotFile,
    #[display(fmt = "failed to canonicalize path")]
    Canonicalizing,
    #[display(fmt = "script not in directory")]
    ScriptNotInDirectory,
    #[display(fmt = "failed to compile query")]
    Compilation,
    #[display(fmt = "errors in query:\n{_0}")]
    InvalidQuery(FenlDiagnostics),
    #[display(fmt = "internal error")]
    Internal,
    #[display(fmt = "output is not a directory")]
    OutputIsNotDirectory,
    #[display(fmt = "failed to execute query")]
    Execution,
}

impl error_stack::Context for Error {}

impl MaterializeCommand {
    pub async fn execute(self) -> error_stack::Result<(), Error> {
        let span = info_span!("Sparrow in materialize-mode");

        let _enter = span.enter();
        info!("Options: {:?}", self);

        error_stack::ensure!(self.script.is_file(), Error::ScriptIsNotFile);

        let script = Script::try_from(&self.script)
            .change_context(Error::InvalidScript)
            .attach_printable_lazy(|| ScriptPath(self.script.clone()))?;
        let schema = Schema::try_from(&self.schema)
            .change_context(Error::InvalidSchema)
            .attach_printable_lazy(|| ScriptPath(self.schema.clone()))?;
        tracing::debug!(
            "Materializing with script: {:?}\nand schema: {:?}",
            script,
            schema
        );

        let tables = schema.tables.clone();
        let output_to = script.output_to.clone();
        let compile_result = sparrow_compiler::compile_proto(
            CompileRequest {
                tables: schema.tables,
                feature_set: Some(script.feature_set),
                slice_request: self.compiler_options.slice_request,
                expression_kind: ExpressionKind::Complete as i32,
                experimental: false,
                per_entity_behavior: self.compiler_options.per_entity_behavior as i32,
            },
            self.compiler_options.internal,
        )
        .await
        .change_context(Error::Compilation)?;

        let diagnostics = compile_result.fenl_diagnostics.unwrap_or_default();
        #[allow(clippy::print_stdout)]
        let plan = if let Some(plan) = compile_result.plan {
            println!("{diagnostics}");
            plan
        } else {
            error_stack::bail!(Error::InvalidQuery(diagnostics));
        };

        let s3_helper = S3Helper::new().await;

        // Note: it might be cleaner to create a separate entry point for materialize, but for now it's ok.
        let result_stream = sparrow_runtime::execute::execute(
            ExecuteRequest {
                plan: Some(plan),
                tables,
                destination: Some(output_to),
                limits: None,
                compute_snapshot_config: None,
                changed_since: None,
                final_result_time: None,
            },
            s3_helper,
            self.flight_record_path,
            FlightRecordHeader::default(),
        )
        .await
        .change_context(Error::Execution)?;

        // For the CLI, we use stdout to print information.
        #[allow(clippy::print_stdout)]
        {
            // Output file vec if the destination is an object store.
            let mut output_files: Vec<_> = Vec::new();

            let _: Vec<_> = result_stream
                .inspect_ok(|next| {
                    let rows_produced = next
                        .progress
                        .as_ref()
                        .expect("progress")
                        .produced_output_rows;
                    let destination = next.destination.as_ref().expect("destination");
                    match &destination.destination {
                        Some(destination::Destination::ObjectStore(o)) => {
                            match &o.output_paths {
                                Some(paths) => {
                                    paths
                                        .paths
                                        .clone()
                                        .into_iter()
                                        .for_each(|p| output_files.push(p));
                                }
                                _ => (),
                            };
                            // TODO: The output paths are empty because we currently don't
                            // report that a file is being written to until we've closed it.
                            // Since in a streaming world, we don't close it, we'll indefinitely write
                            // to a single file.
                            // We need to:
                            // 1. Write to multiple output files
                            // 2. Add to output_files, then log when a new file is produced
                            println!("{} rows produced so far", rows_produced);
                        }
                        Some(destination::Destination::Pulsar(p)) => {
                            let config = p.config.as_ref().expect("config");
                            // TODO: we can avoid creating this string each progress update by moving
                            // this above the stream
                            let topic_url =
                                sparrow_runtime::execute::output::pulsar::format_topic_url(config)
                                    .expect("already created topic url during compilation");

                            println!(
                                "{} rows produced so far to topic {}",
                                rows_produced, topic_url
                            );
                        }
                        _ => (),
                    };
                })
                .try_filter_map(|next| async move {
                    match next.output_paths() {
                        None => Ok(None),
                        Some(output_paths) => Ok(Some(output_paths)),
                    }
                })
                .try_collect()
                .await
                .change_context(Error::Execution)?;
        }
        Ok(())
    }
}
