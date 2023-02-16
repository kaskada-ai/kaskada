use std::path::PathBuf;

use error_stack::{IntoReport, ResultExt};
use futures::TryStreamExt;
use sparrow_api::kaskada::v1alpha::compile_request::ExpressionKind;
use sparrow_api::kaskada::v1alpha::execute_request::Limits;
use sparrow_api::kaskada::v1alpha::{CompileRequest, ExecuteRequest, FenlDiagnostics};
use sparrow_compiler::CompilerOptions;
use sparrow_qfr::kaskada::sparrow::v1alpha::FlightRecordHeader;
use sparrow_runtime::s3::S3Helper;
use tracing::{info, info_span};

mod script;

pub(crate) use script::{Script, ScriptPath};

use crate::batch::script::MakeAbsolute;

/// Options for the Batch command.
#[derive(clap::Args, Debug)]
#[command(version, rename_all = "kebab-case")]
pub struct BatchCommand {
    #[command(flatten)]
    pub compiler_options: CompilerOptions,

    #[command(flatten)]
    pub limits: Limits,

    /// Path to store the Query Flight Record to.
    /// Defaults to not storing anything.
    #[arg(long)]
    pub flight_record_path: Option<PathBuf>,

    /// Only compile (and output the plan/etc. if requested).
    #[arg(long, action)]
    pub compile_only: bool,

    /// Input file containing the script to run.
    pub script: PathBuf,

    /// Output directory to write the output to.
    pub output_dir: PathBuf,
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
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

impl BatchCommand {
    pub async fn execute(self) -> error_stack::Result<(), Error> {
        let span = info_span!("Sparrow in batch-mode");

        let _enter = span.enter();
        info!("Options: {:?}", self);

        error_stack::ensure!(self.script.is_file(), Error::ScriptIsNotFile);

        let script_path = self
            .script
            .canonicalize()
            .into_report()
            .change_context(Error::Canonicalizing)?;
        let mut script = Script::try_from(&self.script)
            .change_context(Error::InvalidScript)
            .attach_printable_lazy(|| ScriptPath(self.script.clone()))?;

        script.make_absolute(script_path.parent().ok_or(Error::ScriptNotInDirectory)?);

        let tables = script.tables.clone();
        let output_to = script.output_to.clone();
        let compile_result = sparrow_compiler::compile_proto(
            CompileRequest {
                tables: script.tables,
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

        if !self.compile_only {
            let s3_helper = S3Helper::new().await;

            if !self.output_dir.exists() {
                tokio::fs::create_dir_all(&self.output_dir)
                    .await
                    .into_report()
                    .change_context(Error::Internal)?;
            }

            error_stack::ensure!(self.output_dir.is_dir(), Error::OutputIsNotDirectory);

            let result_stream = sparrow_runtime::execute::execute(
                ExecuteRequest {
                    plan: Some(plan),
                    tables,
                    output_to: Some(output_to),
                    limits: Some(self.limits),
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
                let output_files: Vec<_> = result_stream
                    .inspect_ok(|next| {
                        println!(
                            "{} rows produced so far",
                            next.progress
                                .as_ref()
                                .expect("progress")
                                .produced_output_rows
                        )
                    })
                    .try_filter_map(|next| async move {
                        match next.output_paths {
                            None => Ok(None),
                            Some(paths) if paths.paths.is_empty() => Ok(None),
                            Some(paths) => Ok(Some(paths.paths)),
                        }
                    })
                    .try_collect()
                    .await
                    .change_context(Error::Execution)?;

                println!("Output files: {output_files:?}");
            }
        }

        Ok(())
    }
}
