#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

use clap::{command, Parser};
use error_stack::ResultExt;
use opentelemetry::global;
use sparrow_main::tracing_setup::{setup_tracing, TracingOptions};
use sparrow_main::{BatchCommand, PrepareCommand, ServeCommand};
use tracing::error;

#[cfg(not(target_os = "windows"))]
const NOTICE: &str = include_str!("../../../NOTICE");

#[cfg(target_os = "windows")]
const NOTICE: &str = include_str!("..\\..\\..\\NOTICE");

/// The top-level options for using Sparrow.
#[derive(clap::Parser, Debug)]
#[command(name = "sparrow", rename_all = "kebab-case", version)]
pub struct SparrowOptions {
    #[command(flatten)]
    tracing_options: TracingOptions,

    #[command(subcommand)]
    command: Command,
    /// Enables a custom panic handler which logs panics.
    ///
    /// This may be disabled to get the default backtrace and re-entrant
    /// panic handler.
    #[arg(long, env = "SPARROW_DISABLE_LOG_PANIC_HANDLER")]
    disable_log_panic_handler: bool,
}

#[derive(clap::Subcommand, Debug)]
#[allow(clippy::large_enum_variant)]
enum Command {
    /// Run the Sparrow gRPC service.
    Serve(ServeCommand),
    /// Run Sparrow in batch-mode on a specific script.
    Batch(BatchCommand),
    /// Prepare a file for use as part of a table.
    Prepare(PrepareCommand),
    /// License report and notice.
    License,
}

#[tokio::main]
async fn main() {
    let options = SparrowOptions::parse();
    setup_tracing(&options.tracing_options);

    if !options.disable_log_panic_handler {
        std::panic::set_hook(Box::new(logging_panic_hook));
    }

    let exit_code = if let Err(err) = main_body(options).await {
        error!("{:?}", err);
        1
    } else {
        0
    };

    // Exit with the given exit code.
    std::process::exit(exit_code);
}

#[derive(derive_more::Display, Debug)]
#[display(fmt = "error running command")]
pub struct Error;

impl error_stack::Context for Error {}

#[allow(clippy::print_stdout)]
async fn main_body(options: SparrowOptions) -> error_stack::Result<(), Error> {
    match options.command {
        Command::Serve(serve) => serve.execute().await.change_context(Error)?,
        Command::Batch(batch) => batch.execute().await.change_context(Error)?,
        Command::Prepare(prepare) => prepare.execute().change_context(Error)?,
        Command::License => {
            println!("{NOTICE}");
        }
    };

    Ok(())
}

fn logging_panic_hook(panic_info: &std::panic::PanicInfo<'_>) {
    let message = panic_info
        .payload()
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| {
            panic_info
                .payload()
                .downcast_ref::<String>()
                .map(|s| s.as_ref())
        });

    // TODO: Look at the implementation of the default panic hook and pull some of
    // that over if needed. Specifically, logging of backtrace (if enabled)
    // and handling of "double panic" (eg., if flushing the tracer provider
    // panics).
    match (message, panic_info.location()) {
        (Some(message), Some(location)) => error!("Panic: {} at {}", message, location),
        (Some(message), None) => error!("Panic: {} at unknown location", message),
        (None, Some(location)) => error!("Panic: unknown at {}", location),
        (None, None) => error!("Panic: unknown at unknown location"),
    }

    // Flush any tracing spans before returning.
    // This is still likely to produce an abrupt termination.
    global::shutdown_tracer_provider();
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_main_help_stdout() {
        let mut cmd = assert_cmd::Command::cargo_bin("sparrow-main").unwrap();
        let assert = cmd.arg("--help").assert().success();
        let output = std::str::from_utf8(&assert.get_output().stdout).unwrap();
        insta::assert_snapshot!(output);
    }

    #[test]
    fn test_batch_help_stdout() {
        let mut cmd = assert_cmd::Command::cargo_bin("sparrow-main").unwrap();
        let assert = cmd.arg("batch").arg("--help").assert().success();
        let output = std::str::from_utf8(&assert.get_output().stdout).unwrap();
        insta::assert_snapshot!(output);
    }

    #[test]
    fn test_prepare_help_stdout() {
        let mut cmd = assert_cmd::Command::cargo_bin("sparrow-main").unwrap();
        let assert = cmd.arg("prepare").arg("--help").assert().success();
        let output = std::str::from_utf8(&assert.get_output().stdout).unwrap();
        insta::assert_snapshot!(output);
    }

    #[test]
    fn test_serve_help_stdout() {
        let mut cmd = assert_cmd::Command::cargo_bin("sparrow-main").unwrap();
        let assert = cmd.arg("serve").arg("--help").assert().success();
        let output = std::str::from_utf8(&assert.get_output().stdout).unwrap();
        insta::assert_snapshot!(output);
    }
}
