#![warn(
    rust_2018_idioms,
    nonstandard_style,
    future_incompatible,
    clippy::mod_module_files,
    clippy::print_stdout,
    clippy::print_stderr,
    clippy::undocumented_unsafe_blocks
)]

use clap::Parser;
use error_stack::{IntoReport, ResultExt};
use std::path::PathBuf;
use std::process::ExitCode;

use futures::{Stream, TryStreamExt};
use itertools::Itertools;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

mod generate;
mod structs;
mod update;

use crate::update::{update_doc_structs, UpdateCommand};

/// Utility for maintaining and generating Fenl documentation.
#[derive(clap::Parser, Debug)]
#[command(name = "sparrow-catalog", rename_all = "kebab-case")]
struct CatalogOptions {
    /// Input directory containing the documentation files.
    #[arg(long)]
    input_dir: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(clap::Subcommand, Debug)]
enum Command {
    /// Checks the input files to ensure they are up-to-date.
    Check(Box<update::UpdateOptions>),
    /// Updates the input files.
    ///
    /// This updates the signature based on the declared function, and
    /// updates the examples with the output of executing each expression.
    Update(Box<update::UpdateOptions>),
    /// Generates a markdown file containing the catalog.
    ///
    /// This does not execute or update any of the signatures / examples.
    Generate(Box<generate::GenerateOptions>),
}

/// List the `*.toml` files in `doc_root`.
async fn list_doc_files(
    doc_root: PathBuf,
) -> Result<impl Stream<Item = Result<PathBuf, std::io::Error>>, std::io::Error> {
    Ok(
        tokio_stream::wrappers::ReadDirStream::new(tokio::fs::read_dir(doc_root).await?)
            .try_filter_map(|file| async move {
                let path = file.path();
                if path.is_file()
                    && matches!(path.extension(), Some(extension) if extension == "toml")
                {
                    Ok(Some(path))
                } else {
                    Ok(None)
                }
            }),
    )
}

#[derive(derive_more::Display, Debug)]
#[display(fmt = "failed")]
pub struct Error;

impl error_stack::Context for Error {}

#[tokio::main]
#[allow(clippy::print_stdout)]
async fn main() -> error_stack::Result<ExitCode, Error> {
    let options = CatalogOptions::parse();

    // Setup tracnig for the catalog generation.
    tracing_subscriber::registry()
        .with(EnvFilter::try_new("egg::=warn,sparrow_=trace,info").unwrap())
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .try_init()
        .into_report()
        .change_context(Error)?;

    match options.command {
        Command::Check(update_options) => {
            let changed = update_doc_structs(
                UpdateCommand::Check,
                update_options.as_ref(),
                options.input_dir.clone(),
            )
            .await
            .change_context(Error)?;

            if !changed.is_empty() {
                println!(
                    "{} files did not match. Run `sparrow-catalog --input_path={:?} update` to \
                     update:\n{}",
                    changed.len(),
                    options.input_dir.to_string_lossy(),
                    changed
                        .into_iter()
                        .format_with("\n", |elt, f| f(&format_args!(
                            "  {}",
                            elt.to_string_lossy()
                        )))
                );
                return Ok(ExitCode::from(1));
            } else {
                info!("Everything up-to-date")
            }
        }
        Command::Update(update_options) => {
            let changed = update_doc_structs(
                UpdateCommand::Update,
                update_options.as_ref(),
                options.input_dir,
            )
            .await
            .change_context(Error)?;

            if !changed.is_empty() {
                info!(
                    "{} files changed:\n{}",
                    changed.len(),
                    changed
                        .into_iter()
                        .format_with("\n", |elt, f| f(&format_args!(
                            "  {}",
                            elt.to_string_lossy()
                        )))
                )
            } else {
                info!("Everything up-to-date")
            }
        }
        Command::Generate(generate_options) => {
            generate::generate(options.input_dir, *generate_options)
                .await
                .change_context(Error)?;
        }
    }

    Ok(ExitCode::from(0))
}
