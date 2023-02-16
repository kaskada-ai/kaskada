use std::path::PathBuf;

use error_stack::{IntoReport, ResultExt};
use futures::TryStreamExt;
use itertools::Itertools;
use sparrow_runtime::s3::S3Helper;
use tracing::{error, info, info_span};

use crate::list_doc_files;
use crate::structs::CatalogEntry;

mod execute_example;

#[derive(clap::Args, Default, Debug)]
pub(super) struct UpdateOptions {
    /// Only execute the example for the named function.
    example: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub(super) enum UpdateCommand {
    Check,
    Update,
}

/// Update (or check) all of the doc `.toml` files in `doc_root`.
pub(super) async fn update_doc_structs(
    command: UpdateCommand,
    options: &UpdateOptions,
    doc_root: PathBuf,
) -> error_stack::Result<Vec<PathBuf>, Error> {
    let s3_helper = S3Helper::new().await;

    if let Some(example) = &options.example {
        let doc_path = doc_root.join(format!("{example}.toml"));
        error_stack::ensure!(doc_path.is_file(), Error::NonFile(doc_path));
        let changed = update_doc_struct(command, doc_path.clone(), s3_helper)
            .await
            .attach_printable_lazy(|| DocFile(doc_path.clone()))?;
        if changed {
            Ok(vec![doc_path])
        } else {
            Ok(vec![])
        }
    } else {
        let file_stream = list_doc_files(doc_root)
            .await
            .into_report()
            .change_context(Error::ListingFiles)?
            .map_err(|e| error_stack::report!(e).change_context(Error::ListingFiles));

        let changed = file_stream
            .map_ok(move |doc_path| {
                let s3_helper = s3_helper.clone();
                async move {
                    let changed = update_doc_struct(command, doc_path.clone(), s3_helper)
                        .await
                        .change_context(Error::UpdatingDocs)?;
                    if changed {
                        Ok(Some(doc_path))
                    } else {
                        Ok(None)
                    }
                }
            })
            .try_buffer_unordered(4)
            .try_filter_map(futures::future::ok)
            .try_collect()
            .await?;

        Ok(changed)
    }
}

#[derive(Debug)]
struct DocFile(PathBuf);

impl std::fmt::Display for DocFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Doc file '{}'", self.0.display())
    }
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to read doc file")]
    ReadingDoc,
    #[display(fmt = "failed to execute example {_0}")]
    ExecuteExample(usize),
    #[display(fmt = "failed to write updated doc file")]
    WriteDoc,
    #[display(fmt = "document toml is not a file '{_0:?}'")]
    NonFile(PathBuf),
    #[display(fmt = "error listing files")]
    ListingFiles,
    #[display(fmt = "error updating docs")]
    UpdatingDocs,
}

impl error_stack::Context for Error {}

/// Populate an input documentation struct.
///
/// This populates the signature from the function, if it is registered.
///
/// This executes the examples to populate the output_csv.
async fn update_doc_struct(
    command: UpdateCommand,
    doc_path: PathBuf,
    s3_helper: S3Helper,
) -> error_stack::Result<bool, Error> {
    let input = tokio::fs::read_to_string(&doc_path)
        .await
        .into_report()
        .change_context(Error::ReadingDoc)?;

    let mut catalog_entry: CatalogEntry = toml::from_str(&input)
        .into_report()
        .change_context(Error::ReadingDoc)?;

    match sparrow_compiler::get_function(&catalog_entry.name) {
        Ok(function) => {
            if catalog_entry.signature.as_ref().map(AsRef::as_ref) == Some(function.signature_str())
            {
                catalog_entry.signature = Some(function.signature_str().to_owned());
            }
        }
        Err(hints) => {
            // This isn't an error. This allows us to document functions that don't exist
            // as "true" functions.
            info!(
                "No function '{}' found, closest matches {:?}",
                catalog_entry.name, hints
            );
        }
    }

    for (index, example) in catalog_entry.examples.iter_mut().enumerate() {
        let span = info_span!("Execute example", function = ?catalog_entry.name, index);
        let _enter = span.enter();

        let output_csv = execute_example::execute_example(example, s3_helper.clone())
            .await
            .change_context(Error::ExecuteExample(index))?;
        example.output_csv = Some(output_csv);
    }

    let output = toml::to_string_pretty(&catalog_entry)
        .into_report()
        .change_context(Error::WriteDoc)?;

    if input != output {
        let diff = similar::TextDiff::from_lines(&input, &output);

        let changes = diff.iter_all_changes().format_with("  ", |change, f| {
            let sign = match change.tag() {
                similar::ChangeTag::Delete => "-",
                similar::ChangeTag::Insert => "+",
                similar::ChangeTag::Equal => " ",
            };
            f(&format_args!("{sign} {change}"))
        });

        match command {
            UpdateCommand::Check => error!("Changes in file '{:?}':\n{}", doc_path, changes),
            UpdateCommand::Update => {
                info!("Changes in file '{:?}':\n{}", doc_path, changes);
                tokio::fs::write(doc_path, output.as_bytes())
                    .await
                    .into_report()
                    .change_context(Error::WriteDoc)?;
            }
        }
        Ok(true)
    } else {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;
    use crate::structs::{ExampleExpression, FunctionExample};

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_docs_up_to_date() {
        sparrow_testing::init_test_logging();

        let doc_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("catalog");
        let changed = update_doc_structs(
            UpdateCommand::Check,
            &UpdateOptions::default(),
            doc_root.clone(),
        )
        .await
        .unwrap();

        if !changed.is_empty() {
            panic!(
                "{:?} catalog entries outdated. Run `cargo run -p sparrow-catalog -- \
                 --input-dir={} update` to update.",
                changed,
                doc_root.to_string_lossy()
            )
        }
    }

    #[test]
    fn test_everything_documented() {
        let doc_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("catalog");

        for function in sparrow_compiler::registered_functions() {
            if function.is_internal() {
                continue;
            }

            let doc_path = doc_root.join(format!("{}.toml", function.name()));
            if !doc_path.exists() {
                let template = CatalogEntry {
                    name: function.name().to_owned(),
                    signature: Some(function.signature_str().to_owned()),
                    operator: None,
                    short_doc: Some("...".to_owned()),
                    experimental: None,
                    long_doc: Some("\n### Parameters\n ...\n\n### Results\n...\n".to_owned()),
                    tags: vec![],
                    examples: vec![FunctionExample {
                        name: Some("...".to_owned()),
                        description: None,
                        expression: ExampleExpression::Expression("...".to_owned()),
                        input_csv: Some("...".to_owned()),
                        output_csv: None,
                        tables: vec![],
                    }],
                };
                let template = toml::to_string_pretty(&template).unwrap();

                panic!(
                    "Missing documentation for '{}'.\nTo create, paste the following into '{}', \
                     fill in, and run `update`.\n\n------\n{}------\n",
                    function.name(),
                    doc_path.to_string_lossy(),
                    template
                );
            }
        }
    }
}
