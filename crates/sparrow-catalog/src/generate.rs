use std::path::PathBuf;

use error_stack::{IntoReport, ResultExt};
use futures::TryStreamExt;
use hashbrown::HashSet;
use serde::Serialize;
use tokio::io::AsyncWriteExt;

use crate::list_doc_files;
use crate::structs::CatalogEntry;

mod filters;

#[derive(clap::Args, Debug)]
pub(super) struct GenerateOptions {
    /// Path to the template directory.
    #[arg(long)]
    template_dir: PathBuf,

    /// Output file to write the output to. Defaults to stdout.
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Serialize)]
struct CatalogContext {
    pub functions: Vec<CatalogEntry>,
    /// The set of all tags. Computing this within the template would be
    /// painful.
    pub tags: HashSet<String>,
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to write catalog")]
    WriteCatalog,
    #[display(fmt = "failed to print catalog")]
    PrintCatalog,
    #[display(fmt = "failed to list files")]
    ListingFiles,
    #[display(fmt = "failed to read input doc")]
    ReadingInputDoc,
    #[display(fmt = "failed to compile templates")]
    CompileTemplates,
    #[display(fmt = "failed to render template")]
    RenderTemplate,
}

impl error_stack::Context for Error {}

pub(super) async fn generate(
    doc_root: PathBuf,
    options: GenerateOptions,
) -> error_stack::Result<(), Error> {
    let catalog = generate_catalog(doc_root, options.template_dir).await?;

    match options.output {
        Some(path) => tokio::fs::write(path, catalog)
            .await
            .into_report()
            .change_context(Error::WriteCatalog)?,
        None => tokio::io::stdout()
            .write_all(catalog.as_bytes())
            .await
            .into_report()
            .change_context(Error::PrintCatalog)?,
    }

    Ok(())
}

/// Generate the `catalog.md` from the templates and function docs.
///
/// This applies the `catalog.md` template.
pub(super) async fn generate_catalog(
    doc_root: PathBuf,
    template_dir: PathBuf,
) -> error_stack::Result<String, Error> {
    let mut functions: Vec<_> = list_doc_files(doc_root)
        .await
        .into_report()
        .change_context(Error::ListingFiles)?
        .map_err(|e| error_stack::report!(e).change_context(Error::ListingFiles))
        .map_ok(parse_doc_file)
        .try_buffer_unordered(4)
        .try_collect()
        .await?;

    functions.sort_by(|a, b| a.name.cmp(&b.name));

    let mut tags = HashSet::new();
    for function in &functions {
        for tag in &function.tags {
            tags.get_or_insert_with(tag, |tag| tag.to_owned());
        }
    }

    let catalog = CatalogContext { functions, tags };

    let template_glob = format!("{}/**/*.md", template_dir.to_string_lossy());
    let mut tera = tera::Tera::new(&template_glob)
        .into_report()
        .change_context(Error::CompileTemplates)?;
    tera.register_filter("csv2md", filters::CsvToMdFilter);
    tera.register_filter("link_fenl_types", filters::LinkFenlTypes);
    tera.register_filter("warning_block", filters::WarningBlockQuote);
    let context = tera::Context::from_serialize(catalog)
        .into_report()
        .change_context(Error::RenderTemplate)?;

    let template_name = "catalog.md";

    let catalog = tera
        .render(template_name, &context)
        .into_report()
        .change_context(Error::RenderTemplate)?;
    Ok(catalog)
}

/// Parse an existing `.toml` document to a CatalogEntry.
async fn parse_doc_file(doc_path: PathBuf) -> error_stack::Result<CatalogEntry, Error> {
    let input = tokio::fs::read_to_string(&doc_path)
        .await
        .into_report()
        .change_context(Error::ReadingInputDoc)?;

    let catalog_entry: CatalogEntry = toml::from_str(&input)
        .into_report()
        .change_context(Error::ReadingInputDoc)?;
    Ok(catalog_entry)
}
