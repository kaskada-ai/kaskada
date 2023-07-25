use std::path::{Path, PathBuf};

use error_stack::{IntoReport, Report, ResultExt};
use futures::{StreamExt, TryStreamExt};
use hashbrown::HashSet;
use tera::Tera;

use crate::list_doc_files;
use crate::structs::CatalogEntry;

mod filters;

#[derive(clap::Args, Debug)]
pub(super) struct GenerateOptions {
    /// Path to the template directory.
    #[arg(long)]
    template_dir: PathBuf,

    /// Output directory to write the output to.
    #[arg(long)]
    output_dir: PathBuf,
}

struct CatalogContext {
    functions: Vec<CatalogEntry>,
    /// The set of all tags. Computing this within the template would be
    /// painful.
    tags: HashSet<String>,
    base_context: tera::Context,
}

impl CatalogContext {
    async fn try_new(doc_root: PathBuf) -> error_stack::Result<Self, Error> {
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

        let mut base_context = tera::Context::new();
        base_context.insert("functions", &functions);
        base_context.insert("tags", &tags);
        Ok(Self {
            functions,
            tags,
            base_context,
        })
    }

    fn global_context(&self) -> tera::Context {
        self.base_context.clone()
    }

    fn function_contexts(&self) -> impl Iterator<Item = (&str, tera::Context)> + '_ {
        self.functions.iter().map(|entry| {
            let mut context = self.base_context.clone();
            context.insert("function", &entry);
            (entry.name.as_ref(), context)
        })
    }

    fn tag_contexts(&self) -> impl Iterator<Item = (&str, tera::Context)> + '_ {
        self.tags.iter().map(|tag| {
            let mut context = self.base_context.clone();
            context.insert("tag", &tag);
            (tag.as_ref(), context)
        })
    }
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "failed to make output dir")]
    MakeOutputDir,
    #[display(fmt = "failed to list files")]
    ListingFiles,
    #[display(fmt = "failed to read input doc")]
    ReadingInputDoc,
    #[display(fmt = "failed to compile templates")]
    CompileTemplates,
    #[display(fmt = "failed to render template '{_0}")]
    RenderTemplate(String),
}

impl error_stack::Context for Error {}

#[allow(clippy::print_stdout)]
pub(super) async fn generate(
    doc_root: PathBuf,
    options: GenerateOptions,
) -> error_stack::Result<(), Error> {
    // Create the output directory if it doesn't exist.
    tokio::fs::create_dir_all(&options.output_dir)
        .await
        .into_report()
        .change_context(Error::MakeOutputDir)?;

    render_templates(doc_root, &options.template_dir, &options.output_dir).await?;

    println!(
        "Generated catalog contents in {}",
        options.output_dir.display()
    );

    Ok(())
}

/// Render the top-level files in the `templates` directory.
pub(super) async fn render_templates(
    doc_root: PathBuf,
    template_dir: &Path,
    output_dir: &Path,
) -> error_stack::Result<(), Error> {
    let context = CatalogContext::try_new(doc_root).await?;

    let template_glob = format!("{}/**/*", template_dir.to_string_lossy());
    let mut tera = tera::Tera::new(&template_glob)
        .into_report()
        .change_context(Error::CompileTemplates)?;
    tera.register_filter("link_fenl_types", filters::LinkFenlTypes);

    // 0. Make the output directories.
    tokio::fs::create_dir_all(output_dir.join("category"))
        .await
        .into_report()
        .change_context(Error::MakeOutputDir)?;
    tokio::fs::create_dir_all(output_dir.join("function"))
        .await
        .into_report()
        .change_context(Error::MakeOutputDir)?;

    let mut futures = Vec::new();

    // 1. Render `nav.adoc`, `index.adoc` and `operators.adoc`.
    futures.push(render(
        &tera,
        context.global_context(),
        "nav.adoc",
        output_dir.join("nav.adoc"),
    ));
    futures.push(render(
        &tera,
        context.global_context(),
        "index.adoc",
        output_dir.join("index.adoc"),
    ));
    futures.push(render(
        &tera,
        context.global_context(),
        "operators.adoc",
        output_dir.join("category/operators.adoc"),
    ));

    // 2. Render `<category>.adoc` for each category.
    for (tag, context) in context.tag_contexts() {
        futures.push(render(
            &tera,
            context,
            "category.adoc",
            output_dir.join(format!("category/{tag}.adoc")),
        ));
    }

    // 3. Render `function.adoc` for each function.
    for (name, context) in context.function_contexts() {
        futures.push(render(
            &tera,
            context,
            "function.adoc",
            output_dir.join(format!("function/{name}.adoc")),
        ))
    }

    futures::stream::iter(futures)
        .buffer_unordered(8)
        .try_collect()
        .await
}

async fn render(
    tera: &Tera,
    context: tera::Context,
    template_name: &str,
    destination: PathBuf,
) -> error_stack::Result<(), Error> {
    let error = || Error::RenderTemplate(template_name.to_owned());
    let contents = tera
        .render(template_name, &context)
        .map_err(|e| {
            // Converting tera errors to error stack drops important context.
            // Make sure to grab the causes.
            let mut sources = Vec::new();
            let mut error: &dyn std::error::Error = &e;
            while let Some(source) = error.source() {
                sources.push(source.to_string());
                error = source;
            }

            let mut report = Report::new(e);
            for source in sources {
                report = report.attach_printable(source);
            }
            report
        })
        .change_context_lazy(error)?;

    tokio::fs::write(destination, contents)
        .await
        .into_report()
        .change_context_lazy(error)
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
