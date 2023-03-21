use std::str::FromStr;
use std::sync::Arc;

use crate::object_stores::{ObjectStoreRegistry, ObjectStoreUrl};
use crate::parquet::parquet_file::ParquetFile;
use crate::parquet::ParquetScan;
use arrow_schema::{Schema, SchemaRef};
use error_stack::ResultExt;
use fenl_catalog::scan::Scan;
use fenl_catalog::source;
use fenl_common::ExtensionContext;
use futures::stream::StreamExt;
use futures::TryStreamExt;

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct ParquetSource {
    urls: Vec<String>,
    #[serde(skip, default)]
    schema: async_once_cell::OnceCell<SchemaRef>,
}

async fn get_combined_schema(
    context: &ExtensionContext,
    urls: &[String],
) -> error_stack::Result<SchemaRef, Error> {
    let registry = context.get_ext::<Arc<ObjectStoreRegistry>>().clone();

    let file_info: Vec<_> = futures::stream::iter(urls)
        .then(|url| {
            let registry = registry.clone();
            async move {
                let parsed_url = ObjectStoreUrl::from_str(url)
                    .change_context(Error::InvalidUrl)
                    .attach_printable_lazy(|| format!("URL: {url}"))?;
                ParquetFile::try_new(registry.as_ref(), parsed_url)
                    .await
                    .change_context(Error::Internal)
                    .attach_printable_lazy(|| format!("URL: {url:?}"))
            }
        })
        .try_collect()
        .await?;

    let combined_schema = if file_info.is_empty() {
        Arc::new(Schema::empty())
    } else {
        assert_eq!(
            file_info.len(),
            1,
            "TODO: Support multiple files by combining schema or asserting they're equal"
        );

        file_info[0].file_schema().clone()
    };

    Ok(combined_schema)
}

#[typetag::serde(name = "parquet")]
#[async_trait::async_trait]
impl source::Source for ParquetSource {
    async fn schema(
        &self,
        context: &ExtensionContext,
    ) -> error_stack::Result<SchemaRef, source::Error> {
        self.schema
            .get_or_try_init(get_combined_schema(context, &self.urls))
            .await
            .change_context(source::Error::Internal)
            .cloned()
    }

    async fn scan(
        &self,
        _context: &ExtensionContext,
    ) -> error_stack::Result<Box<dyn Scan>, source::Error> {
        let urls = self.urls.clone();
        Ok(Box::new(ParquetScan { urls }))
    }
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "invalid parquet file url")]
    InvalidUrl,
    #[display(fmt = "internal error reading parquet files")]
    Internal,
}

impl error_stack::Context for Error {}
