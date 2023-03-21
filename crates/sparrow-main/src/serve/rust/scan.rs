use std::str::FromStr;
use std::sync::Arc;

use error_stack::ResultExt;
use fenl_catalog::scan::{Error, Scan};
use fenl_common::ExtensionContext;
use futures::stream::BoxStream;

use crate::object_stores::{ObjectStoreRegistry, ObjectStoreUrl};
use crate::parquet::parquet_file::ParquetFile;

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct ParquetScan {
    pub(super) urls: Vec<String>,
}

#[typetag::serde(name = "parquet_scan")]
#[async_trait::async_trait]
impl Scan for ParquetScan {
    fn clone_box(&self) -> Box<dyn Scan> {
        Box::new(self.clone())
    }

    async fn scan(
        &self,
        context: &ExtensionContext,
    ) -> error_stack::Result<
        BoxStream<'static, error_stack::Result<arrow_array::RecordBatch, Error>>,
        Error,
    > {
        assert_eq!(self.urls.len(), 1);
        let registry = context.get_ext::<Arc<ObjectStoreRegistry>>().clone();
        let url =
            ObjectStoreUrl::from_str(&self.urls[0]).change_context(Error::FailedToScanSource)?;
        let parquet_file = ParquetFile::try_new(registry.as_ref(), url)
            .await
            .change_context(Error::FailedToScanSource)?;

        parquet_file
            .read_stream()
            .await
            .change_context(Error::FailedToScanSource)
    }
}
