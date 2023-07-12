use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, ResultExt};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryFutureExt};
use object_store::{ObjectMeta, ObjectStore};
use parquet::arrow::{
    parquet_to_arrow_schema_by_columns, ParquetRecordBatchStreamBuilder, ProjectionMask,
};
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;

use crate::stores::{ObjectStoreRegistry, ObjectStoreUrl};

/// A Parquet file in an [ObjectStore]. May be read asynchronously.
#[derive(Clone)]
pub struct ParquetFile {
    object_store: Arc<dyn ObjectStore>,
    pub object_meta: ObjectMeta,
    parquet_metadata: Arc<ParquetMetaData>,
    pub schema: SchemaRef,
}

#[derive(derive_more::Display, Debug)]
pub enum Error {
    #[display(fmt = "invalid parquet file url")]
    InvalidUrl,
    #[display(fmt = "invalid parquet file schema")]
    InvalidFileSchema,
    #[display(fmt = "invalid parquet file metadata")]
    InvalidParquetMetadata,
    #[display(fmt = "reading parquet file")]
    ReadingParquetFile,
}

const BATCH_SIZE_ROWS: usize = 4_096;

impl error_stack::Context for Error {}

impl ParquetFile {
    /// Create a new parquet file for the given URL.
    ///
    /// This will return an error if the file doesn't exist.
    pub async fn try_new(
        object_stores: &ObjectStoreRegistry,
        url: ObjectStoreUrl,
        object_meta: Option<ObjectMeta>,
    ) -> error_stack::Result<Self, Error> {
        let object_store = object_stores
            .object_store(&url)
            .change_context(Error::InvalidUrl)?
            .clone();
        let path = url.path().change_context(Error::InvalidUrl)?;

        let object_meta = match object_meta {
            Some(object_meta) => object_meta,
            None => object_store
                .head(&path)
                .await
                .into_report()
                .change_context(Error::InvalidParquetMetadata)?,
        };

        let metadata = get_parquet_metadata(object_store.as_ref(), &object_meta).await?;

        let key_value_metadata = metadata.file_metadata().key_value_metadata();
        let schema = parquet_to_arrow_schema_by_columns(
            metadata.file_metadata().schema_descr(),
            parquet::arrow::ProjectionMask::all(),
            key_value_metadata,
        )
        .map(Arc::new)
        .into_report()
        .change_context(Error::InvalidFileSchema)?;

        Ok(Self {
            object_store,
            object_meta,
            parquet_metadata: metadata,
            schema,
        })
    }

    pub fn num_rows(&self) -> usize {
        self.parquet_metadata.file_metadata().num_rows() as usize
    }

    pub async fn read_stream(
        &self,
        batch_size: Option<usize>,
        projection: Option<Vec<usize>>,
    ) -> error_stack::Result<BoxStream<'static, error_stack::Result<RecordBatch, Error>>, Error>
    {
        let reader = AsyncParquetObjectReader {
            object_store: self.object_store.clone(),
            object_meta: self.object_meta.clone(),
            parquet_metadata: self.parquet_metadata.clone(),
        };

        let mut batch_stream = ParquetRecordBatchStreamBuilder::new(reader)
            .await
            .into_report()
            .change_context(Error::ReadingParquetFile)
            .attach_printable_lazy(|| self.object_meta.location.clone())?;

        batch_stream = batch_stream.with_batch_size(batch_size.unwrap_or(BATCH_SIZE_ROWS));
        if let Some(projection) = projection {
            let mask = ProjectionMask::roots(
                self.parquet_metadata.file_metadata().schema_descr(),
                projection,
            );
            batch_stream = batch_stream.with_projection(mask);
        }

        let batch_stream = batch_stream
            .build()
            .into_report()
            .change_context(Error::ReadingParquetFile)
            .attach_printable_lazy(|| self.object_meta.location.clone())?;

        let location = self.object_meta.location.clone();
        Ok(batch_stream
            .map(move |batch| {
                batch
                    .into_report()
                    .change_context(Error::ReadingParquetFile)
                    .attach_printable_lazy(|| location.clone())
            })
            .boxed())
    }
}

struct AsyncParquetObjectReader {
    object_store: Arc<dyn ObjectStore>,
    object_meta: ObjectMeta,
    parquet_metadata: Arc<ParquetMetaData>,
}

impl parquet::arrow::async_reader::AsyncFileReader for AsyncParquetObjectReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<usize>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        self.object_store
            .get_range(&self.object_meta.location, range)
            // Need to produce parquet error, so can't use `error_stack` here.
            .map_err(|e| {
                ParquetError::General(format!("AsyncParquetObjectReader::get_bytes error: {e}"))
            })
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<usize>>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        async move {
            self.object_store
                .get_ranges(&self.object_meta.location, &ranges)
                .await
                // Need to produce parquet error, so can't use `error_stack` here.
                .map_err(|e| {
                    ParquetError::General(format!(
                        "AsyncParquetObjectReader::get_byte_ranges error: {e}"
                    ))
                })
        }
        .boxed()
    }

    fn get_metadata(
        &mut self,
    ) -> futures::future::BoxFuture<
        '_,
        parquet::errors::Result<Arc<parquet::file::metadata::ParquetMetaData>>,
    > {
        futures::future::ready(Ok(self.parquet_metadata.clone())).boxed()
    }
}

/// Number of bytes to prefetch of the footer.
///
/// Providing a larger than default (8) value allows skipping an extra round
/// of fetches:
///
/// 1. Fetch last `PREFETCH_FOOTER_BYTES`
/// 2. Look at last 8 bytes to determine size of footer.
/// 3. If the footer is larger than `PREFETCH_FOOTER_BYTES` fetch more.
const PREFETCH_FOOTER_BYTES: usize = 1024;

/// Fetch the parquet metadata for the given path.
async fn get_parquet_metadata(
    object_store: &dyn ObjectStore,
    object_meta: &ObjectMeta,
) -> error_stack::Result<Arc<ParquetMetaData>, Error> {
    let fetch = |byte_range| {
        object_store
            .get_range(&object_meta.location, byte_range)
            .map_err(|e| parquet::errors::ParquetError::External(Box::new(e)))
    };
    parquet::arrow::async_reader::fetch_parquet_metadata(
        fetch,
        object_meta.size,
        Some(PREFETCH_FOOTER_BYTES),
    )
    .await
    .into_report()
    .change_context(Error::InvalidParquetMetadata)
    .map(Arc::new)
}
