use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use error_stack::{IntoReport, ResultExt};
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryFutureExt};
use object_store::ObjectStore;
use parquet::arrow::{
    parquet_to_arrow_schema_by_columns, ParquetRecordBatchStreamBuilder, ProjectionMask,
};
use parquet::errors::ParquetError;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::FOOTER_SIZE;

use crate::stores::{ObjectStoreRegistry, ObjectStoreUrl};

/// A Parquet file in an [ObjectStore]. May be read asynchronously.
#[derive(Clone)]
pub struct ParquetFile {
    object_store: Arc<dyn ObjectStore>,
    path: object_store::path::Path,
    metadata: Arc<ParquetMetaData>,
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

const BATCH_SIZE_ROWS: usize = 100_000;

impl error_stack::Context for Error {}

impl ParquetFile {
    /// Create a new parquet file for the given URL.
    ///
    /// This will return an error if the file doesn't exist.
    pub async fn try_new(
        object_stores: &ObjectStoreRegistry,
        url: ObjectStoreUrl,
    ) -> error_stack::Result<Self, Error> {
        let object_store = object_stores
            .object_store(&url)
            .change_context(Error::InvalidUrl)?
            .clone();
        let path = url.path().change_context(Error::InvalidUrl)?;

        let metadata = get_parquet_metadata(object_store.as_ref(), &path).await?;

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
            path,
            metadata,
            schema,
        })
    }

    pub async fn read_stream(
        self,
        projection: Option<Vec<usize>>,
    ) -> error_stack::Result<BoxStream<'static, error_stack::Result<RecordBatch, Error>>, Error>
    {
        let path = self.path.clone();
        let metadata = self.metadata.clone();

        let mut batch_stream = ParquetRecordBatchStreamBuilder::new(self)
            .await
            .into_report()
            .change_context(Error::ReadingParquetFile)
            .attach_printable_lazy(|| path.clone())?;

        batch_stream = batch_stream.with_batch_size(BATCH_SIZE_ROWS);
        // if let Some(projection) = projection {
        //     let mask = ProjectionMask::leaves(metadata.file_metadata().schema_descr(), projection);
        //     batch_stream = batch_stream.with_projection(mask);
        // }

        let batch_stream = batch_stream
            .build()
            .into_report()
            .change_context(Error::ReadingParquetFile)
            .attach_printable_lazy(|| path.clone())?;

        Ok(batch_stream
            .map(move |batch| {
                batch
                    .into_report()
                    .change_context(Error::ReadingParquetFile)
                    .attach_printable_lazy(|| path.clone())
            })
            .boxed())
    }
}

impl parquet::arrow::async_reader::AsyncFileReader for ParquetFile {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<usize>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<bytes::Bytes>> {
        self.object_store
            .get_range(&self.path, range)
            // Need to produce parquet error, so can't use `error_stack` here.
            .map_err(|e| ParquetError::General(format!("AsyncFileReader::get_bytes error: {e}")))
            .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<std::ops::Range<usize>>,
    ) -> futures::future::BoxFuture<'_, parquet::errors::Result<Vec<bytes::Bytes>>> {
        async move {
            self.object_store
                .get_ranges(&self.path, &ranges)
                .await
                // Need to produce parquet error, so can't use `error_stack` here.
                .map_err(|e| {
                    ParquetError::General(format!("AsyncFileReader::get_byte_ranges error: {e}"))
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
        futures::future::ready(Ok(self.metadata.clone())).boxed()
    }
}

/// Fetch the parquet metadata for the given path.
///
/// # Optimization Opportunity
/// This does two `get_range` calls -- one for the FOOTER_SIZE bytes to determine
/// the footer size, then one for the appropriate number of bytes. It may be better
/// to start with a reasonable guess (a few KB) and fetch that, and only do the second
/// fetch if the FOOTER_SIZE indicates it wasn't enough. This could cut the number of
/// object store calls to get metadata in half. Of course, if the metadata is stored in
/// the database, then the calls wouldn't be needed at all.
async fn get_parquet_metadata(
    object_store: &dyn ObjectStore,
    path: &object_store::path::Path,
) -> error_stack::Result<Arc<ParquetMetaData>, Error> {
    // First determine the length of the file.
    let metadata = object_store
        .head(path)
        .await
        .into_report()
        .change_context(Error::InvalidParquetMetadata)?;
    let length = metadata.size;

    let footer_bytes = object_store
        .get_range(path, length - FOOTER_SIZE..length)
        .await
        .into_report()
        .change_context(Error::InvalidParquetMetadata)?;
    let footer_bytes: &[u8; FOOTER_SIZE] = footer_bytes
        .as_ref()
        .try_into()
        .expect("Footer bytes should have been FOOTER_SIZE");
    let metadata_len = parquet::file::footer::decode_footer(footer_bytes)
        .into_report()
        .change_context(Error::InvalidParquetMetadata)?;

    let metadata_bytes = object_store
        .get_range(
            path,
            length - metadata_len - FOOTER_SIZE..length - FOOTER_SIZE,
        )
        .await
        .into_report()
        .change_context(Error::InvalidParquetMetadata)?;

    parquet::file::footer::decode_metadata(metadata_bytes.as_ref())
        .into_report()
        .map(Arc::new)
        .change_context(Error::InvalidParquetMetadata)
}
