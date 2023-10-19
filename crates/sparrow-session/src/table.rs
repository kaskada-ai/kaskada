use std::str::FromStr;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::ArrowPrimitiveType;
use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use error_stack::ResultExt;
use futures::TryStreamExt;
use sparrow_api::kaskada::v1alpha;
use sparrow_compiler::{FileSets, TableInfo};
use sparrow_io::in_memory::InMemoryBatches;
use sparrow_runtime::preparer::Preparer;
use sparrow_runtime::stores::ObjectStoreUrl;
use sparrow_runtime::ParquetFile;
use sparrow_runtime::{key_hash_inverse::ThreadSafeKeyHashInverse, stores::ObjectStoreRegistry};

use crate::{Error, Expr};

pub struct Table {
    pub expr: Expr,
    preparer: Preparer,
    key_column: usize,
    key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
    source: Source,
    registry: Arc<ObjectStoreRegistry>,
    prepare_prefix: ObjectStoreUrl,
}

#[derive(Debug)]
enum Source {
    InMemoryBatches(Arc<InMemoryBatches>),
    Parquet(Arc<FileSets>),
}

impl Table {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        table_info: &mut TableInfo,
        key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
        key_column: usize,
        expr: Expr,
        queryable: bool,
        time_unit: Option<&str>,
        object_stores: Arc<ObjectStoreRegistry>,
        source: Option<&str>,
        prepare_prefix: ObjectStoreUrl,
    ) -> error_stack::Result<Self, Error> {
        let prepared_fields: Fields = KEY_FIELDS
            .iter()
            .chain(table_info.schema().fields.iter())
            .cloned()
            .collect();
        let prepared_schema = Arc::new(Schema::new(prepared_fields));
        let prepare_hash = 0;

        // filesets and in_memory_batches should initially be empty.
        // From python, we create the table with no inputs, then add data.
        error_stack::ensure!(table_info.file_sets.is_empty(), Error::internal());
        error_stack::ensure!(table_info.in_memory.is_none(), Error::internal());

        // TODO: Support other sources
        // TODO: Source might be an enum, for safety and clarity
        let source = match source {
            Some("parquet") => {
                let concurrent_file_sets = Arc::new(FileSets::default());

                // Clone into the table_info, so that any modifications to our
                // original reference are reflected within the table_info.
                table_info.file_sets = concurrent_file_sets.clone();

                Source::Parquet(concurrent_file_sets)
            }
            _ => {
                let in_memory_batches =
                    Arc::new(InMemoryBatches::new(queryable, prepared_schema.clone()));

                // Clone into the table_info, so that any modifications to our
                // original reference are reflected within the table_info.
                table_info.in_memory = Some(in_memory_batches.clone());

                Source::InMemoryBatches(in_memory_batches)
            }
        };

        let preparer = Preparer::new(
            table_info.config().clone(),
            prepared_schema.clone(),
            prepare_hash,
            time_unit,
            object_stores.clone(),
        )
        .change_context_lazy(|| Error::CreateTable {
            name: table_info.name().to_owned(),
        })?;

        Ok(Self {
            expr,
            preparer,
            key_hash_inverse,
            key_column: key_column + KEY_FIELDS.len(),
            source,
            registry: object_stores,
            prepare_prefix: prepare_prefix.to_owned(),
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.preparer.schema()
    }

    pub async fn add_data(&self, batch: RecordBatch) -> error_stack::Result<(), Error> {
        let source = match &self.source {
            Source::InMemoryBatches(in_memory) => in_memory.clone(),
            other => error_stack::bail!(Error::internal_msg(format!(
                "expected in-memory data source, saw {:?}",
                other
            ))),
        };

        let prepared = self
            .preparer
            .prepare_batch(batch)
            .change_context(Error::Prepare)?;

        let key_hashes = prepared.column(2).as_primitive();
        let keys = prepared.column(self.key_column);
        self.key_hash_inverse
            .add(keys.as_ref(), key_hashes)
            .await
            .change_context(Error::Prepare)?;

        source
            .add_batch(prepared)
            .await
            .change_context(Error::Prepare)?;
        Ok(())
    }

    pub async fn add_parquet(&self, file: &str) -> error_stack::Result<(), Error> {
        let file_sets = match &self.source {
            Source::Parquet(file_sets) => file_sets.clone(),
            other => error_stack::bail!(Error::internal_msg(format!(
                "expected parquet data source, saw {:?}",
                other
            ))),
        };
        let url =
            ObjectStoreUrl::from_str(file).change_context(Error::InvalidUrl(file.to_owned()))?;

        let prepared = self
            .preparer
            .prepare_parquet(url, &self.prepare_prefix)
            .await
            .change_context(Error::Prepare)?;

        self.update_key_hash_inverse(&prepared).await?;

        // TODO: Slicing
        file_sets.append(None, prepared);

        Ok(())
    }

    /// Given prepared metadata files, update the key hash inverse.
    async fn update_key_hash_inverse(
        &self,
        prepared: &[v1alpha::PreparedFile],
    ) -> error_stack::Result<(), Error> {
        let metadata_paths: Vec<_> = prepared.iter().map(|p| p.metadata_path.clone()).collect();
        let mut metadata_streams = Vec::new();
        for file in metadata_paths {
            let url =
                ObjectStoreUrl::from_str(&file).change_context(Error::InvalidUrl(file.clone()))?;
            let parquet_file = ParquetFile::try_new(&self.registry, url, None)
                .await
                .change_context(Error::OpeningFile(file.clone()))?;
            let stream = parquet_file
                .read_stream(None, None)
                .await
                .change_context(Error::OpeningFile(file.clone()))?;
            metadata_streams.push(stream);
        }
        let mut stream = futures::stream::select_all(metadata_streams)
            .map_err(|e| e.change_context(Error::ReadingFile));
        while let Some(batch) = stream.try_next().await? {
            let hash_col = batch.column(0);
            let hash_col: &UInt64Array = hash_col.as_primitive();
            let entity_key_col = batch.column(1);
            self.key_hash_inverse
                .add(entity_key_col.as_ref(), hash_col)
                .await
                .change_context(Error::ReadingFile)?;
        }

        Ok(())
    }
}

#[static_init::dynamic]
pub(super) static KEY_FIELDS: Vec<arrow_schema::FieldRef> = vec![
    Arc::new(Field::new(
        "_time",
        arrow_array::types::TimestampNanosecondType::DATA_TYPE,
        false,
    )),
    Arc::new(Field::new("_subsort", DataType::UInt64, false)),
    Arc::new(Field::new("_key_hash", DataType::UInt64, false)),
];
