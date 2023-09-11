use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::ArrowPrimitiveType;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use error_stack::ResultExt;
use sparrow_api::kaskada::v1alpha::compute_table::FileSet;
use sparrow_api::kaskada::v1alpha::{compute_table, PreparedFile};
use sparrow_compiler::{ConcurrentFileSets, TableInfo};
use sparrow_merge::InMemoryBatches;
use sparrow_runtime::preparer::Preparer;
use sparrow_runtime::{key_hash_inverse::ThreadSafeKeyHashInverse, stores::ObjectStoreRegistry};

use crate::{Error, Expr};

pub struct Table {
    pub expr: Expr,
    preparer: Preparer,
    // in_memory_batches: Arc<InMemoryBatches>,
    key_column: usize,
    key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
    // files: Arc<Vec<compute_table::FileSet>>,
    source: Source,
    // TODO: FRAZ: How is tableinfo created?
    // Answer:  DataContext.add_table (ComputeTable holds the filesets)
    // ComputeTable is created via session.add_table(). With no filesets nor source
    // Make sure new files are added to compute table?

    // TODO: Need to pass in the Source Type, so we can check that
    // when we call add_data, or add_parquet on the `Table` in python, we're
    // adding the correct data type to the source

    // TODO: Wrap the in_memory_batches and files in a `Source` type?
    // Batch, Paths, Streams, per type?
}

#[derive(Debug)]
enum Source {
    InMemoryBatches(Arc<InMemoryBatches>),
    Parquet(ConcurrentFileSets),
}

impl Table {
    pub(crate) fn new(
        table_info: &mut TableInfo,
        key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
        key_column: usize,
        expr: Expr,
        queryable: bool,
        time_unit: Option<&str>,
        object_stores: Arc<ObjectStoreRegistry>,
        source: Option<&str>,
    ) -> error_stack::Result<Self, Error> {
        let prepared_fields: Fields = KEY_FIELDS
            .iter()
            .chain(table_info.schema().fields.iter())
            .cloned()
            .collect();
        let prepared_schema = Arc::new(Schema::new(prepared_fields));
        let prepare_hash = 0;

        // Filesets and in_memory should initially be empty.
        // From python, we create the table with no inputs, then add data.
        // TODO: Make these a single "Source" type in TableInfo?
        error_stack::ensure!(table_info.file_sets().is_empty(), Error::internal());
        error_stack::ensure!(table_info.in_memory.is_none(), Error::internal());

        // TODO: Slicing
        let concurrent_file_sets = ConcurrentFileSets::default();

        // Clone into the table_info, so that any modifications to our
        // original reference are reflected within the table_info.
        table_info.file_sets = concurrent_file_sets.clone();

        let preparer = Preparer::new(
            table_info.config().clone(),
            prepared_schema.clone(),
            prepare_hash,
            time_unit,
            object_stores,
        )
        .change_context_lazy(|| Error::CreateTable {
            name: table_info.name().to_owned(),
        })?;

        // TODO: FRAZ NExt steps:
        // 1. Async thing -- look in exeuction.rs and copy that pyarrow async method
        // in my table.rs add_parquet file
        // 2. See how Scan is reading in_memory and skipping normal execution, and emulate that
        //    for parquet files

        // TODO: Support other sources
        let source = match source {
            Some("parquet") => Source::Parquet(concurrent_file_sets),
            _ => Source::InMemoryBatches(Arc::new(InMemoryBatches::new(prepared_schema))),
        };

        Ok(Self {
            expr,
            preparer,
            key_hash_inverse,
            key_column: key_column + KEY_FIELDS.len(),
            source,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.preparer.schema()
    }

    pub async fn add_data(&mut self, batch: RecordBatch) -> error_stack::Result<(), Error> {
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

    pub async fn add_parquet(&self, path: String) -> error_stack::Result<(), Error> {
        let mut source = match &self.source {
            Source::Parquet(file_sets) => file_sets.clone(),
            other => error_stack::bail!(Error::internal_msg(format!(
                "expected parquet data source, saw {:?}",
                other
            ))),
        };

        let prepared = self
            .preparer
            .prepare_parquet(&path)
            .await
            .change_context(Error::Prepare)?;

        source.push(FileSet {
            slice_plan: None,
            prepared_files: prepared,
        });

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
