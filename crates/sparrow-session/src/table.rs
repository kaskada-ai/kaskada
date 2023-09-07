use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::ArrowPrimitiveType;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use error_stack::ResultExt;
use sparrow_api::kaskada::v1alpha::compute_table::FileSet;
use sparrow_api::kaskada::v1alpha::{compute_table, PreparedFile};
use sparrow_compiler::TableInfo;
use sparrow_merge::InMemoryBatches;
use sparrow_runtime::preparer::Preparer;
use sparrow_runtime::{key_hash_inverse::ThreadSafeKeyHashInverse, stores::ObjectStoreRegistry};

use crate::{Error, Expr};

pub struct Table {
    pub expr: Expr,
    preparer: Preparer,
    in_memory_batches: Arc<InMemoryBatches>,
    key_column: usize,
    key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
    files: Arc<Vec<compute_table::FileSet>>,
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

impl Table {
    pub(crate) fn new(
        table_info: &mut TableInfo,
        key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
        key_column: usize,
        expr: Expr,
        queryable: bool,
        time_unit: Option<&str>,
        object_stores: Arc<ObjectStoreRegistry>,
    ) -> error_stack::Result<Self, Error> {
        let prepared_fields: Fields = KEY_FIELDS
            .iter()
            .chain(table_info.schema().fields.iter())
            .cloned()
            .collect();
        let prepared_schema = Arc::new(Schema::new(prepared_fields));
        let prepare_hash = 0;

        assert!(table_info.in_memory.is_none());

        let in_memory_batches = Arc::new(InMemoryBatches::new(queryable, prepared_schema.clone()));
        table_info.in_memory = Some(in_memory_batches.clone());

        let prepared_files: Vec<PreparedFile> = Vec::new();
        // The table info here should be empty. We create with empty source and empty filesets.
        // TODO: Slicing
        let file_set = Arc::new(compute_table::FileSet {
            slice_plan: None,
            prepared_files: prepared_files.clone(),
        });
        table_info.file_sets = Some(file_set.clone());

        // TODO: FRAZ - Preparer can hold the ObjectStRegistry. Needs it, to read parquet files.
        let preparer = Preparer::new(
            // table_info.config().time_column_name.clone(),
            // table_info.config().subsort_column_name.clone(),
            // table_info.config().group_column_name.clone(),
            table_info.config().clone(),
            prepared_schema,
            prepare_hash,
            time_unit,
            object_stores,
        )
        .change_context_lazy(|| Error::CreateTable {
            name: table_info.name().to_owned(),
        })?;

        Ok(Self {
            expr,
            preparer,
            in_memory_batches,
            key_hash_inverse,
            key_column: key_column + KEY_FIELDS.len(),
            files: file_set,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.preparer.schema()
    }

    pub async fn add_data(&self, batch: RecordBatch) -> error_stack::Result<(), Error> {
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

        self.in_memory_batches
            .add_batch(prepared)
            .await
            .change_context(Error::Prepare)?;
        Ok(())
    }

    pub async fn add_parquet(&mut self, path: &str) -> error_stack::Result<(), Error> {
        let prepared = self
            .preparer
            .prepare_parquet(path)
            .await
            .change_context(Error::Prepare)?;
        self.files.push(prepared);

        // TODO: Also add files to the session's table info?
        // could pass in session to add_parquet method, then mutate datacontext.table info from there?

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
