use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::ArrowPrimitiveType;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use error_stack::ResultExt;
use sparrow_compiler::TableInfo;
use sparrow_merge::InMemoryBatches;
use sparrow_runtime::key_hash_inverse::ThreadSafeKeyHashInverse;
use sparrow_runtime::preparer::Preparer;

use crate::{Error, Expr};

pub struct Table {
    pub expr: Expr,
    preparer: Preparer,
    in_memory_batches: Arc<InMemoryBatches>,
    key_column: usize,
    key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
}

impl Table {
    pub(crate) fn new(
        table_info: &mut TableInfo,
        key_hash_inverse: Arc<ThreadSafeKeyHashInverse>,
        key_column: usize,
        expr: Expr,
        retained: bool,
        time_unit: Option<&str>,
    ) -> error_stack::Result<Self, Error> {
        let prepared_fields: Fields = KEY_FIELDS
            .iter()
            .chain(table_info.schema().fields.iter())
            .cloned()
            .collect();
        let prepared_schema = Arc::new(Schema::new(prepared_fields));
        let prepare_hash = 0;

        assert!(table_info.in_memory.is_none());
        let in_memory_batches = Arc::new(InMemoryBatches::new(retained, prepared_schema.clone()));
        table_info.in_memory = Some(in_memory_batches.clone());

        let preparer = Preparer::new(
            table_info.config().time_column_name.clone(),
            table_info.config().subsort_column_name.clone(),
            table_info.config().group_column_name.clone(),
            prepared_schema,
            prepare_hash,
            time_unit,
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
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.preparer.schema()
    }

    pub fn add_data(&mut self, batch: RecordBatch) -> error_stack::Result<(), Error> {
        let prepared = self
            .preparer
            .prepare_batch(batch)
            .change_context(Error::Prepare)?;

        let key_hashes = prepared.column(2).as_primitive();
        let keys = prepared.column(self.key_column);
        self.key_hash_inverse
            .blocking_add(keys.as_ref(), key_hashes)
            .change_context(Error::Prepare)?;

        self.in_memory_batches
            .add_batch(prepared)
            .change_context(Error::Prepare)?;
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
