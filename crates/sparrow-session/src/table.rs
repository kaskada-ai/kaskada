use std::sync::Arc;

use arrow_array::types::ArrowPrimitiveType;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use error_stack::ResultExt;
use sparrow_compiler::TableInfo;
use sparrow_merge::InMemoryBatches;
use sparrow_runtime::preparer::Preparer;

use crate::{Error, Expr};

pub struct Table {
    pub expr: Expr,
    preparer: Preparer,
    in_memory_batches: Arc<InMemoryBatches>,
}

impl Table {
    pub(crate) fn new(table_info: &mut TableInfo, expr: Expr) -> Self {
        let prepared_fields: Fields = KEY_FIELDS
            .iter()
            .chain(table_info.schema().fields.iter())
            .cloned()
            .collect();
        let prepared_schema = Arc::new(Schema::new(prepared_fields));
        let prepare_hash = 0;

        assert!(table_info.in_memory.is_none());
        let in_memory_batches = Arc::new(InMemoryBatches::new(prepared_schema.clone()));
        table_info.in_memory = Some(in_memory_batches.clone());

        let preparer = Preparer::new(
            table_info.config().time_column_name.clone(),
            table_info.config().subsort_column_name.clone(),
            table_info.config().group_column_name.clone(),
            prepared_schema,
            prepare_hash,
        );

        Self {
            expr,
            preparer,
            in_memory_batches,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        self.preparer.schema()
    }

    pub fn add_data(&mut self, batch: RecordBatch) -> error_stack::Result<(), Error> {
        let prepared = self
            .preparer
            .prepare_batch(batch)
            .change_context(Error::Prepare)?;
        self.in_memory_batches
            .add_batch(prepared)
            .change_context(Error::Prepare)?;
        Ok(())
    }
}

#[static_init::dynamic]
static KEY_FIELDS: Vec<arrow_schema::FieldRef> = vec![
    Arc::new(Field::new(
        "_time",
        arrow_array::types::TimestampNanosecondType::DATA_TYPE,
        false,
    )),
    Arc::new(Field::new("_subsort", DataType::UInt64, false)),
    Arc::new(Field::new("_key_hash", DataType::UInt64, false)),
];
