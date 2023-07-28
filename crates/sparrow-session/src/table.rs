use std::sync::Arc;

use arrow_array::types::ArrowPrimitiveType;
use arrow_array::RecordBatch;
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use error_stack::ResultExt;
use sparrow_compiler::TableInfo;
use sparrow_runtime::preparer::Preparer;

use crate::{Error, Expr};

pub struct Table {
    pub expr: Expr,
    preparer: Preparer,
    data: RecordBatch,
}

impl Table {
    pub(crate) fn new(expr: Expr, table_info: &TableInfo) -> Self {
        let prepared_fields: Fields = KEY_FIELDS
            .iter()
            .chain(table_info.schema().fields.iter())
            .cloned()
            .collect();
        let prepared_schema = Arc::new(Schema::new(prepared_fields));
        let prepare_hash = 0;

        let preparer = Preparer::new(
            table_info.config().time_column_name.clone(),
            table_info.config().subsort_column_name.clone(),
            table_info.config().group_column_name.clone(),
            prepared_schema.clone(),
            prepare_hash,
        );

        Self {
            expr,
            preparer,
            data: RecordBatch::new_empty(prepared_schema),
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

        // TODO: Merge the data in.
        assert_eq!(self.data.num_rows(), 0);
        self.data = prepared;
        Ok(())
    }

    pub fn data(&self) -> &RecordBatch {
        &self.data
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
