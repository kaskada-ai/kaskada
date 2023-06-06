use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;

#[derive(Debug)]
pub struct RecordBatchAttachment {
    label: &'static str,
    batch: RecordBatch,
}

impl RecordBatchAttachment {
    pub fn new(label: &'static str, batch: &RecordBatch) -> Self {
        Self {
            label,
            batch: batch.clone(),
        }
    }
}

impl std::fmt::Display for RecordBatchAttachment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let num_rows = self.batch.num_rows();
        if num_rows < 5 {
            let batch = batch_to_json(self.batch.clone())?;
            write!(f, "Record Batch: {}\n{batch}", self.label)
        } else {
            let prefix = batch_to_json(self.batch.slice(0, 2))?;
            let suffix = batch_to_json(self.batch.slice(num_rows - 3, 2))?;

            let remainder = num_rows - 4;
            write!(
                f,
                "Record Batch: {}\n{prefix}... {remainder} extra rows...\n{suffix}",
                self.label
            )
        }
    }
}

fn batch_to_json(batch: RecordBatch) -> Result<String, std::fmt::Error> {
    let mut json_string = Vec::new();
    let mut writer = arrow::json::LineDelimitedWriter::new(&mut json_string);

    writer.write_batches(&[&batch]).map_err(|e| {
        tracing::error!("Error formatting batch: {}", e);
        std::fmt::Error
    })?;
    writer.finish().map_err(|e| {
        tracing::error!("Error formatting batch: {}", e);
        std::fmt::Error
    })?;
    let json_string = String::from_utf8(json_string).map_err(|e| {
        tracing::error!("Error formatting batch: {}", e);
        std::fmt::Error
    })?;

    Ok(json_string)
}

#[derive(Debug)]

pub struct SchemaAttachment {
    label: &'static str,
    schema: SchemaRef,
}

impl SchemaAttachment {
    pub fn new(label: &'static str, schema: &SchemaRef) -> Self {
        Self {
            label,
            schema: schema.clone(),
        }
    }
}

impl std::fmt::Display for SchemaAttachment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Schema: {}\n", self.label)?;

        for (k, v) in self.schema.metadata() {
            writeln!(f, "  Metadata: '{k}' = '{v}'")?;
        }

        for field in self.schema.fields.iter() {
            let nullable = if field.is_nullable() {
                ""
            } else {
                "(non-nullable) "
            };
            write!(
                f,
                "  Field: '{}' {nullable}= {}",
                field.name(),
                field.data_type(),
            )?;
            if field.metadata().is_empty() {
                writeln!(f)?;
            } else {
                writeln!(f, " {:?}", field.metadata())?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use arrow::array::{Array, ArrayRef, Int32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_batch_format() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2]));
        let b: ArrayRef = Arc::new(UInt64Array::from(vec![None, Some(1), Some(8)]));

        let schema = Schema::new(vec![
            Field::new("a", a.data_type().clone(), false),
            Field::new("b", b.data_type().clone(), true),
        ]);
        let schema = Arc::new(schema);
        let batch = RecordBatch::try_new(schema, vec![a, b]).unwrap();

        let annotation = RecordBatchAttachment::new("test batch", &batch);
        insta::assert_display_snapshot!(annotation)
    }

    #[test]
    fn test_long_batch_format() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9]));
        let b: ArrayRef = Arc::new(UInt64Array::from(vec![
            None,
            Some(1),
            Some(8),
            None,
            None,
            None,
            Some(2),
            Some(8),
            Some(9),
            Some(8),
        ]));

        let schema = Schema::new(vec![
            Field::new("a", a.data_type().clone(), false),
            Field::new("b", b.data_type().clone(), true),
        ]);
        let schema = Arc::new(schema);
        let batch = RecordBatch::try_new(schema, vec![a, b]).unwrap();

        let annotation = RecordBatchAttachment::new("test batch", &batch);
        insta::assert_display_snapshot!(annotation)
    }

    #[test]
    fn test_schema_format() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, true),
        ]);
        let schema = Arc::new(schema);

        let annotation = SchemaAttachment::new("test schema", &schema);
        insta::assert_display_snapshot!(annotation)
    }
}
