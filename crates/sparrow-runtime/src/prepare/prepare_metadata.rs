use std::sync::Arc;

use anyhow::Context;
use arrow::{
    array::Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use error_stack::{IntoReport, ResultExt};
use hashbrown::HashSet;

use super::Error;

/// Stores metadata about each batch that is prepared.
pub struct PrepareMetadata {
    pub metadata_schema: SchemaRef,
    // The set of previous keys that have been seen.  This is used to
    // construct the entity key hash -> entity key mapping.
    pub previous_keys: HashSet<u64>,
    pub metadata_batches: Vec<RecordBatch>,
}

impl PrepareMetadata {
    pub fn new(entity_key_type: DataType) -> PrepareMetadata {
        let metadata_schema: SchemaRef = {
            Arc::new(Schema::new(vec![
                Field::new("_hash", DataType::UInt64, false),
                Field::new("_entity_key", entity_key_type, true),
            ]))
        };
        PrepareMetadata {
            metadata_schema,
            previous_keys: HashSet::new(),
            metadata_batches: vec![],
        }
    }

    pub fn add_entity_keys(
        &mut self,
        hashes: Arc<dyn Array>,
        entity_keys: Arc<dyn Array>,
    ) -> anyhow::Result<()> {
        let new_batch =
            RecordBatch::try_new(self.metadata_schema.clone(), vec![hashes, entity_keys])
                .with_context(|| "unable to add entity keys to metadata")?;
        self.metadata_batches.push(new_batch);
        Ok(())
    }

    pub fn get_flush_metadata(&mut self) -> error_stack::Result<RecordBatch, Error> {
        let metadata =
            arrow::compute::concat_batches(&self.metadata_schema, &self.metadata_batches)
                .into_report()
                .change_context(Error::Internal);
        self.previous_keys.clear();
        self.metadata_batches.clear();
        metadata
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prepare::ColumnBehavior;
    use arrow::array::{StringArray, UInt64Array};

    #[tokio::test]
    async fn test_entity_key_mapping() {
        let mut behavior = ColumnBehavior::PrepareEntityKey {
            index: 0,
            nullable: false,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("names", DataType::Utf8, true)]));
        let data = StringArray::from(vec!["awkward", "tacos", "awkward", "tacos", "apples"]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(data)]).unwrap();
        let mut metadata = PrepareMetadata::new(DataType::Utf8.clone());
        behavior
            .get_result(Some(&mut metadata), None, &batch)
            .await
            .unwrap();
        assert_eq!(metadata.get_flush_metadata().unwrap().num_rows(), 3);
        assert!(metadata.previous_keys.is_empty());
        assert!(metadata.metadata_batches.is_empty());
    }
    #[tokio::test]
    async fn test_entity_key_mapping_int() {
        let mut behavior = ColumnBehavior::PrepareEntityKey {
            index: 0,
            nullable: false,
        };
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::UInt64, true)]));
        let data = UInt64Array::from(vec![1, 2, 3, 1, 2, 3]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(data)]).unwrap();
        let mut metadata = PrepareMetadata::new(DataType::UInt64.clone());
        behavior
            .get_result(Some(&mut metadata), None, &batch)
            .await
            .unwrap();
        assert_eq!(metadata.get_flush_metadata().unwrap().num_rows(), 3);
        assert!(metadata.previous_keys.is_empty());
        assert!(metadata.metadata_batches.is_empty());
    }
}
