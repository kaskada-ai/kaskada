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
