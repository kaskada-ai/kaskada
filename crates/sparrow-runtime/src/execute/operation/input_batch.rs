use std::sync::Arc;

use anyhow::Context;
use arrow::array::ArrayRef;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use sparrow_core::{KeyTriple, KeyTriples};
use sparrow_instructions::GroupingIndices;

use sparrow_merge::old::BinaryMergeInput;

/// The input information an operation is running on.
///
/// The key columns (`time`, `subsort` and `key_hash`) correspond to the
/// result domain of the operation.
///
/// The `input_columns` are the columns consumed by the operation mapped
/// to the same domain.
/// The bounds correspond to the overall bounds of the batch that the operation
/// created this input batch from. Note the columns may not necessarily
/// correspond to the bounds in cases where the batch was filtered, spread, or
/// otherwise changed.
#[derive(Debug)]
#[non_exhaustive]
pub(super) struct InputBatch {
    pub time: ArrayRef,
    pub subsort: ArrayRef,
    pub key_hash: ArrayRef,
    pub grouping: GroupingIndices,
    pub input_columns: Vec<ArrayRef>,
    pub lower_bound: KeyTriple,
    pub upper_bound: KeyTriple,
}

impl InputBatch {
    /// Creates a new empty input batch with a single additional `tick` column,
    /// using the given bounds.
    pub fn new_empty(schema: SchemaRef, lower_bound: KeyTriple, upper_bound: KeyTriple) -> Self {
        let columns: Vec<ArrayRef> = schema
            .fields
            .iter()
            .map(|f| arrow::array::new_empty_array(f.data_type()))
            .collect();

        InputBatch {
            time: columns[0].clone(),
            subsort: columns[1].clone(),
            key_hash: columns[2].clone(),
            grouping: GroupingIndices::new_empty(),
            input_columns: columns[3..].to_vec(),
            lower_bound,
            upper_bound,
        }
    }

    #[cfg(debug_assertions)]
    pub fn validate_bounds(&self) -> anyhow::Result<()> {
        crate::validate_bounds(
            &self.time,
            &self.subsort,
            &self.key_hash,
            &self.lower_bound,
            &self.upper_bound,
        )?;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.time.len()
    }

    pub fn as_merge_input(&self) -> anyhow::Result<BinaryMergeInput<'_>> {
        BinaryMergeInput::from_array_refs(&self.time, &self.subsort, &self.key_hash)
    }

    /// Splits input batch into a prefix of size `length`, with the suffix the
    /// remainder.
    ///
    /// Updates the bounds accordingly.
    pub fn split(self, length: usize) -> anyhow::Result<(Self, Self)> {
        anyhow::ensure!(
            length <= self.time.len(),
            "Split length must be greater than 0"
        );
        let prefix = self.slice_lower(0, length)?;
        let suffix = self.slice_upper(length, self.time.len() - length)?;
        Ok((prefix, suffix))
    }

    fn slice_lower(&self, offset: usize, length: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(offset + length <= self.time.len());
        let time = self.time.slice(offset, length);
        let subsort = self.subsort.slice(offset, length);
        let key_hash = self.key_hash.slice(offset, length);
        let upper_bound = if time.len() == 0 {
            self.lower_bound
        } else {
            KeyTriples::try_new(time.clone(), subsort.clone(), key_hash.clone())?
                .last()
                .context("last key triple")?
        };
        Ok(Self {
            time,
            subsort,
            key_hash,
            grouping: self.grouping.slice(offset, length)?,
            input_columns: self
                .input_columns
                .iter()
                .map(|column| column.slice(offset, length))
                .collect(),
            lower_bound: self.lower_bound,
            upper_bound,
        })
    }

    fn slice_upper(&self, offset: usize, length: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(offset + length <= self.time.len());
        let time = self.time.slice(offset, length);
        let subsort = self.subsort.slice(offset, length);
        let key_hash = self.key_hash.slice(offset, length);
        let lower_bound = if time.len() == 0 {
            self.upper_bound
        } else {
            KeyTriples::try_new(time.clone(), subsort.clone(), key_hash.clone())?
                .first()
                .context("first key triple")?
        };
        Ok(Self {
            time,
            subsort,
            key_hash,
            grouping: self.grouping.slice(offset, length)?,
            input_columns: self
                .input_columns
                .iter()
                .map(|column| column.slice(offset, length))
                .collect(),
            lower_bound,
            upper_bound: self.upper_bound,
        })
    }

    #[allow(dead_code)]
    pub fn as_json(&self) -> AsJson<'_> {
        AsJson(self)
    }
}

#[repr(transparent)]
pub struct AsJson<'a>(&'a InputBatch);

impl<'a> std::fmt::Display for AsJson<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut json_string = Vec::new();
        let mut writer = arrow::json::LineDelimitedWriter::new(&mut json_string);

        // TODO: We should probably just have a schema for the input batch
        // and/or use a `RecordBatch` for storing the fields.
        let mut fields = vec![
            arrow::datatypes::Field::new("time", self.0.time.data_type().clone(), false),
            arrow::datatypes::Field::new("subsort", self.0.subsort.data_type().clone(), false),
            arrow::datatypes::Field::new("key_hash", self.0.key_hash.data_type().clone(), false),
        ];
        for (index, column) in self.0.input_columns.iter().enumerate() {
            fields.push(arrow::datatypes::Field::new(
                format!("i{index}"),
                column.data_type().clone(),
                true,
            ));
        }
        let schema = arrow::datatypes::Schema::new(fields);
        let schema = Arc::new(schema);
        let columns: Vec<_> = [
            self.0.time.clone(),
            self.0.subsort.clone(),
            self.0.key_hash.clone(),
        ]
        .into_iter()
        .chain(self.0.input_columns.iter().cloned())
        .collect();
        let batch = RecordBatch::try_new(schema, columns).map_err(|e| {
            tracing::error!("Error formatting batch: {}", e);
            std::fmt::Error
        })?;

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

        write!(f, "{json_string}")
    }
}
