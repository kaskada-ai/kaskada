use arrow::array::{ArrayRef, TimestampNanosecondArray, UInt64Array};
use itertools::Itertools;
use sparrow_arrow::downcast::downcast_primitive_array;
use sparrow_core::KeyTriple;
use sparrow_instructions::ComputeStore;

use crate::execute::operation::expression_executor::InputColumn;
use crate::execute::operation::InputBatch;
use crate::key_hash_index::KeyHashIndex;
use crate::Batch;

/// Helper for simple operations that consume one input stream.
#[derive(Debug)]
pub(super) struct SingleConsumerHelper {
    pub incoming_columns: Vec<usize>,
    key_index_state: KeyHashIndex,
}

impl SingleConsumerHelper {
    pub fn try_new(input_operation: u32, input_columns: &[InputColumn]) -> anyhow::Result<Self> {
        let input_columns = input_columns
            .iter()
            .map(|input_column| {
                anyhow::ensure!(input_column.input_ref.producing_operation == input_operation);

                let incoming_column_index = input_column.input_ref.input_column as usize;
                Ok(incoming_column_index)
            })
            .try_collect()?;

        Ok(Self {
            incoming_columns: input_columns,
            key_index_state: KeyHashIndex::default(),
        })
    }

    pub(super) fn restore_from(
        &mut self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()> {
        self.key_index_state
            .restore_from(operation_index, compute_store)
    }

    pub(super) fn store_to(
        &self,
        operation_index: u8,
        compute_store: &ComputeStore,
    ) -> anyhow::Result<()> {
        self.key_index_state
            .store_to(operation_index, compute_store)
    }

    /// Create an input batch by applying some transform to every needed column.
    pub fn new_input_batch(
        &mut self,
        incoming: Batch,
        transform: impl Fn(&ArrayRef) -> anyhow::Result<ArrayRef>,
    ) -> anyhow::Result<InputBatch> {
        debug_assert_eq!(incoming.schema().field(0).name(), "_time");
        debug_assert_eq!(incoming.schema().field(1).name(), "_subsort");
        debug_assert_eq!(incoming.schema().field(2).name(), "_key_hash");

        let time = transform(incoming.column(0))?;
        let subsort = transform(incoming.column(1))?;
        let key_hash = transform(incoming.column(2))?;

        let input_columns = self
            .incoming_columns
            .iter()
            .map(|index| match *index {
                0 => Ok(time.clone()),
                1 => Ok(subsort.clone()),
                2 => Ok(key_hash.clone()),
                index => transform(incoming.column(index)),
            })
            .try_collect()?;

        let grouping = self
            .key_index_state
            .get_or_update_indices(downcast_primitive_array(key_hash.as_ref())?)?;

        Ok(InputBatch {
            time,
            subsort,
            key_hash,
            grouping,
            input_columns,
            lower_bound: incoming.lower_bound,
            upper_bound: incoming.upper_bound,
        })
    }

    /// Create an input batch by applying a transform to the input columns.
    ///
    /// Use this when we need to create specific `time`, `subsort` and/or
    /// `key_hash` columns rather than just applying the transform to them.
    ///
    /// This method determines new lower/upper bounds based on the new key
    /// columns prior to the transformation. Thus, it requires the batch
    /// be non-empty.
    pub fn new_input_batch_with_keys(
        &mut self,
        incoming: &Batch,
        time: ArrayRef,
        subsort: ArrayRef,
        key_hash: ArrayRef,
        transform: impl Fn(&ArrayRef) -> anyhow::Result<ArrayRef>,
    ) -> anyhow::Result<Option<InputBatch>> {
        if time.len() == 0 {
            // TODO: Rather than computing the bounds below, we should instead have
            // them passed in, since the calling code can properly compute them. At
            // that point, we should be able to always rneturn an `InputBatch`, since
            // we won't need at least one row from which to compute bounds.
            return Ok(None);
        }

        debug_assert_eq!(incoming.schema().field(0).name(), "_time");
        debug_assert_eq!(incoming.schema().field(1).name(), "_subsort");
        debug_assert_eq!(incoming.schema().field(2).name(), "_key_hash");
        debug_assert_eq!(time.len(), subsort.len());
        debug_assert_eq!(time.len(), key_hash.len());

        // Determine the new lower / upper bounds.
        //
        // TODO: Rather than computing the bounds in this method, we should
        // have the expected bounds passed in and verify them against the
        // information.
        let (lower_bound, upper_bound) = bounds(&time, &subsort, &key_hash)?;

        // PERFORMANCE: There may be cases where one or more of the transformed
        // `time`, `subsort` and `key_hash` columns are already computed. In
        // this case, it would be more efficient (CPU and memory) to just clone
        // the reference to those rather than re-transforming them. The difficulty
        // is that we don't know whether they are simply the transformation of
        // one of the inputs, or if they have been operated on differently.
        //
        // Eg., if this is a `select`, then we know that the transformed input time
        // is the same as the `time` column. But if this is a `with_key`, then the
        // transformed input `key_hash` is different from the `key_hash` column.
        let input_columns = self
            .incoming_columns
            .iter()
            .map(|index| transform(incoming.column(*index)))
            .try_collect()?;

        let grouping = self
            .key_index_state
            .get_or_update_indices(downcast_primitive_array(key_hash.as_ref())?)?;

        Ok(Some(InputBatch {
            time,
            subsort,
            key_hash,
            grouping,
            input_columns,
            lower_bound,
            upper_bound,
        }))
    }
}

fn bounds(
    time: &ArrayRef,
    subsort: &ArrayRef,
    key_hash: &ArrayRef,
) -> anyhow::Result<(KeyTriple, KeyTriple)> {
    anyhow::ensure!(time.len() > 0);

    let time: &TimestampNanosecondArray = downcast_primitive_array(time.as_ref())?;
    let subsort: &UInt64Array = downcast_primitive_array(subsort.as_ref())?;
    let key_hash: &UInt64Array = downcast_primitive_array(key_hash.as_ref())?;

    let last = time.len() - 1;

    let lower_bound = KeyTriple::new(time.value(0), subsort.value(0), key_hash.value(0));
    let upper_bound = KeyTriple::new(time.value(last), subsort.value(last), key_hash.value(last));
    Ok((lower_bound, upper_bound))
}
