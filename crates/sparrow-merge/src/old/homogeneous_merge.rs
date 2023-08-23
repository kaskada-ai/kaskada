use std::collections::BinaryHeap;
use std::sync::Arc;

use anyhow::Context;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use itertools::izip;
use sparrow_arrow::downcast::downcast_primitive_array;

use crate::old::binary_merge::BinaryMergeResult;
use crate::old::{binary_merge, BinaryMergeInput};

/// Merges 0 or more batches with the same schema into a single result.
///
/// This applies an iterative 2-way merge strategy, concatenating the
/// batches in increasing order of size.
pub fn homogeneous_merge(
    schema: &SchemaRef,
    batches: impl IntoIterator<Item = RecordBatch>,
) -> anyhow::Result<RecordBatch> {
    // Create a queue with all the batches. We use a for-loop to make it easier
    // to check the schema of each batch before adding it, if non-empty to a vector.
    let batches = batches.into_iter();
    let size_hint = batches.size_hint();
    let mut to_merge = Vec::with_capacity(size_hint.1.unwrap_or(size_hint.0));
    for batch in batches {
        // Check to make sure all of the batches have the same schema.
        debug_assert_eq!(&batch.schema(), schema);

        // Only collect non-empty batches.
        if batch.num_rows() > 0 {
            to_merge.push(PendingMerge(batch))
        }
    }
    let mut to_merge: BinaryHeap<_> = BinaryHeap::from(to_merge);
    // Do the actual merge -- if there are no non-empty batches we're done.
    if to_merge.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    // Otherwise, we loop and merge the two smallest batches.
    let result = loop {
        let first = to_merge.pop().context("Unexpected empty merge queue")?;

        if let Some(second) = to_merge.pop() {
            to_merge.push(do_merge(schema, first, second)?);
        } else {
            // And we break if there isn't a second batch in the heap.
            break first;
        }
    };

    Ok(result.0)
}

fn do_merge(schema: &SchemaRef, a: PendingMerge, b: PendingMerge) -> anyhow::Result<PendingMerge> {
    let BinaryMergeResult {
        time,
        subsort,
        key_hash,
        take_a,
        take_b,
    } = binary_merge(a.as_merge_input()?, b.as_merge_input()?)?;

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());
    columns.push(Arc::new(time));
    columns.push(Arc::new(subsort));
    columns.push(Arc::new(key_hash));

    for (a, b) in izip!(&a.0.columns()[3..], &b.0.columns()[3..]) {
        // TODO: We could create a kernel that takes two inputs and two exclusive
        // take vectors and use that instead of this approach which first applies
        // the take vectors and then applies cond. Even if it doesn't do the whole
        // take, it could be a simple version of `zip + is_not_null` that assumes
        // the inputs are disjoint.
        //
        // We could go further -- we could build up a vector of which input each
        // row should be taken from, and then use that. This would allow us to
        // defer the intermediate merges (and thus intermediate allocations).
        let a = arrow_select::take::take(a.as_ref(), &take_a, None)?;
        let b = arrow_select::take::take(b.as_ref(), &take_b, None)?;

        // TODO: As implemented, this will prefer items from `a`. Since we merge ordered
        // by size, this is potentially non-deterministic if two files have
        // duplicate rows and the same length. We should figure out if we should
        // (a) fail (and do so) or (b) allow indicating which side to prefer.
        // The simplest would be for one file to be "newer".
        let a_is_valid = arrow_arith::boolean::is_not_null(a.as_ref())?;
        let merged = arrow_select::zip::zip(&a_is_valid, a.as_ref(), b.as_ref())?;
        columns.push(merged);
    }

    Ok(PendingMerge(RecordBatch::try_new(schema.clone(), columns)?))
}

#[repr(transparent)]
struct PendingMerge(RecordBatch);

impl PendingMerge {
    // Opportunity to rename/refactor Batch to TemporalBatch to emphasize
    // the presence of the additional `_time`, `_subsort`, and `_key_hash` columns.
    fn as_merge_input(&self) -> anyhow::Result<BinaryMergeInput<'_>> {
        Ok(BinaryMergeInput::new(
            downcast_primitive_array(self.0.column(0).as_ref())?,
            downcast_primitive_array(self.0.column(1).as_ref())?,
            downcast_primitive_array(self.0.column(2).as_ref())?,
        ))
    }
}

impl std::cmp::PartialEq for PendingMerge {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl std::cmp::Eq for PendingMerge {}

impl std::cmp::PartialOrd for PendingMerge {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl std::cmp::Ord for PendingMerge {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.num_rows().cmp(&other.0.num_rows())
    }
}

#[cfg(test)]
mod tests {

    use arrow_array::RecordBatch;
    use arrow_array::UInt8Array;
    use proptest::prelude::*;
    use sparrow_core::TableSchema;

    use super::*;
    use crate::old::testing::arb_key_triples;

    proptest! {
        #[test]
        fn test_merge_1_input((split, merged) in arb_split_and_merged(1000, 1)) {
            check_split_merge(1, split, merged)?;
        }

        #[test]
        fn test_merge_2_input((split, merged) in arb_split_and_merged(1000, 2)) {
            check_split_merge(2, split, merged)?;
        }

        #[test]
        fn test_merge_3_input((split, merged) in arb_split_and_merged(1000, 3)) {
            check_split_merge(3, split, merged)?;
        }

        #[test]
        fn test_merge_4_input((split, merged) in arb_split_and_merged(1000, 4)) {
            check_split_merge(4, split, merged)?;
        }


        #[test]
        fn test_merge_10_input((split, merged) in arb_split_and_merged(1000, 10)) {
            check_split_merge(10, split, merged)?;
        }
    }

    fn arb_split_and_merged(
        len: impl Into<prop::collection::SizeRange>,
        inputs: u8,
    ) -> impl Strategy<Value = (Vec<u8>, RecordBatch)> {
        arb_key_triples(len)
            .prop_map(|(time, subsort, key_hash)| {
                RecordBatch::try_new(
                    TableSchema::default_schema().schema_ref().clone(),
                    vec![Arc::new(time), Arc::new(subsort), Arc::new(key_hash)],
                )
                .unwrap()
            })
            .prop_flat_map(move |merged| {
                (
                    prop::collection::vec(0..inputs, merged.num_rows()),
                    Just(merged),
                )
            })
    }

    fn check_split_merge(
        inputs: u8,
        split: Vec<u8>,
        merged: RecordBatch,
    ) -> Result<(), TestCaseError> {
        // Create the split array by filtering the record batch for each split index.
        let split_array = UInt8Array::from(split);
        let inputs: Vec<_> = (0..inputs)
            .map(|n| {
                let filter = arrow_ord::comparison::eq_scalar(&split_array, n).unwrap();
                arrow_select::filter::filter_record_batch(&merged, &filter).unwrap()
            })
            .collect();

        let actual = homogeneous_merge(&merged.schema(), inputs).unwrap();

        prop_assert_eq!(merged, actual);

        Ok(())
    }
}
