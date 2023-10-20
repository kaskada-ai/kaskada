use sparrow_batch::{Batch, RowTime};
use std::collections::BinaryHeap;

/// The result of a gather operation
#[derive(Debug, PartialEq)]
pub struct GatheredBatches {
    /// For each participant in the gather, a sequence of gathered input batches.
    ///
    /// This may be multiple batches for each input stream -- for instance, the
    /// slice of a previous batch that wasn't emitted or a few batches that were
    /// collected while waiting for a different input stream to receive a batch
    /// and "unblock" gathering up to a certain tiime.
    ///
    /// We don't concatenate the input batches because that would lead to
    /// allocating and copying data into the concatenated chunk. Instead, we
    /// leave the batches separate (here) and concatenate later.
    pub batches: Vec<Vec<Batch>>,

    /// The time up to which the batches are "complete".
    ///
    /// Any rows in future gathered batches must be strictly greater than this.
    pub up_to_time: RowTime,
}

/// Treat the smallest priority as the greatest value for use with max heap.
struct MinPriority<T> {
    up_to_time: RowTime,
    index: usize,
    rest: T,
}

impl<T> PartialEq for MinPriority<T> {
    fn eq(&self, other: &Self) -> bool {
        self.up_to_time == other.up_to_time && self.index == other.index
    }
}

impl<T> Eq for MinPriority<T> {}

impl<T> PartialOrd for MinPriority<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T> Ord for MinPriority<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.up_to_time
            .cmp(&other.up_to_time)
            .reverse()
            .then_with(|| self.index.cmp(&other.index))
    }
}

struct PendingBatches {
    /// For each index, holds the pending batches.
    ///
    /// The batches should be non-decreasing by `up_to_time`.
    pending: Vec<Vec<Batch>>,
}

impl PendingBatches {
    fn with_capacity(len: usize) -> Self {
        Self {
            pending: vec![vec![]; len],
        }
    }

    /// Add a batch to the pending set for the given index.
    ///
    /// Returns `true` if the batch caused the index to become non-empty.
    fn add_batch(&mut self, index: usize, batch: Batch) -> bool {
        if !batch.is_empty() {
            let result = self.pending[index].is_empty();
            self.pending[index].push(batch);
            result
        } else {
            false
        }
    }

    /// Extract the batches that are ready to be emitted, leaving the remainders.
    ///
    /// This should be called when it is determined that it is safe to emit elements
    /// up to and including `time_inclusive`. This should generally be called with
    /// the time of the minimum element in the queue.
    fn split_up_to(&mut self, time_inclusive: RowTime) -> GatheredBatches {
        let batches = self
            .pending
            .iter_mut()
            .map(|pending_batches| split_up_to(pending_batches, time_inclusive))
            .collect();

        GatheredBatches {
            batches,
            up_to_time: time_inclusive,
        }
    }

    fn finish(self) -> GatheredBatches {
        GatheredBatches {
            batches: self.pending,
            up_to_time: RowTime::MAX,
        }
    }
}

/// Split the vector of pending batches into the output and remainder.
///
/// Returns the batches containing all rows up to (and including) `time_inclusive`.
///
/// Modifies `pending_batches` to contain the remainder -- those batches contain
/// rows after `time_inclusive`.
///
/// Order of the pending batches is preserved.
fn split_up_to(pending_batches: &mut Vec<Batch>, time_inclusive: RowTime) -> Vec<Batch> {
    // Batches 0..complete_batches can be used completely.
    let complete_batches =
        pending_batches.partition_point(|batch| batch.up_to_time <= time_inclusive);

    // After this, `pending_batches` contains the elements that can be output
    // in their entirety. The first batch in remainder (if non-empty) may contain
    // rows less than the `up_to_time` which need to be split out and added back
    // to the `pending_batches` to produce the complete output set.
    let mut remainder = pending_batches.split_off(complete_batches);

    match remainder.first().and_then(|batch| batch.min_present_time()) {
        Some(min_time) if min_time > time_inclusive => {
            // The first row of the first batch in the remainder is *after*
            // up_to_time, so we don't need to split anything.
        }
        None => {
            // Either there is no remainder or the first batch in the remainder is empty.
            // If there is no remainder, we can just return that.
            //
            // If there was a remainder and the first batch is empty we *don't* need
            // to consider later batches in the remainder. Consider the case where
            //  we have two batches in the remainder such that:
            // `up_to_time < batch1.up_to_time <= batch_2.up_to_time`.
            //
            // 1. Note we don't need to consider `up_to_time = batch1.up_to_time` since that
            //    would cause us to have included `batch1` in the complete set.
            // 2. Note we don't need to consider cases where the up to times are decreasing
            //    since we require that batches are produced with non-decreasing `up_to_time`.
            //
            // In such a case, we would need to split `batch2` if it contained a row less
            // than or equal to `up_to_time`. But in that case, it would be incorrect for
            // `batch1` to have indicated that it contained all rows up to and including
            // `up_to_time`.
            //
            // Thus, if the the first batch in the remainder is empty we also can just
            // return the remainder.
        }
        Some(_) => {
            // If we reach this point we know the data in the first remainder is non-empty
            // *and* it contains data at or before `up_to_time` which needs to be included
            // in the pending batches.
            if let Some(complete) = remainder[0].split_up_to(time_inclusive) {
                pending_batches.push(complete);
            }
        }
    }

    std::mem::replace(pending_batches, remainder)
}

#[cfg(test)]
mod tests {
    use arrow_select::concat::concat_batches;

    use super::GatheredBatches;
    use proptest::prelude::*;

    // async fn run_gather(items: Vec<Vec<Batch>>) -> Vec<GatheredBatches> {
    //     super::gather(
    //         items
    //             .into_iter()
    //             .map(|batches| {
    //                 futures::stream::iter(batches)
    //                     .map(|b| -> Result<_, ()> { Ok(b) })
    //                     .boxed()
    //             })
    //             .collect(),
    //     )
    //     .try_collect()
    //     .await
    //     .unwrap()
    // }

    // fn blocking_gather(items: Vec<Vec<Batch>>) -> Vec<GatheredBatches> {
    //     let rt = runtime::Builder::new_current_thread()
    //         .enable_all()
    //         .build()
    //         .unwrap();

    //     rt.block_on(run_gather(items))
    // }

    // Prop test -- arbitrary batch on each side.
    // Generate a single batch on each side. The result should be a single
    // gathered batch containing the batch from each side.

    // proptest::proptest! {
    //     #[test]
    //     fn test_two_one_batch_streams(a in arb_batch(2..100), b in arb_batch(2..100)) {
    //         let results = blocking_gather(vec![vec![a.clone()], vec![b.clone()]]);

    //         // Rebuild the batch on each side and make sure we get the input back.
    //         let a_batches: Vec<_> = results.iter().flat_map(|gathered| &gathered.batches[0]).flat_map(|batch| batch.record_batch().cloned()).collect();
    //         let a_batches = concat_batches(&Batch::minimal_schema(), &a_batches).unwrap();
    //         prop_assert_eq!(&a_batches, a.record_batch().unwrap());

    //         let b_batches: Vec<_> = results.iter().flat_map(|gathered| &gathered.batches[1]).flat_map(|batch| batch.record_batch().cloned()).collect();
    //         let b_batches = concat_batches(&Batch::minimal_schema(), &b_batches).unwrap();
    //         prop_assert_eq!(&b_batches, b.record_batch().unwrap());

    //         // Iterate over the batches, verifying the up_to_time properties are met.
    //         let mut up_to = -1;
    //         for gathered in results.iter() {
    //             prop_assert_eq!(gathered.batches.len(), 2);

    //             // Checks.
    //             // 1. Must only produce rows greater than the previous batches "up to".
    //             for batch in gathered.batches.iter().flatten() {
    //                 if let Some(times) = batch.time() {
    //                     for time in times.values() {
    //                         prop_assert!(*time > up_to);
    //                     }
    //                 }
    //             }

    //             // Update things.
    //             up_to = i64::from(gathered.up_to_time);
    //         }
    //     }
    // }
}
