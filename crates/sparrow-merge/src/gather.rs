mod gathered_batches;

use crate::gather::gathered_batches::GatheredBatches;
use sparrow_batch::{Batch, RowTime};
use std::collections::BinaryHeap;

pub struct Gatherer {
    /// Information about the input with the *minimum* `up_to_time`.
    ///
    // TODO: Use a "tournament heap" for improved performance for our needs.
    active: BinaryHeap<MinPriority>,
    /// Batches which are pending for each input index.
    pending: Vec<Pending>,
    emitted_up_to_time: RowTime,
}

impl Gatherer {
    pub fn new(inputs: usize) -> Self {
        let active: BinaryHeap<_> = (0..inputs)
            .into_iter()
            .map(|index| MinPriority {
                up_to_time: RowTime::ZERO,
                index,
            })
            .collect();

        let pending = vec![Pending::default(); active.len()];
        Self {
            active,
            pending,
            emitted_up_to_time: RowTime::ZERO,
        }
    }

    /// Used to peek at the first *true* active item.
    // fn peek_active(&mut self) -> Option<&MinPriority> {
    //     while let Some((active_index, active_up_to_time)) = self
    //         .active
    //         .peek()
    //         .map(|active| (active.index, active.up_to_time))
    //     {
    //         // If the input is closed, it's no longer blocking anything.
    //         if self.pending[active_index].closed {
    //             self.active.pop();
    //             continue;
    //         } else if self.pending[active_index].up_to_time > active_up_to_time {
    //             // SAFETY: We peeked an active item to enter this loop.
    //             let mut active = unsafe { self.active.pop().unwrap_unchecked() };
    //             active.up_to_time = self.pending[active_index].up_to_time;
    //             self.active.push(active);
    //             continue;
    //         } else {
    //             return self.active.peek();
    //         };
    //     }

    //     None
    // }

    /// Return the index of the input we need in order to advance.
    ///
    /// If all inputs have been closed, returns `None`.
    pub fn blocking_input(&self) -> Option<usize> {
        self.active.peek().map(|min| min.index)
    }

    /// Adds a batch to the pending set for the given index.
    ///
    /// Returns `true` if the gatherer is ready to produce output.
    pub fn add_batch(&mut self, index: usize, batch: Batch) -> bool {
        println!("ADD BATCH---------");
        println!("Total number of pending inputs: {:?}", self.pending.len());
        println!("Adding batch for index: {}", index);
        let batch_up_to_time = batch.up_to_time;
        self.pending[index].batches.push(batch);
        println!(
            "Current pending index batches length: {:?}",
            self.pending[index].batches.len()
        );

        if let Some(mut entry) = self.active.pop() {
            // print popped active:
            println!(
                "Setting entry up to_time: {} with batch up to_time: {}",
                entry.up_to_time, batch_up_to_time
            );
            entry.up_to_time = batch_up_to_time;
            self.active.push(entry);
            // SAFETY: We just pushed an element so we know it's not empty.
            let top = unsafe { self.active.peek().unwrap_unchecked() };
            println!("Top up to time: {}", top.up_to_time);
            println!("Emitted up to time: {}", self.emitted_up_to_time);
            // We can output if minimum pending index is larger than previous emitted up to.
            top.up_to_time > self.emitted_up_to_time
        } else {
            // We could make the logic more complex to handle this case.
            // Specifically, the information for an index stored in pending
            // could be updated, and we could check the front of the queue
            // on each peek/pop to see if the information we get is fresh.
            //
            // For now, we choose not to do that and instead require that
            // we only operate on the "blocking" element.
            panic!("should only add batch for the current blocking input");
        }
    }

    pub fn close(&mut self, index: usize) -> bool {
        println!("ClOSING INDEX: {}", index);
        assert!(!self.pending[index].closed);
        self.pending[index].closed = true;
        let active = self.active.pop().expect("non empty");
        assert_eq!(active.index, index);

        self.active.is_empty() || self.active.peek().unwrap().up_to_time > self.emitted_up_to_time
    }

    /// If a batch is ready to be processed, returns it.
    pub fn next_batch(&mut self) -> Option<GatheredBatches> {
        println!("NEXT BATCH ---------------");
        match self.active.peek() {
            None => {
                println!("No active peek");
                if self
                    .pending
                    .iter()
                    .all(|p| p.batches.iter().all(|b| b.is_empty()))
                {
                    println!("All empty pending returning None");
                    return None;
                }

                tracing::info!("All inputs are closed. Gathering final batches.");
                let batches: Vec<Vec<_>> = self
                    .pending
                    .drain(..)
                    .map(|pending| {
                        assert!(pending.closed);
                        pending.batches
                    })
                    .collect();
                println!("All inputs closed, gathering {} batches", batches.len());

                Some(GatheredBatches {
                    batches,
                    up_to_time: RowTime::MAX, // TODO: We may have to do this for real, and find the max up to time.
                                              // Otherwise I worry a downstream window function will think it needs to produce a window up to max row time?
                })
            }
            Some(top) if top.up_to_time > self.emitted_up_to_time => {
                println!("Emitting something here");
                println!("Top up to time: {}", top.up_to_time);
                println!("Self emitted up to time: {}", self.emitted_up_to_time);
                self.emitted_up_to_time = top.up_to_time;
                let batches = self
                    .pending
                    .iter_mut()
                    .map(|pending| pending.split_up_to(top.up_to_time))
                    .collect();
                println!("Batches after split: {:?}", batches);
                Some(GatheredBatches {
                    batches,
                    up_to_time: top.up_to_time,
                })
            }
            Some(top) => {
                println!("Not emitting something here");
                println!("Top up to time: {:?}", top.up_to_time);
                println!("Self emitted up to time: {:?}", self.emitted_up_to_time);
                None
            }
        }
    }
}

#[derive(Default, Clone)]
struct Pending {
    /// The batches for this input index.
    ///
    /// The batches should arrive (and be stored) non-decreasing by `up_to_time`.
    batches: Vec<Batch>,
    /// The `up_to_time` of the latest batch received on this input index.
    up_to_time: RowTime,
    /// True if the given entry has reported closed.
    closed: bool,
    /// The input index
    input: usize,
}

impl Pending {
    fn new(index: usize) -> Self {
        Self {
            input: index,
            ..Default::default()
        }
    }

    fn add_batch(&mut self, batch: Batch) {
        if !batch.is_empty() {
            self.batches.push(batch)
        }
    }

    /// Split the vector of pending batches into the output and remainder.
    ///
    /// TODO: FRAZ - I think this should be exclusive
    /// Returns the batches containing all rows up to `time_exclusive`.
    ///
    /// Modifies `pending_batches` to contain the remainder -- those batches contain
    /// rows after `time_inclusive`.
    ///
    /// Order of the pending batches is preserved.
    fn split_up_to(&mut self, time_exclusive: RowTime) -> Vec<Batch> {
        println!("SPLIT UP TO time: {}", time_exclusive);
        let pending = &mut self.batches;

        // The partition point is the index of the first element in second partition.
        // In this case, it is the index of the first batch with up_to_time < time_exclusive.
        let partition_index = pending.partition_point(|batch| batch.up_to_time < time_exclusive);
        println!("Partition index: {}", partition_index);

        // After this, `pending_batches` contains the elements that can be output
        // in their entirety.
        //
        // Note that `split_off` is inclusive, so the pending set will contain
        // [0, partition_index), and the remainder will contain [partition_index, len)
        println!("Pending before split: {:?}", pending);
        let mut remainder = pending.split_off(partition_index);
        println!("Pending after split: {:?}", pending);
        println!("Remainder here: {:?}", remainder);

        match remainder.first().and_then(|batch| batch.min_present_time()) {
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
            Some(min_time) if min_time >= time_exclusive => {
                // The first row of the first batch in the remainder is *after*
                // up_to_time, so we don't need to split anything.
                //
                // Since we know the batches are non-decreasing, and a proceeding batch's
                // min_present_time must be >= the previous batch's up_to_time, we know we
                // don't need to split any other batches in the remainder.
            }
            Some(_) => {
                // If we reach this point we know the data in the first remainder is non-empty
                // *and* it contains data at or before `up_to_time` which needs to be included
                // in the pending batches.
                println!("HERE-----");
                if let Some(split_lt) = remainder[0].split_up_to(time_exclusive) {
                    println!("Split lt: {:?}", split_lt);
                    pending.push(split_lt);
                }
            }
        }

        println!("Pending set: {:?}", pending);
        println!("Remainder set: {:?}", remainder);
        std::mem::replace(pending, remainder)
    }
}

/// Treat the smallest priority as the greatest value for use with max heap.
struct MinPriority {
    up_to_time: RowTime,
    index: usize,
}

impl PartialEq for MinPriority {
    fn eq(&self, other: &Self) -> bool {
        self.up_to_time == other.up_to_time && self.index == other.index
    }
}

impl Eq for MinPriority {}

impl PartialOrd for MinPriority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MinPriority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.up_to_time
            .cmp(&other.up_to_time)
            .reverse()
            .then_with(|| self.index.cmp(&other.index))
    }
}

#[cfg(test)]
mod tests {
    use arrow_select::concat::concat_batches;

    use super::*;
    use proptest::prelude::*;
    use sparrow_batch::testing::arb_arrays::arb_batch;
    use sparrow_batch::Batch;

    fn run_gather(items: Vec<Vec<Batch>>) -> Vec<GatheredBatches> {
        let mut results = Vec::new();
        let mut gatherer = Gatherer::new(items.len());
        let mut items: Vec<_> = items
            .into_iter()
            .map(|batches| batches.into_iter())
            .collect();
        while let Some(next_index) = gatherer.blocking_input() {
            let ready = match items[next_index].next() {
                Some(batch) => {
                    // println!("FRAZ - batch: {:?}", batch);
                    let result = gatherer.add_batch(next_index, batch);
                    println!("READY? {}", result);
                    result
                }
                None => gatherer.close(next_index),
            };

            if ready {
                results.push(gatherer.next_batch().unwrap())
            }
        }
        results
    }

    // Prop test -- arbitrary batch on each side.
    // Generate a single batch on each side. The result should be a single
    // gathered batch containing the batch from each side.

    proptest::proptest! {
        #[test]
        fn test_two_one_batch_streams(a in arb_batch(2..100), b in arb_batch(2..100)) {
            let schema = a.clone().record_batch().unwrap().schema();
            let results = run_gather(vec![vec![a.clone()], vec![b.clone()]]);

            // Rebuild the batch on each side and make sure we get the input back.
            let a_batches: Vec<_> = results.iter().flat_map(|gathered| &gathered.batches[0]).map(|batch| batch.clone().record_batch().unwrap()).collect();
            let a_batches = concat_batches(&schema.clone(), &a_batches).unwrap();
            prop_assert_eq!(&a_batches, &a.record_batch().unwrap());

            let b_batches: Vec<_> = results.iter().flat_map(|gathered| &gathered.batches[1]).map(|batch| batch.clone().record_batch().unwrap()).collect();
            let b_batches = concat_batches(&schema.clone(), &b_batches).unwrap();
            prop_assert_eq!(&b_batches, &b.record_batch().unwrap());

            // Iterate over the batches, verifying the up_to_time properties are met.
            let mut up_to = -1;
            for gathered in results.iter() {
                prop_assert_eq!(gathered.batches.len(), 2);

                // Checks.
                // 1. Must only produce rows greater than the previous batches "up to".
                for batch in gathered.batches.iter().flatten() {
                    if let Some(times) = batch.time() {
                        for time in times.values() {
                            prop_assert!(*time >= up_to);
                        }
                    }
                }

                // Update things.
                up_to = i64::from(gathered.up_to_time);
            }
        }
    }
}
