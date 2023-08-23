//! Gathers batches from 2 or more input streams.
//!
//! For each stream, the [OrderedInputs] records the batches that have been
//! received as well as the maximum time associated with those batches.  Once
//! the [Gatherer] has received at least one batch from each input stream it
//! selects a point in time to slice at such that all input streams are complete
//! to the selected time. It then splits all of the collected batches at that
//! point and sends them on to be merged. The rows after the selected time act
//! as a remainder which carries over to the next iteration.

use anyhow::Context;
use bit_set::BitSet;
use itertools::Itertools;

use crate::old::input::{InputItem, OrderedInputs};

/// Gathered batches to be merged.
#[derive(Debug, PartialEq, Eq)]
pub struct GatheredBatches<T: InputItem> {
    /// For each participating stream, the sequence of gathered input batches.
    ///
    /// This may be multiple batches for each input stream -- for instance, the
    /// slice of a previous batch that wasn't emitted or a few batches that
    /// were collected while waiting for a different input stream to receive
    /// a batch and "unblock" and "complete" the input up to a certain time.
    ///
    /// We don't concatenate the input batches because that would lead to
    /// allocating additional copies of the input data.
    ///
    /// This uses a small vector to minimize indirections. We generally
    /// anticipate this being small, so 4 slots seems reasonable (although
    /// somewhat arbitrary).
    pub batches: Vec<OrderedInputs<T>>,

    /// The minimum time (possibly) included in the gathered batches.
    pub min_time_inclusive: i64,

    /// The maximum time (possibly) included in the gathered batches.
    pub max_time_inclusive: i64,
}

#[derive(Debug)]
pub struct Gatherer<T: InputItem> {
    remaining_sources: usize,
    /// The last input was emitted up to (but excluding) the given time.
    pub last_output_time: Option<i64>,
    pub max_time_gathered: i64,
    /// Items currently collected from each source.
    source_items: Vec<OrderedInputs<T>>,
    /// Which sources are marked as finished.
    finished_sources: BitSet,
}

impl<T: InputItem + std::fmt::Debug> Gatherer<T> {
    pub fn new(num_inputs: usize, last_output_time: Option<i64>) -> Self {
        Self {
            remaining_sources: num_inputs,
            last_output_time,
            // We want to start with the minimum new time. This is either
            // the last time previously output (if we're resuming), or
            // i64::MIN.
            max_time_gathered: last_output_time.unwrap_or(i64::MIN),
            source_items: (0..num_inputs).map(|_| OrderedInputs::new()).collect(),
            finished_sources: BitSet::with_capacity(num_inputs),
        }
    }

    pub fn remaining_sources(&self) -> usize {
        self.remaining_sources
    }

    pub fn skip_to(&mut self, input_index: usize, time: i64) -> anyhow::Result<()> {
        anyhow::ensure!(
            !self.finished_sources.contains(input_index),
            "Can't skip forward finished source {:?}",
            input_index
        );

        self.source_items[input_index].skip_to(time)
    }

    /// Adds an input for the given input stream.
    ///
    /// If [Self::determine_max_emit_time] indicates that after receiving the
    /// input there is a time up to which we can emit a complete batch, the
    /// resulting [GatheredBatches] are returned.
    ///
    /// If the `input` is `None` it indicates that the given input stream is
    /// "finished", and no more input from that index will be needed.
    pub fn add_batch(
        &mut self,
        input_index: usize,
        input: Option<T>,
    ) -> anyhow::Result<Option<GatheredBatches<T>>> {
        anyhow::ensure!(
            !self.finished_sources.contains(input_index),
            "Received input for finished index {:?}",
            input_index
        );

        match input {
            Some(input) => {
                self.max_time_gathered = i64::max(self.max_time_gathered, input.max_time());
                let min_time = input.min_time();
                let max_time = input.max_time();
                self.source_items[input_index]
                    .add_item(input)
                    .with_context(|| {
                        format!(
                            "Adding item with min={min_time}, max={max_time} to input \
                             {input_index}",
                        )
                    })?;
            }
            None => {
                self.remaining_sources -= 1;
                self.finished_sources.insert(input_index);
            }
        };

        debug_assert_eq!(
            self.remaining_sources,
            self.source_items.len() - self.finished_sources.len()
        );

        if self.remaining_sources == 0 {
            // If there are no remaining sources, then this is the final
            // batch. We should include all remaining input.

            // The minimum time for what we'll emit is either the last output time
            // or i64::MIN if we haven't output anything.
            let min_time_inclusive = self
                .last_output_time
                // This only happens if no output has been produced on
                // any input stream. We set the minimum time to i64::MIN
                // so we don't restrict the *future* output times
                // (if we were to resume from this).
                .unwrap_or(i64::MIN);

            // The max time is the time of the last event that we actually gathered.
            // If we haven't gathered anything, we haven't gathered anything, so the max
            // time can be the same as the min time
            let max_time_inclusive = self.max_time_gathered;
            self.last_output_time = Some(max_time_inclusive);

            // Just take the batches == we want everything, so we just replace this
            // `source_items` with an empty vector (we shouldn't be using it again).
            let batches = std::mem::take(&mut self.source_items);

            Ok(Some(GatheredBatches {
                batches,
                min_time_inclusive,
                max_time_inclusive,
            }))
        } else if let Some(emit_time) = self.determine_max_emit_time() {
            // Asserts that the gatherer only receives non-empty inputs or a time.
            if self.last_output_time.is_none() {
                anyhow::ensure!(
                    self.source_items.iter().any(|t| !t.is_empty()),
                    "Gatherer expected either a non-empty input or a last_output_time."
                );
            }

            // The minimum time is either the last output time (so that we're sure
            // the time range is continuous) or the minimum time in the sources (if
            // we've not yet emitted anything).
            let min_time_inclusive = self
                .last_output_time
                .or_else(|| {
                    self.source_items
                        .iter()
                        .filter(|t| !t.is_empty())
                        .map(|t| t.min_time)
                        .min()
                })
                // This shouldn't happen -- if we're here, we should have been able to
                // determine a time to output, which means there should be input on at
                // least some of the sources, which means we should have been able to
                // use either the last output time or the minimum time in the first
                // gathered batch.
                .context("getting minimum output time")?;

            // TODO: Should this just be the `emit_time`? Add unit test
            let max_time_inclusive = emit_time - 1;

            self.last_output_time = Some(emit_time);
            let batches: Vec<_> = self
                .source_items
                .iter_mut()
                .map(|tracker| -> anyhow::Result<OrderedInputs<T>> {
                    let lt = tracker.split_lt(emit_time)?;
                    Ok(lt)
                })
                .try_collect()?;
            debug_assert_eq!(batches.len(), self.source_items.len());

            Ok(Some(GatheredBatches {
                batches,
                min_time_inclusive,
                max_time_inclusive,
            }))
        } else {
            Ok(None)
        }
    }

    /// Determine whether (and up to when) to emit a new batch.
    ///
    /// This *must* represent a "complete" point in time. Specifically, all
    /// input streams must have received batches containing rows *after* the
    /// selected point in time. Since each stream produces rows in order, this
    /// means the complete rows up to that time have been received for that
    /// input.
    ///
    /// Currently, the approach is eager -- it creates a slice from the previous
    /// emitted time (inclusive) to the minimum possible emit time across all
    /// inputs. This may produce an excess of small batches. If that poses a
    /// a problem, we could employ a heuristic considering the total number of
    /// elements that would be sliced from the inputs.
    fn determine_max_emit_time(&self) -> Option<i64> {
        // Each input is complete up to the times *less than* the maximum received time.
        // For instance consider the three batches with time ranges as follows
        // `(0..=1), (1..=1), (1..5)`. Until the final batch which contains rows
        // *greater than* `1`, we could continue receiving new events with time
        // `1`. Thus, we can't output *any* rows at time `1` until that stream
        // either finishes or receives events with a greater time.
        let complete_time = self
            .source_items
            .iter()
            .enumerate()
            .filter_map(|(index, items)| {
                if self.finished_sources.contains(index) {
                    None
                } else {
                    Some(items.max_time())
                }
            })
            .min();

        // We need the emit time to be greater than the last output, otherwise
        // it doesn't include enough new data to slice off another piece. We also
        // need it to be after `i64::MIN`, since otherwise at least one of the inputs
        // is empty.
        let last_output_time = self.last_output_time.unwrap_or(i64::MIN);
        complete_time.filter(|time| *time > last_output_time)
    }
}

#[cfg(test)]
mod tests {

    use std::ops::RangeInclusive;

    use super::*;

    // Implement `InputItem` for range, so we can easily test the gather/split
    // logic.
    impl InputItem for RangeInclusive<i64> {
        fn min_time(&self) -> i64 {
            *self.start()
        }

        fn max_time(&self) -> i64 {
            *self.end()
        }

        fn split_at(self, split_point: i64) -> anyhow::Result<(Option<Self>, Option<Self>)> {
            assert!(
                self.contains(&split_point),
                "Split point {split_point} must be in range {self:?}"
            );

            let start = *self.start();
            let end = *self.end();

            Ok((Some(start..=split_point - 1), Some(split_point..=end)))
        }
    }

    fn inputs(
        min_time: i64,
        max_time: i64,
        ranges: impl IntoIterator<Item = RangeInclusive<i64>>,
    ) -> OrderedInputs<RangeInclusive<i64>> {
        OrderedInputs {
            items: ranges.into_iter().collect(),
            min_time,
            max_time,
        }
    }

    #[test]
    fn test_gather_basic() {
        let mut gatherer: Gatherer<RangeInclusive<i64>> = Gatherer::new(2, None);
        assert_eq!(gatherer.add_batch(0, Some(0..=5)).unwrap(), None);
        assert_eq!(gatherer.add_batch(0, Some(5..=5)).unwrap(), None);
        assert_eq!(gatherer.add_batch(0, Some(5..=8)).unwrap(), None);
        assert_eq!(
            gatherer.add_batch(1, Some(0..=6)).unwrap(),
            Some(GatheredBatches {
                batches: vec![inputs(0, 5, [0..=5, 5..=5, 5..=5]), inputs(0, 5, [0..=5])],
                min_time_inclusive: 0,
                max_time_inclusive: 5,
            })
        );

        assert_eq!(
            gatherer.add_batch(1, Some(6..=8)).unwrap(),
            Some(GatheredBatches {
                batches: vec![inputs(6, 7, [6..=7]), inputs(6, 7, [6..=6, 6..=7]),],
                min_time_inclusive: 6,
                max_time_inclusive: 7,
            })
        );

        assert_eq!(gatherer.add_batch(0, None).unwrap(), None);
        assert_eq!(
            gatherer.add_batch(1, None).unwrap(),
            Some(GatheredBatches {
                batches: vec![inputs(8, 8, [8..=8]), inputs(8, 8, [8..=8]),],
                min_time_inclusive: 8,
                max_time_inclusive: 8,
            })
        );
    }

    #[test]
    fn test_skipping_empty_inputs_to_time_fails() {
        // Currently, we expect this behavior to fail. The gatherer only
        // expects files for which we have inputs, so we don't expect to
        // see this.
        let mut gatherer: Gatherer<RangeInclusive<i64>> = Gatherer::new(2, None);

        // Skip the first inputs time to greater than i64::MIN
        gatherer.skip_to(0, 10).unwrap();

        match gatherer.add_batch(1, None) {
            Ok(_) => panic!("Expected a failure adding empty batch"),
            Err(e) => assert_eq!(
                e.to_string(),
                "Gatherer expected either a non-empty input or a last_output_time."
            ),
        }
    }

    // TODO: proptests for the gatherer?
    // These should focus on the correctness conditions, so that
    // the heuristics can be adjusted.
}
