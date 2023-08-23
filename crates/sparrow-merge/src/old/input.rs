use smallvec::SmallVec;

/// An `InputItem` is is a splittable container of time-ordered data.
///
/// The typical implementation used is `MergeInput` which implements this
/// for an ordered `RecordBatch`.
pub trait InputItem: Sized {
    /// Return the minimum time in `self`.
    fn min_time(&self) -> i64;

    /// Return the maximum time in `self`.
    fn max_time(&self) -> i64;

    /// Split the input item at the given time.
    ///
    /// Return the tuple `(lt, gte)` containing the values less than the
    /// `split_time` and greater than or equal to the `split_time`,
    /// respectively.
    fn split_at(self, split_time: i64) -> anyhow::Result<(Option<Self>, Option<Self>)>;
}

/// A collection of zero or more `InputItems` ordered by time.
#[derive(Debug, PartialEq, Eq)]
pub struct OrderedInputs<T> {
    /// The input items.
    ///
    /// We use a `SmallVec` because in most cases we're dealing with a
    /// small number of items.
    pub(super) items: SmallVec<[T; 4]>,
    /// The minimum time contained in any of the items.
    ///
    /// If items is non-empty, this should be the minimum of the first item.
    /// Other items should be ordered, so this should be the minimum of all
    /// items.
    ///
    /// If items is empty, this should equal the max time, and any added items
    /// must have a minimum time equal to or greater than this.
    ///
    /// This should be less than or equal to the `max_time`.
    pub(super) min_time: i64,
    /// The maximum time contained in any of the items.
    ///
    /// If items is non-empty, this should be the maximum time contained in the
    /// last item. Other items should be ordered, so this should be the
    /// maximum of all items.
    ///
    /// If items is empty, this should equal the min time, and any added items
    /// must have a minimum time equal to or greater than this.
    ///
    /// Specifically, whether items is empty or not, any future items must have
    /// a minimum time greater than or equal to the max time.
    pub(super) max_time: i64,
}

impl<T> Default for OrderedInputs<T> {
    fn default() -> Self {
        Self {
            items: SmallVec::new(),
            min_time: i64::MIN,
            max_time: i64::MIN,
        }
    }
}

impl<T> OrderedInputs<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn max_time(&self) -> i64 {
        self.max_time
    }

    pub fn skip_to(&mut self, time: i64) -> anyhow::Result<()> {
        // If this fails, we'll likely need to change the logic around
        // `possible_emit_time` since we need to be able to indicate "emit up to
        // and including `i64::MAX`".
        anyhow::ensure!(time < i64::MAX, "Skipping to max time not expected");
        anyhow::ensure!(
            self.max_time <= time,
            "Unable to skip backwards from {:?} to {:?}",
            self.max_time,
            time
        );

        self.max_time = time;

        Ok(())
    }
}

impl<T: InputItem> OrderedInputs<T> {
    /// Add an item to this `OrderedInputs`.
    ///
    /// This will panic if the item's times are not monotonically increasing
    /// in relation to the previous item.
    pub fn add_item(&mut self, item: T) -> anyhow::Result<()> {
        // If this fails, we'll likely need to change the logic around
        // `possible_emit_time` since we need to be able to indicate "emit up to
        // and including `i64::MAX`".
        anyhow::ensure!(
            item.max_time() < i64::MAX,
            "Rows at max time are not expected"
        );

        anyhow::ensure!(
            self.max_time <= item.min_time(),
            "Batches must be received in increasing order of time but item min {} is less than \
             recorded max {}",
            item.min_time(),
            self.max_time,
        );

        anyhow::ensure!(
            item.min_time() <= item.max_time(),
            "Item minimum time ({:?}) must be less than or equal to maximum time ({:?})",
            item.min_time(),
            item.max_time()
        );

        if self.is_empty() {
            // If the range was empty, update the min time to reflect what is actually
            // present. We know that this will only (correctly) increase the min time since
            // `self.min_time <= self.max_time <= item.min_time()`.
            self.min_time = item.min_time();
        }

        // We know that self.max_time <= item.min_time() <= item.max_time(),
        // therefore this increases the maximum time.
        self.max_time = item.max_time();
        self.items.push(item);

        Ok(())
    }

    /// Split self at the given time.
    ///
    /// Returns the items (exclusively) less than the given time, and mutates
    /// `self` to contain the remaining items with times greater than or equal
    /// to `split_time`.
    pub fn split_lt(&mut self, split_at: i64) -> anyhow::Result<Self> {
        // If the split is the maximum time, then we want *all* batches.
        if split_at > self.max_time {
            let result = Self {
                items: std::mem::take(&mut self.items),
                min_time: self.min_time,
                max_time: self.max_time,
            };

            // All future batches must be *after* `split_at`.
            self.min_time = split_at;
            self.max_time = split_at;
            return Ok(result);
        }

        if self.items.is_empty() || split_at <= self.min_time {
            // Min time in this `OrderedInputs` is already greater than `split_at`.
            // There are no rows to include.
            return Ok(Self {
                items: SmallVec::new(),
                min_time: i64::MIN,
                max_time: split_at - 1,
            });
        }

        // Determine how many "complete" batches are outputtable.
        //
        // We do this by finding the first batch which may contain values
        // from the emit_time_exclusive (determined as `max_time >=
        // emit_time_exclusive`).
        let complete = self
            .items
            .iter()
            .position(|item| item.max_time() >= split_at);

        if let Some(complete) = complete {
            // Found a batch with a max time after the emit_time, which means we need to
            // split the batches and we'll have a residual.

            // Note: We drain up to and including the first item with max time greater than
            // emit_time exclusive.
            let mut result: SmallVec<[T; 4]> = self.items.drain(0..=complete).collect();

            // Now, examine the last item in the result, and split it if necessary.
            if let Some(last) = result.pop() {
                if last.max_time() >= split_at {
                    let (lt, gte) = last.split_at(split_at)?;
                    if let Some(lt) = lt {
                        result.push(lt)
                    }
                    if let Some(gte) = gte {
                        self.items.insert(0, gte);
                    }
                } else {
                    result.push(last)
                }
            }

            // Self was previous `min_time..=max_time`.
            // We have now split things so self should be `split_at..=max_time` and
            // the result is `min_time..=split_at`.
            let result_max_time = if result.is_empty() {
                split_at - 1
            } else {
                result[result.len() - 1].max_time()
            };
            let result = Self {
                items: result,
                min_time: self.min_time,
                max_time: result_max_time,
            };
            self.min_time = split_at;
            Ok(result)
        } else {
            unreachable!("All items had max_time < split_at, but should have been checked earlier")
        }
    }
}

impl<T: InputItem> IntoIterator for OrderedInputs<T> {
    type Item = T;
    type IntoIter = <SmallVec<[Self::Item; 4]> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.items.into_iter()
    }
}

impl<T: InputItem> std::ops::Index<usize> for OrderedInputs<T> {
    type Output = T;

    fn index(&self, index: usize) -> &Self::Output {
        &self.items[index]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ops::RangeInclusive;

    fn ranges(
        ranges: impl IntoIterator<Item = RangeInclusive<i64>>,
    ) -> SmallVec<[RangeInclusive<i64>; 4]> {
        ranges.into_iter().collect()
    }

    #[test]
    fn test_ordered_inputs_before_first_batch() {
        let mut inputs: OrderedInputs<RangeInclusive<i64>> = OrderedInputs::new();
        inputs.add_item(3..=5).unwrap();
        assert_eq!(inputs.min_time, 3);
        assert_eq!(inputs.max_time, 5);
        inputs.add_item(5..=5).unwrap();
        inputs.add_item(5..=10).unwrap();
        assert_eq!(inputs.min_time, 3);
        assert_eq!(inputs.max_time, 10);

        let lt = inputs.split_lt(2).unwrap();
        assert_eq!(lt.items, ranges([]));
        assert_eq!(lt.min_time, i64::MIN);
        assert_eq!(lt.max_time, 1);
        assert_eq!(inputs.items, ranges([3..=5, 5..=5, 5..=10]));
        assert_eq!(inputs.min_time, 3);
        assert_eq!(inputs.max_time, 10);
    }

    #[test]
    fn test_ordered_inputs_in_of_first_batch() {
        let mut inputs: OrderedInputs<RangeInclusive<i64>> = OrderedInputs::new();
        inputs.add_item(0..=5).unwrap();
        assert_eq!(inputs.min_time, 0);
        assert_eq!(inputs.max_time, 5);
        inputs.add_item(5..=5).unwrap();
        inputs.add_item(5..=10).unwrap();
        assert_eq!(inputs.min_time, 0);
        assert_eq!(inputs.max_time, 10);

        let lt = inputs.split_lt(4).unwrap();
        assert_eq!(lt.items, ranges([0..=3]));
        assert_eq!(lt.min_time, 0);
        assert_eq!(lt.max_time, 3);
        assert_eq!(inputs.items, ranges([4..=5, 5..=5, 5..=10]));
        assert_eq!(inputs.min_time, 4);
        assert_eq!(inputs.max_time, 10);
    }

    #[test]
    fn test_ordered_inputs_end_of_first_batch() {
        let mut inputs: OrderedInputs<RangeInclusive<i64>> = OrderedInputs::new();
        inputs.add_item(0..=5).unwrap();
        assert_eq!(inputs.min_time, 0);
        assert_eq!(inputs.max_time, 5);
        inputs.add_item(5..=5).unwrap();
        inputs.add_item(5..=10).unwrap();
        assert_eq!(inputs.min_time, 0);
        assert_eq!(inputs.max_time, 10);

        let lt = inputs.split_lt(5).unwrap();
        assert_eq!(lt.items, ranges([0..=4]));
        assert_eq!(lt.min_time, 0);
        assert_eq!(lt.max_time, 4);
        assert_eq!(inputs.items, ranges([5..=5, 5..=5, 5..=10]));
        assert_eq!(inputs.min_time, 5);
        assert_eq!(inputs.max_time, 10);
    }

    #[test]
    fn test_ordered_inputs_in_middle_batch() {
        let mut inputs: OrderedInputs<RangeInclusive<i64>> = OrderedInputs::new();
        inputs.add_item(1..=5).unwrap();
        assert_eq!(inputs.min_time, 1);
        assert_eq!(inputs.max_time, 5);
        inputs.add_item(5..=7).unwrap();
        inputs.add_item(8..=10).unwrap();
        assert_eq!(inputs.min_time, 1);
        assert_eq!(inputs.max_time, 10);

        let lt = inputs.split_lt(6).unwrap();
        assert_eq!(lt.items, ranges([1..=5, 5..=5]));
        assert_eq!(lt.min_time, 1);
        assert_eq!(lt.max_time, 5);
        assert_eq!(inputs.items, ranges([6..=7, 8..=10]));
        assert_eq!(inputs.min_time, 6);
        assert_eq!(inputs.max_time, 10);
    }

    #[test]
    fn test_ordered_inputs_in_last_batch() {
        let mut inputs: OrderedInputs<RangeInclusive<i64>> = OrderedInputs::new();
        inputs.add_item(1..=5).unwrap();
        assert_eq!(inputs.min_time, 1);
        assert_eq!(inputs.max_time, 5);
        inputs.add_item(5..=5).unwrap();
        inputs.add_item(5..=10).unwrap();
        assert_eq!(inputs.min_time, 1);
        assert_eq!(inputs.max_time, 10);

        let lt = inputs.split_lt(8).unwrap();
        assert_eq!(lt.items, ranges([1..=5, 5..=5, 5..=7]));
        assert_eq!(lt.min_time, 1);
        assert_eq!(lt.max_time, 7);
        assert_eq!(inputs.items, ranges([8..=10]));
        assert_eq!(inputs.min_time, 8);
        assert_eq!(inputs.max_time, 10);
    }

    // TODO: proptests for the OrderedInputs?
    // These should focus on the correctness conditions, so that
    // the heuristics can be adjusted.
}
