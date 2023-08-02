use serde::de::DeserializeOwned;
use serde::Serialize;
use std::collections::VecDeque;

use crate::{ComputeStore, StateToken, StoreKey};

/// State token used for the lag operator.
#[derive(Default, Debug)]
pub struct CollectToken<T>
where
    T: Clone,
    T: Serialize + DeserializeOwned,
    Vec<VecDeque<Option<T>>>: Serialize + DeserializeOwned,
{
    state: Vec<VecDeque<Option<T>>>,

    /// Contains the offset for the each window part in the state.
    ///
    /// Each vecdeque in `state` contains the entire possible set of
    /// values that any currently existing window can have. When a window
    /// closes (i.e. receive a `tick`), we slice the state to the offset
    /// determined by the size of the sliding window, using the offset values
    /// contained in this vec.
    ///
    /// Invariants maintained:
    ///  * len(offsets) <= num_windows
    ///    After we've seen `num_windows` ticks, the len(offsets) should always be equal to `num_windows`
    ///  * offsets[0] == 0
    ///    The first window always starts at the beginning of the state
    ///  * offset[i] maintains that all values at state[offset[i]] exist in the window(s) prior to the ith window
    ///
    /// Consider the inputs [1, 2, 3, 4, 4.1, 4.2, 4.3, 5, 5.5, 6, 7, 9.5]
    /// Assume each input happens at the hour, then anything between happens between
    /// the hour.
    ///
    /// In this example, we have a sliding window of width 2 hours, slide 1 hour.
    /// We know that there will be 2 overlapping windows of any time
    /// ((width / slide) + (width % slide)).
    ///
    ///
    ///  1]                                                    (-inf-1 window)
    ///  1, 2]                                                 (0-2 window)
    /// [   2, 3]                                              (1-3 window)
    ///    [   3, 4]                                           (2-4 window)
    ///        [  4, 4.1, 4.2, 4.3, 5]                         (3-5 window)
    ///           [  4.1, 4.2, 4.3, 5, 5.5, 6.1]               (4-6 window)
    ///                             [  5.5, 6.1, 7]            (5-7 window)
    ///                                    [6.1, 7]            (6-8 window)
    ///                                          [ ]           (7-9 window)
    ///                                               [9.5]    (8-10 window)
    ///                                               [9.5]    (9-11 window)
    ///
    /// The state will contain values from the beginning of the first window to the
    /// end of the last window. The offsets contains the indices in the state at which
    /// a new window is created.
    ///
    /// After each tick, we will:
    /// 1) Slice the state to the 1st offset index
    /// 2) Shift the offsets to the left, and subtract the first offset from all
    /// 3) Add the new value to the end of the state
    /// 4) Add the new offset min(0, (len(state))) to the end of the offsets
    ///
    /// Initial state:
    /// state: []
    /// offsets: [0, 0]
    ///
    /// First, value [0.1]
    /// state: [0.1]
    /// offsets: [0, 0]
    /// output: [0.1]
    ///
    /// Next, value [0.2]
    /// state: [0.1, 0.2]
    /// offsets: [0, 0]
    /// output: [0.1, 0.2]
    ///
    /// Next, value [1]
    /// state: [0.1, 0.2, 1]
    /// offsets: [0, 0]
    /// output: [0.1, 0.2, 1]
    ///
    /// Next, tick at 1
    /// state: [0.1, 0.2, 1]
    /// offsets: [0, 0]
    /// output: [0.1, 0.2, 1]
    ///
    /// Next, value [2]
    /// state: [0.1, 0.2, 1, 2]
    /// offsets: [0, 3] // Set offset[1] to len(state) in previous step (TODO: If we don't get a subsequent value, this is technically "out of bounds")
    /// output: [0.1, 0.2, 1, 2]
    ///
    /// Next, tick at 2
    /// state: [0.1, 0.2, 1, 2]
    /// offsets: [0, 3] // Slice the state to index offsets[1] == 3
    /// output: [0.1, 0.2, 1, 2]
    ///
    /// Next, value [3]
    /// state: [2, 3]
    /// offsets: [0, 1]
    /// output: [2, 3]
    ///
    /// Next, tick at 3
    /// state: [2, 3]
    /// offsets: [0, 1] // Slice the state at index offsets[1] == 1
    /// output: [2, 3]
    ///
    /// Next, value [4]
    /// state: [3, 4]
    /// offsets: [0, 1]
    /// output: [3, 4]
    ///
    /// Next, tick at 4
    /// state: [3, 4]
    /// offsets: [0, 1]
    /// output: [3, 4]
    ///
    /// Next, value [4.1]
    /// state: [3, 4, 4.1]
    /// offsets: [0, 1]
    /// output: [3, 4, 4.1]
    ///
    /// Next, value [4.2]
    /// state: [3, 4, 4.1, 4.2]
    /// offsets: [0, 1]
    /// output: [3, 4, 4.1, 4.2]
    ///
    /// Next, value [4.3]
    /// state: [3, 4, 4.1, 4.2, 4.3]
    /// offsets: [0, 1]
    /// output: [3, 4, 4.1, 4.2, 4.3]
    ///
    /// Next, value [5]
    /// state: [3, 4, 4.1, 4.2, 4.3, 5]
    /// offsets: [0, 1]
    /// output: [3, 4, 4.1, 4.2, 4.3, 5]
    ///
    /// Next, tick at 5
    /// state: [3, 4, 4.1, 4.2, 4.3, 5]
    /// offsets: [0, 1] // Slice the state at index offsets[1] == 1
    /// output: [3, 4, 4.1, 4.2, 4.3, 5]
    ///
    /// Next, value [5.5]
    /// state: [4, 4.1, 4.2, 4.3, 5, 5.5]
    /// offsets: [0, 5] // offset[1] is set to len(state) after slicing in the previous step
    /// output: [4, 4.1, 4.2, 4.3, 5, 5.5]
    ///
    /// Next, tick at 6
    /// state: [4, 4.1, 4.2, 4.3, 5, 5.5]
    /// offsets: [0, 5] // Slice state at index offsets[1] == 5
    /// output: [4, 4.1, 4.2, 4.3, 5, 5.5]
    ///
    /// Next, value [6.1]
    /// state: [5.5, 6.1]
    /// offsets: [0, 1] // offset[1] set to len(state) after slicing in previous step
    /// output: [5.5, 6.1]
    ///
    /// Next, value [7]
    /// state: [5.5, 6.1, 7]
    /// offsets: [0, 1]
    /// output: [5.5, 6.1, 7]
    ///
    /// Next, tick at 7
    /// state: [5.5, 6.1, 7]
    /// offsets: [0, 1] // Slice state at index offsets[1] == 1
    /// output: [5.5, 6.1, 7]
    ///
    /// Next, tick at 8
    /// state: [6.1, 7]
    /// offsets: [0, 2] // Slice state at index offsets[1] == 2
    /// output: [6.1, 7]
    ///
    /// Next, tick at 9
    /// state: []
    /// offsets: [0, 0] // offset[1] set to len(state) after slicing
    /// output: []
    ///
    /// Next, value [9.5]
    /// state: [9.5]
    /// offsets: [0, 0]
    /// output: 9.5
    ///
    /// Essentially, what we're doing is maintaining the entire set of values that may exist,
    /// but keeping track of what values exist within each window division, which allows us to
    /// slice the current window out of the state, then drop values as the window moves forward
    /// in time.
    offsets: VecDeque<usize>,
}

impl<T> CollectToken<T>
where
    T: Clone,
    T: Serialize + DeserializeOwned,
    Vec<VecDeque<Option<T>>>: Serialize + DeserializeOwned,
{
    pub fn resize(&mut self, len: usize) {
        if len >= self.state.len() {
            self.state.resize(len + 1, VecDeque::new());
        }
    }

    pub fn add_value(&mut self, max: usize, index: usize, input: Option<T>) {
        self.state[index].push_back(input);
        if self.state[index].len() > max {
            self.state[index].pop_front();
        }
    }

    pub fn state(&self, index: usize) -> &VecDeque<Option<T>> {
        &self.state[index]
    }

    pub fn reset(&mut self, index: usize) {
        self.state[index].clear();
    }
}

impl<T> StateToken for CollectToken<T>
where
    T: Clone,
    T: Serialize + DeserializeOwned,
    Vec<VecDeque<Option<T>>>: Serialize + DeserializeOwned,
{
    fn restore(&mut self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        if let Some(state) = store.get(key)? {
            self.state = state;
        } else {
            self.state.clear();
        }

        Ok(())
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self.state)
    }
}
