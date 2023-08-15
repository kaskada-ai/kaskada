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
    Vec<VecDeque<T>>: Serialize + DeserializeOwned,
{
    state: Vec<VecDeque<T>>,
    /// Stores the times of the state values.
    ///
    /// Comprised of lists of timestamps for each entity.
    ///
    /// This array is only used when we have a `trailing` window.
    /// Likely this should be separated into a different implementation.
    times: Vec<VecDeque<i64>>,
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
            self.times.resize(len + 1, VecDeque::new());
        }
    }

    pub fn add_value(&mut self, max: usize, index: usize, input: T) {
        self.state[index].push_back(input);
        if self.state[index].len() > max {
            self.state[index].pop_front();
        }
    }

    /// Adds the input and time, then removes any values that are outside of the window.
    pub fn add_value_with_time(
        &mut self,
        max: usize,
        index: usize,
        input: T,
        time: i64,
        window_duration: i64,
    ) {
        self.state[index].push_back(input);
        self.times[index].push_back(time);
        if self.times[index].len() > max {
            self.state[index].pop_front();
            self.times[index].pop_front();
        }
        debug_assert_eq!(self.times[index].len(), self.state[index].len());

        self.check_time(index, time, window_duration)
    }

    /// Pops all values and times that are outside of the window
    pub fn check_time(&mut self, index: usize, time: i64, window_duration: i64) {
        debug_assert_eq!(self.times[index].len(), self.state[index].len());
        let min_time = time - window_duration;

        if let Some(mut front) = self.times[index].front() {
            while *front <= min_time {
                self.state[index].pop_front();
                self.times[index].pop_front();

                if let Some(f) = self.times[index].front() {
                    front = f
                } else {
                    break;
                }
            }
        }
    }

    pub fn state(&self, index: usize) -> &VecDeque<T> {
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
        // TODO: restore times
        panic!("time restoration not implemented")
    }

    fn store(&self, key: &StoreKey, store: &ComputeStore) -> anyhow::Result<()> {
        store.put(key, &self.state)
    }
}
