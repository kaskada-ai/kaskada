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
