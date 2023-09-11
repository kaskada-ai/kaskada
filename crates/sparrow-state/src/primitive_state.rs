use std::marker::PhantomData;

use bitvec::vec::BitVec;

use crate::{Keys, StateToken};

/// Token for a specific primitive state.
pub struct PrimitiveToken<T> {
    state_index: u16,
    _phantom: PhantomData<fn(T) -> T>,
}

impl<T> PrimitiveToken<T> {
    pub fn new(state_index: u16) -> Self {
        Self {
            state_index,
            _phantom: PhantomData,
        }
    }
}

impl<T: 'static> StateToken for PrimitiveToken<T> {
    type State = PrimitiveState<T>;

    fn state_index(&self) -> u16 {
        self.state_index
    }
}

/// Representation of the primitive state.
pub struct PrimitiveState<T> {
    non_null: BitVec,
    value: Vec<T>,
}

impl<T: Copy + Default> PrimitiveState<T> {
    pub fn empty(keys: &Keys) -> Self {
        let mut non_null = BitVec::with_capacity(keys.num_key_hashes());
        non_null.resize(keys.num_rows(), false);
        let value = vec![T::default(); keys.num_key_hashes()];
        Self { non_null, value }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            non_null: BitVec::with_capacity(capacity),
            value: Vec::with_capacity(capacity),
        }
    }

    pub fn push_none(&mut self) {
        self.non_null.push(false);
        self.value.push(T::default());
    }

    pub fn push_some(&mut self, value: T) {
        self.non_null.push(true);
        self.value.push(value);
    }

    pub fn len(&self) -> usize {
        self.value.len()
    }

    pub fn is_empty(&self) -> bool {
        self.value.is_empty()
    }

    pub fn values(&self) -> impl Iterator<Item = Option<T>> + '_ {
        self.non_null
            .iter()
            .zip(self.value.iter().copied())
            .map(
                |(non_null, value)| {
                    if *non_null {
                        Some(value)
                    } else {
                        None
                    }
                },
            )
    }

    pub fn values2(&self) -> impl Iterator<Item = (bool, &T)> + '_ {
        self.non_null.iter().map(|b| *b).zip(self.value.iter())
    }

    pub fn get(&self, index: usize) -> Option<T> {
        if self.non_null[index] {
            Some(self.value[index])
        } else {
            None
        }
    }

    pub fn set(&mut self, index: usize, value: T) {
        self.non_null.set(index, true);
        self.value[index] = value;
    }

    pub fn clear(&mut self, index: usize) {
        self.non_null.set(index, false);
    }
}

// impl<T: serde::Serialize> StateToken for PrimitiveToken<T> {
//     // TODO: We could reduce memory usage by using a bit-vector on the side.
//     //
//     // Specifically, Option<i64> = 128 bits.
//     type InMemory = Vec<Option<T>>;

//     fn read(
//         &self,
//         backend: &dyn crate::StateBackend,
//     ) -> error_stack::Result<Self::InMemory, crate::Error> {
//         todo!()
//     }

//     fn write(
//         &self,
//         backend: &dyn crate::StateBackend,
//         value: Self::InMemory,
//     ) -> error_stack::Result<(), crate::Error> {
//         todo!()
//     }
// }
