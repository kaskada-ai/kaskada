//! Defines the kernels for `lag`.
//!
//! The `lag` operation is stateful, like an aggregation. However, if the input
//! is discrete, the output is still discrete. Specifically, it doesn't change
//! the continuity of the input.
//!
//! We currently only implement `lag` for numeric types, since we anticipate
//! those being most useful. Other types (booleans, strings, structs) can be
//! implemented as needed.
//!
//! # TODO Buffer Sharing
//!
//! Currently, no optimizations is done to share the `lag` buffer. Specifically,
//! if a query contains both `lag(e, 1)` and `lag(e, 2)` we could satisfy that
//! with a single `lag` buffer holding the 2 preceding outputs. Doing this
//! optimization is relatively straightforward -- collect all `lag` operations
//! for each expression and either (1) rewrite them to something like
//! `lag_buffer(e, max(N))` and `lag_extract(lag_buffer(e, max(N)), n)` where
//! the first manages the buffer and the second extracts from it or (2)
//! configure the plan with some `lag` information similar to ticks. Option (1)
//! reuses machinery, but would run into some weirdness since it would need to
//! produce a *value*, or bypass the standard instruction behavior.
//!
//! Another option would be to combine them into something like `lag(e, 1, 2, 5,
//! 7)` to indicate the positions required. Then produce a struct containing
//! four columns. So:
//!
//! ```no_run
//! { a: lag(e, 1), b: lag(e, 2), c: lag(e, 5), d: lag(e, 7) }
//! ```
//!
//! Could be compiled to
//!
//! ```
//! let lag_buffer = lag(e, 1, 2, 5, 7)
//! in { a: lag_buffer.1, b: lag_buffer.2, c: lag_buffer.5, d: lag_buffer.7 }
//! ```
//!
//! Where the input has *all* required inputs and the output puts them as fields
//! of a struct.

use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{ArrayRef, PrimitiveArray, UInt32Array};
use arrow::datatypes::ArrowPrimitiveType;
use itertools::izip;
use sparrow_core::downcast_primitive_array;

use crate::utils::BitBufferIterator;

pub struct LagPrimitive<T: ArrowPrimitiveType> {
    pub state: Vec<VecDeque<T::Native>>,
}

impl<T: ArrowPrimitiveType> LagPrimitive<T> {
    pub fn new() -> Self {
        Self {
            state: Vec::default(),
        }
    }
}

impl<T: ArrowPrimitiveType> Default for LagPrimitive<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: ArrowPrimitiveType> std::fmt::Debug for LagPrimitive<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LagPrimitive")
            .field("state", &self.state)
            .finish()
    }
}

impl<T> LagPrimitive<T>
where
    T: ArrowPrimitiveType,
    T::Native: Copy,
{
    #[inline]
    fn execute_one(
        &mut self,
        entity_index: u32,
        input_is_valid: bool,
        input: T::Native,
        lag: usize,
    ) -> Option<T::Native> {
        let entity_index = entity_index as usize;
        let queue = &mut self.state[entity_index];

        // We don't put the current value in until after we get it, so `lag(1)` is in
        // position 0, `lag(2)` is in position 1, etc.
        let result = if queue.len() == lag {
            queue.front().copied()
        } else {
            None
        };

        if input_is_valid {
            if queue.len() == lag {
                queue.pop_front();
            }
            queue.push_back(input);
        };

        result
    }

    fn ensure_entity_capacity(&mut self, key_capacity: usize, n: usize) {
        if self.state.len() < key_capacity {
            self.state.resize(key_capacity, VecDeque::with_capacity(n))
        }
    }

    /// Update the lag state with the given inputs and return the
    /// lagging value.
    ///
    /// The `key_capacity` must be greater than all values in the
    /// `entity_indices`.
    pub fn execute(
        &mut self,
        key_capacity: usize,
        entity_indices: &UInt32Array,
        input: &ArrayRef,
        lag: usize,
    ) -> anyhow::Result<ArrayRef> {
        assert_eq!(entity_indices.len(), input.len());

        // Make sure the internal buffers are large enough for the accumulators we may
        // want to store.
        self.ensure_entity_capacity(key_capacity, lag);

        let input = downcast_primitive_array::<T>(input.as_ref())?;

        // TODO: Handle the case where the input is empty (null_count == len) and we
        // don't need to compute anything.

        let result: PrimitiveArray<T> = if let Some(is_valid) =
            BitBufferIterator::array_valid_bits(input)
        {
            let iter = izip!(is_valid, entity_indices.values(), input.values()).map(
                |(is_valid, entity_index, input)| {
                    self.execute_one(*entity_index, is_valid, *input, lag)
                },
            );

            // SAFETY: `izip!` and map are trusted length iterators.
            unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
        } else {
            // Handle the case where input contains no nulls. This allows us to
            // use `prim_input.values()` instead of `prim_input.iter()`.
            let iter = izip!(entity_indices.values(), input.values())
                .map(|(entity_index, input)| self.execute_one(*entity_index, true, *input, lag));

            // SAFETY: `izip!` and `map` are trusted length iterators.
            unsafe { PrimitiveArray::from_trusted_len_iter(iter) }
        };
        Ok(Arc::new(result))
    }
}
