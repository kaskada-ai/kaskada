use std::cmp::Ordering;
use std::convert::TryFrom;
use std::ops;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, PrimitiveArray, TimestampNanosecondArray, UInt64Array};
use arrow::datatypes::{ArrowPrimitiveType, TimestampNanosecondType, UInt64Type};
use arrow::record_batch::RecordBatch;
use owning_ref::ArcRef;

use crate::TableSchema;
use sparrow_arrow::downcast::downcast_primitive_array;

/// The key columns for a given batch.
///
/// These are frequently manipulated as a unit and this struct allows pulling
/// them out and manipulating as a unit.
#[derive(Clone, Debug)]
pub struct KeyTriples {
    /// The time column.
    time: ArcRef<dyn Array, TimestampNanosecondArray>,
    /// The subsort column represented as a `u64`.
    subsort: ArcRef<dyn Array, UInt64Array>,
    /// The `key` column represented as a `u64`.
    key_hash: ArcRef<dyn Array, UInt64Array>,
}

/// Struct representing a single key triple.
#[derive(Debug, Copy, Clone)]
pub struct KeyTriple {
    pub time: i64,
    pub subsort: u64,
    pub key_hash: u64,
}

impl std::fmt::Display for KeyTriple {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<{}, {}, {}>", self.time, self.subsort, self.key_hash)
    }
}

impl TryFrom<&RecordBatch> for KeyTriples {
    type Error = anyhow::Error;

    fn try_from(value: &RecordBatch) -> anyhow::Result<Self> {
        anyhow::ensure!(
            value.schema().field(0).name() == TableSchema::TIME,
            "First field must be '{}' but was '{}'",
            TableSchema::TIME,
            value.schema().field(0).name()
        );

        anyhow::ensure!(
            value.schema().field(1).name() == TableSchema::SUBSORT,
            "Second field must be '{}', but was '{}'",
            TableSchema::SUBSORT,
            value.schema().field(1).name()
        );

        anyhow::ensure!(
            value.schema().field(2).name() == TableSchema::KEY_HASH,
            "Third field must be '{}', but was '{}'",
            TableSchema::KEY_HASH,
            value.schema().field(2).name()
        );

        Self::try_new(
            value.column(0).clone(),
            value.column(1).clone(),
            value.column(2).clone(),
        )
    }
}

/// As `downcast_primitive_array` but retaining the owning `Arc`.
fn owning_downcast_primitive<T: ArrowPrimitiveType>(
    array: ArrayRef,
) -> anyhow::Result<ArcRef<dyn Array, PrimitiveArray<T>>> {
    ArcRef::from(array.clone()).try_map(|arr| downcast_primitive_array::<T>(arr))
}

fn owning_array_slice<T: 'static + Array>(
    array: &ArcRef<dyn Array, T>,
    offset: usize,
    length: usize,
) -> ArcRef<dyn Array, T> {
    let owner = array.as_owner().clone();
    let owner_slice = owner.slice(offset, length);
    ArcRef::from(owner_slice).map(|array| {
        array
            .as_any()
            .downcast_ref::<T>()
            .expect("Slicing shouldn't change types")
    })
}

impl KeyTriples {
    /// Create a new struct from the given arrays.
    ///
    /// # Errors
    ///
    /// This will fail if the arrays are not of the correct types.
    pub fn try_new(
        time: ArrayRef,
        subsort: ArrayRef,
        key_hash: ArrayRef,
    ) -> anyhow::Result<KeyTriples> {
        let time = owning_downcast_primitive::<TimestampNanosecondType>(time)?;
        let subsort = owning_downcast_primitive::<UInt64Type>(subsort)?;
        let key_hash = owning_downcast_primitive::<UInt64Type>(key_hash)?;

        debug_assert_eq!(
            time.len(),
            subsort.len(),
            "Time and subsort columns columns must be same length, was {} and {}",
            time.len(),
            subsort.len()
        );
        debug_assert_eq!(
            time.len(),
            key_hash.len(),
            "Time and key_hash columns must be same length, was {} and {}",
            time.len(),
            key_hash.len()
        );

        Ok(Self {
            time,
            subsort,
            key_hash,
        })
    }

    /// Create a KeyTriples from slices.
    ///
    /// This is mostly useful for testing. It will panic if the arrays aren't of
    /// the same length or aren't sorted.
    pub fn new_from_slices(times: &[i64], subsorts: &[u64], key_hashes: &[u64]) -> Self {
        let triples = KeyTriples::try_new(
            Arc::new(TimestampNanosecondArray::from(times.to_vec())),
            Arc::new(UInt64Array::from(subsorts.to_vec())),
            Arc::new(UInt64Array::from(key_hashes.to_vec())),
        )
        .unwrap();

        // Make sure the results are sorted.
        for i in 1..triples.len() {
            assert!(triples.value(i - 1) < triples.value(i));
        }

        triples
    }

    pub fn slice(&self, offset: usize, length: usize) -> Self {
        Self {
            time: owning_array_slice(&self.time, offset, length),
            subsort: owning_array_slice(&self.subsort, offset, length),
            key_hash: owning_array_slice(&self.key_hash, offset, length),
        }
    }

    pub fn time_array(&self) -> &TimestampNanosecondArray {
        self.time.as_ref()
    }

    pub fn subsort_array(&self) -> &UInt64Array {
        self.subsort.as_ref()
    }

    pub fn key_hash_array(&self) -> &UInt64Array {
        self.key_hash.as_ref()
    }

    pub fn time_array_ref(&self) -> &ArrayRef {
        self.time.as_owner()
    }

    pub fn subsort_array_ref(&self) -> &ArrayRef {
        self.subsort.as_owner()
    }

    pub fn key_hash_array_ref(&self) -> &ArrayRef {
        self.key_hash.as_owner()
    }

    /// Returns the length of the key triple columns.
    pub fn len(&self) -> usize {
        self.time.len()
    }

    pub fn is_empty(&self) -> bool {
        self.time.is_empty()
    }

    /// Returns the `KeyTriple` for the given row.
    pub fn value(&self, index: usize) -> KeyTriple {
        debug_assert!(
            index < self.len(),
            "Index {} must be less than length {}",
            index,
            self.len()
        );

        let time = self.time.value(index);
        let subsort = self.subsort.value(index);
        let key_hash = self.key_hash.value(index);
        KeyTriple {
            time,
            subsort,
            key_hash,
        }
    }

    /// Return the time value associated with the given index.
    pub fn time(&self, index: usize) -> i64 {
        debug_assert!(
            index < self.len(),
            "Index {} must be less than length {}",
            index,
            self.len()
        );
        self.time.value(index)
    }

    /// Return the subsort value associated with the given index.
    pub fn subsort(&self, index: usize) -> u64 {
        debug_assert!(
            index < self.len(),
            "Index {} must be less than length {}",
            index,
            self.len()
        );
        self.subsort.value(index)
    }

    /// Return the key hash value associated with the given index.
    pub fn key_hash(&self, index: usize) -> u64 {
        debug_assert!(
            index < self.len(),
            "Index {} must be less than length {}",
            index,
            self.len()
        );
        self.key_hash.value(index)
    }

    /// Fast-path equality based on pointers.
    pub fn ptr_eq(lhs: &Self, rhs: &Self) -> bool {
        ptr_eq(&lhs.time, &rhs.time)
            && ptr_eq(&lhs.subsort, &rhs.subsort)
            && ptr_eq(&lhs.key_hash, &rhs.key_hash)
    }

    /// Finds the maximum length of a run starting at the given position.
    ///
    /// A run is a sequence of items of length less than or equal to
    /// `max_length` all of which are less than or equal to `max_inclusive`.
    pub fn run_length(&self, start: usize, max_inclusive: &KeyTriple) -> usize {
        // TODO: Benchmark this and see if the value accessor is expensive. We could
        // write a method `self.compare_idx(idx, other)` that compares without
        // accessing and constructing the value.

        // We start with an exponential search rather than a binary search. This
        // provides the property that if the run length is short, the complexity
        // is determined by the position rather than the number of remaining
        // items.
        let mut size = 1;
        while start + size < self.len() && &self.value(start + size) < max_inclusive {
            size *= 2;
        }

        // We know the actual item lies between size / 2 and size.
        // If there are fewer than 16 items, we'll just do a linear search which will
        // have better locality.
        // TODO: Adjust this threshold.
        let lower = size / 2;
        let upper = size.min(self.len() - start - 1);
        let length = if upper - lower <= 8 {
            (lower..=upper)
                .find_map(|i| match &self.value(start + i).cmp(max_inclusive) {
                    Ordering::Equal => Some(i + 1),
                    Ordering::Greater => Some(i),
                    Ordering::Less => None,
                })
                .unwrap_or(upper + 1)
        } else {
            // Perform the binary search between size / 2 and size.
            match self.binary_search(start, lower, upper, max_inclusive) {
                Ok(found) => found + 1,
                Err(missing) => missing,
            }
        };

        // A variety of assertions for debugging. This asserts that
        //   (1) the length is within the expected range
        //   (2) the last included item is less than or equal to the maximum
        //   (3) the first excluded item (if any) is greater than the maximum
        #[cfg(debug_assertions)]
        {
            debug_assert!(length <= self.len());

            if length > 0 {
                let last_included_key = self.value(start + length - 1);
                debug_assert!(
                    &last_included_key <= max_inclusive,
                    "Run was too long; expected {last_included_key:?} <= {max_inclusive:?}"
                );
            }

            if start + length < self.len() {
                let first_excluded_key = self.value(start + length);
                debug_assert!(
                    &first_excluded_key > max_inclusive,
                    "Run was too short; expected {first_excluded_key:?} > {max_inclusive:?}"
                );
            }
        }

        length
    }

    /// A specialized version of binary search that allows us to operate on the
    /// key-triples from the individual arrays. This is based on the code
    /// from for `Vec::binary_search_by`.
    fn binary_search(
        &self,
        start: usize,
        min: usize,
        max: usize,
        max_inclusive: &KeyTriple,
    ) -> std::result::Result<usize, usize> {
        debug_assert!(start + max <= self.len());

        let mut size = max - min + 1;
        if size == 0 {
            return Err(start);
        }

        let mut base = start + min;
        while size > 1 {
            let half = size / 2;
            let mid = base + half;

            base = if &self.value(mid) > max_inclusive {
                base
            } else {
                mid
            };
            size -= half;
        }

        match self.value(base).cmp(max_inclusive) {
            Ordering::Less => Err(base + 1 - start),
            Ordering::Equal => Ok(base - start),
            Ordering::Greater => Err(base - start),
        }
    }

    pub fn iter(&self) -> KeyTriplesIter<'_> {
        KeyTriplesIter {
            key_triples: self,
            limit: self.len(),
            next: 0,
        }
    }

    pub fn first(&self) -> Option<KeyTriple> {
        if self.is_empty() {
            None
        } else {
            Some(self.value(0))
        }
    }

    pub fn last(&self) -> Option<KeyTriple> {
        if self.is_empty() {
            None
        } else {
            Some(self.value(self.len() - 1))
        }
    }

    pub fn first_out_of_order(&self) -> Option<(KeyTriple, KeyTriple)> {
        match self.first() {
            None => None,
            Some(first) => {
                let mut prev = first;
                for idx in 1..self.len() {
                    let next = self.value(idx);
                    if prev >= next {
                        return Some((prev, next));
                    }
                    prev = next;
                }
                None
            }
        }
    }

    /// Concatenate two key triples together.
    ///
    /// Note: This method does not enforce any requirements that the result
    /// is properly ordered. That is up to the caller. This method preserves
    /// the order of the original arrays, starting with `self` followed by
    /// `other.
    pub fn concat(&self, other: &KeyTriples) -> anyhow::Result<KeyTriples> {
        let time = arrow::compute::concat(&[self.time_array(), other.time_array()])?;
        let subsort = arrow::compute::concat(&[self.subsort_array(), other.subsort_array()])?;
        let key_hash = arrow::compute::concat(&[self.key_hash_array(), other.key_hash_array()])?;
        KeyTriples::try_new(time, subsort, key_hash)
    }
}

/// Iterator over the key triples.
pub struct KeyTriplesIter<'a> {
    key_triples: &'a KeyTriples,
    limit: usize,
    next: usize,
}

impl<'a> Iterator for KeyTriplesIter<'a> {
    type Item = KeyTriple;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next < self.limit {
            let result = self.key_triples.value(self.next);
            self.next += 1;
            Some(result)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.limit - self.next;
        (size, Some(size))
    }
}

impl<'a> ExactSizeIterator for KeyTriplesIter<'a> {
    fn len(&self) -> usize {
        self.limit - self.next
    }
}

fn ptr_eq<T>(lhs: &T, rhs: &T) -> bool
where
    T: ops::Deref,
    T::Target: Array,
{
    arrow::array::ArrayData::ptr_eq(&lhs.to_data(), &rhs.to_data())
}

impl KeyTriple {
    pub fn new(time: i64, subsort: u64, key_hash: u64) -> Self {
        Self {
            time,
            subsort,
            key_hash,
        }
    }

    /// The minimum `KeyTriple`.
    pub const MIN: Self = Self {
        time: i64::MIN,
        subsort: u64::MIN,
        key_hash: u64::MIN,
    };

    /// Returns this key triple minus one.
    ///
    /// Useful when passing an exclusive maximum to operations expecting
    /// inclusive maximum.
    pub fn minus_one(&self) -> Self {
        if self.key_hash > 0 {
            Self {
                time: self.time,
                subsort: self.subsort,
                key_hash: self.key_hash - 1,
            }
        } else if self.subsort > 0 {
            Self {
                time: self.time,
                subsort: self.subsort - 1,
                key_hash: u64::MAX,
            }
        } else {
            assert!(self.time > 0, "Unable to subtract 1 from MIN key triple");
            Self {
                time: self.time - 1,
                subsort: u64::MAX,
                key_hash: u64::MAX,
            }
        }
    }
}

impl PartialEq for KeyTriple {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.subsort == other.subsort && self.key_hash == other.key_hash
    }
}

impl Eq for KeyTriple {}

impl PartialOrd for KeyTriple {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for KeyTriple {
    fn cmp(&self, other: &Self) -> Ordering {
        self.time
            .cmp(&other.time)
            .then(self.subsort.cmp(&other.subsort))
            .then(self.key_hash.cmp(&other.key_hash))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{TimestampNanosecondArray, UInt64Array};

    use super::*;

    #[test]
    fn test_run_length() {
        let triples = KeyTriples::try_new(
            Arc::new(TimestampNanosecondArray::from(vec![
                1, 1, 1, 2, 2, 2, 3, 3, 3,
            ])),
            Arc::new(UInt64Array::from(vec![0, 1, 2, 1, 2, 3, 1, 5, 7])),
            Arc::new(UInt64Array::from(vec![0; 9])),
        )
        .unwrap();

        assert_eq!(triples.run_length(0, &KeyTriple::new(5, 10, 0)), 9);
        assert_eq!(triples.run_length(0, &KeyTriple::new(3, 10, 0)), 9);
        assert_eq!(triples.run_length(0, &KeyTriple::new(3, 0, 0)), 6);
        assert_eq!(triples.run_length(0, &KeyTriple::new(2, 2, 0)), 5);
        assert_eq!(triples.run_length(0, &KeyTriple::new(1, 1, 0)), 2);

        assert_eq!(triples.run_length(1, &KeyTriple::new(5, 10, 0)), 8);
        assert_eq!(triples.run_length(1, &KeyTriple::new(3, 10, 0)), 8);
        assert_eq!(triples.run_length(1, &KeyTriple::new(3, 0, 0)), 5);
        assert_eq!(triples.run_length(1, &KeyTriple::new(2, 2, 0)), 4);
        assert_eq!(triples.run_length(1, &KeyTriple::new(1, 1, 0)), 1);
    }

    #[test]
    fn test_run_length_short() {
        let triples = KeyTriples::try_new(
            Arc::new(TimestampNanosecondArray::from(vec![1, 2, 3])),
            Arc::new(UInt64Array::from(vec![0, 1, 2])),
            Arc::new(UInt64Array::from(vec![0; 3])),
        )
        .unwrap();

        assert_eq!(triples.run_length(0, &KeyTriple::new(5, 10, 0)), 3);

        assert_eq!(triples.run_length(1, &KeyTriple::new(5, 10, 0)), 2);
    }
}
