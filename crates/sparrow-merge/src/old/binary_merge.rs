//! Implements an Arrow kernel that takes two inputs consisting of
//! the time, subsort and key_hash and produces the merged time,
//! subsort and key_hash columns.
//!
//! The process is a binary merge algorithm that attempts to use
//! find runs of input, allowing it to append slices to the output.

use std::cmp::Ordering;

use arrow_array::builder::{TimestampNanosecondBuilder, UInt64Builder};
use arrow_array::{ArrayRef, TimestampNanosecondArray, UInt64Array};
use arrow_schema::ArrowError;

/// Struct describing one of the two inputs to the [binary_merge]
/// function.
pub struct BinaryMergeInput<'a> {
    /// The column containing times to be merged.
    time: &'a TimestampNanosecondArray,
    /// The column containing subsorts to be merged.
    subsort: &'a UInt64Array,
    /// The column containing key hashes to be merged.
    key_hash: &'a UInt64Array,
}

impl<'a> BinaryMergeInput<'a> {
    pub fn new(
        time: &'a TimestampNanosecondArray,
        subsort: &'a UInt64Array,
        key_hash: &'a UInt64Array,
    ) -> Self {
        assert_eq!(time.len(), subsort.len());
        assert_eq!(time.len(), key_hash.len());
        Self {
            time,
            subsort,
            key_hash,
        }
    }

    /// Create a [BinaryMergeInput] from [ArrayRef]
    ///
    /// This is used in tests and benchmarks.
    pub fn from_array_refs(
        time: &'a ArrayRef,
        subsort: &'a ArrayRef,
        key_hash: &'a ArrayRef,
    ) -> anyhow::Result<Self> {
        use sparrow_arrow::downcast::downcast_primitive_array;

        let time = downcast_primitive_array(time.as_ref())?;
        let subsort = downcast_primitive_array(subsort.as_ref())?;
        let key_hash = downcast_primitive_array(key_hash.as_ref())?;

        Ok(BinaryMergeInput::new(time, subsort, key_hash))
    }

    /// Creates a [MergeInput] from the record batch.
    ///
    /// This assumes the key columns are present as the first 3 columns.
    pub fn from_batch(batch: &'a arrow_array::RecordBatch) -> anyhow::Result<Self> {
        use sparrow_core::TableSchema;

        anyhow::ensure!(
            batch.schema().field(0).name() == TableSchema::TIME,
            "First field must be '{}' but was '{}'",
            TableSchema::TIME,
            batch.schema().field(0).name()
        );

        anyhow::ensure!(
            batch.schema().field(1).name() == TableSchema::SUBSORT,
            "Second field must be '{}', but was '{}'",
            TableSchema::SUBSORT,
            batch.schema().field(1).name()
        );

        anyhow::ensure!(
            batch.schema().field(2).name() == TableSchema::KEY_HASH,
            "Third field must be '{}', but was '{}'",
            TableSchema::KEY_HASH,
            batch.schema().field(2).name()
        );

        Self::from_array_refs(batch.column(0), batch.column(1), batch.column(2))
    }

    fn len(&self) -> usize {
        self.time.len()
    }

    fn is_empty(&self) -> bool {
        self.time.is_empty()
    }

    /// Returns the key at the given inndex.
    ///
    /// This function doesn't check the bounds.
    unsafe fn key_unchecked(&self, index: usize) -> Key {
        debug_assert!(index < self.len());
        Key {
            time: self.time.value_unchecked(index),
            subsort: self.subsort.value_unchecked(index),
            key_hash: self.key_hash.value_unchecked(index),
        }
    }

    /// Returns the key at the given index.
    fn key(&self, index: usize) -> Key {
        Key {
            time: self.time.value(index),
            subsort: self.subsort.value(index),
            key_hash: self.key_hash.value(index),
        }
    }

    /// Determine the run length of keys strictly less than `max_exclusive`.
    ///
    /// Determines a value `N` such that:
    /// 1. `start..(start + N)` are valid indices in this array.
    /// 2. All keys in the range `start..(start + N)` are less than
    /// `max_exclusive`.
    ///
    /// Finding runs of values increased performance in the `two_dense` micro
    /// benchmarks 1300%.
    #[inline]
    fn run_length_lt(&self, start: usize, max_exclusive: &Key) -> usize {
        // Do a linear (or binary search) to find the run length.
        // We experimented with an exponential search (extending the range by doubling)
        // but it didn't seem to pay off in the micro benchmarks. We could revisit
        // if no benchmarks suggest it.
        let max_length = self.len() - start;
        if max_length < 4 {
            for length in 0..max_length {
                // SAFETY: length in 0..max_length; max_length <= len - start.
                // Thus, start + length is in bounds.
                let cmp = unsafe { self.compare_indices_unchecked(start + length, max_exclusive) };
                if cmp.is_ge() {
                    return length;
                }
            }
            max_length
        } else {
            match self.binary_search(start, max_length, max_exclusive) {
                Ok(found) => found - start,
                Err(to_insert) => to_insert - start,
            }
        }
    }

    /// A specialized version of binary search that allows us to operate on the
    /// key-triples from the individual arrays. This is based on the code
    /// from for `Vec::binary_search_by`.
    fn binary_search(&self, start: usize, len: usize, key: &Key) -> Result<usize, usize> {
        debug_assert!(start + len <= self.len());

        let mut size = len;
        let mut left = start;
        let mut right = start + size;
        while left < right {
            let mid = left + size / 2;

            // SAFETY: the call is made by the following invariants:
            // - `mid >= 0`
            // - `mid < size`: `mid` is limited by `[left; right)` bound.
            let cmp = unsafe { self.compare_indices_unchecked(mid, key) };

            // The reason why we use if/else control flow rather than match
            // is because match reorders comparison operations, which is perf sensitive.
            if cmp == Ordering::Less {
                left = mid + 1;
            } else if cmp == Ordering::Greater {
                right = mid;
            } else {
                // Vec::binary_search_by uses the following to inform the optimizer
                // that `mid < len`. Not sure it is useful, and currently it is an
                // experimental only API, so we can't use it.
                // SAFETY: same as the `compare_indices_unchecked`.
                // unsafe { std::intrinsics::assume(mid < len) };
                return Ok(mid);
            }

            size = right - left;
        }
        Err(left)
    }

    /// Compare the values at two `self[self_index]` to `key`.
    ///
    /// Only retrieves the values necessary to perform the comparison.
    ///
    /// NOTE: There may be better / other ways to improve the performance of
    /// this comparison, such as retaining the key looked up from previous
    /// iterations.
    #[inline]
    unsafe fn compare_indices_unchecked(&self, self_index: usize, key: &Key) -> std::cmp::Ordering {
        let time = self.time.value_unchecked(self_index);
        time.cmp(&key.time)
            .then_with(|| {
                let subsort = self.subsort.value_unchecked(self_index);
                subsort.cmp(&key.subsort)
            })
            .then_with(|| {
                let key_hash = self.key_hash.value_unchecked(self_index);
                key_hash.cmp(&key.key_hash)
            })
    }

    /// Determine the run length of equal keys between two inputs.
    ///
    /// Determines a value `N` such that:
    /// 1. `start..(start + N)` are valid indices in this array.
    /// 2. `other_start..(other_start + N)` are valid indices `other` array.
    /// 3. All keys in the range `start..(start + N)` in this array equal
    ///    the keys in the range `other_start..(other_start + N)` in `other`
    ///    array.
    ///
    /// Finding runs of equal values increased performance in the sparse micro
    /// benchmarks 800% (compared to always returning 1).
    #[inline]
    fn run_length_eq(
        &self,
        start: usize,
        other: &BinaryMergeInput<'_>,
        other_start: usize,
    ) -> usize {
        debug_assert_eq!(self.key(start), other.key(other_start));

        // This is a relatively naive approach for discovering runs.
        let mut length = 1;
        let max_length = std::cmp::min(self.len() - start, other.len() - other_start);
        while length < max_length {
            let self_index = start + length;
            let other_index = other_start + length;
            // SAFETY: `self_index` and `other_index` are valid because
            // `length` is less than `max_length`, which is the minimum value
            // that could be added to `start` or `other_start` to make them invalid.
            unsafe {
                if self.time.value_unchecked(self_index) != other.time.value_unchecked(other_index)
                    || self.subsort.value_unchecked(self_index)
                        != other.subsort.value_unchecked(other_index)
                    || self.key_hash.value_unchecked(self_index)
                        != other.key_hash.value_unchecked(other_index)
                {
                    break;
                }
            }

            length += 1;
        }
        length
    }
}

/// The result of [binary_merge].
#[derive(Debug, PartialEq)]
pub struct BinaryMergeResult {
    /// The merged time column.
    pub time: TimestampNanosecondArray,
    /// The merged subsort column.
    pub subsort: UInt64Array,
    /// The merged key_hash column.
    pub key_hash: UInt64Array,
    /// A column of (nullable) integers which may be used with
    /// [arrow::compute::take] to convert from an array of values associated
    /// with the keys of `a` to the merged result.
    pub take_a: UInt64Array,
    /// A column of (nullable) integers which may be used with
    /// [arrow::compute::take] to convert from an array of values associated
    /// with the keys of `b` to the merged result.
    pub take_b: UInt64Array,
}

/// A key to be merged. Ordering is lexicographic on `(time, subsort,
/// key_hash)`.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
struct Key {
    time: i64,
    subsort: u64,
    key_hash: u64,
}

impl Key {
    #[cfg(test)]
    fn new(time: i64, subsort: u64, key_hash: u64) -> Self {
        Self {
            time,
            subsort,
            key_hash,
        }
    }
}

/// Perform a 2-way merge operation between the two inputs.
///
/// The result contains the merged key columns and take indices for
/// producing the merged results from the other columns of `a` and `b`.
///
/// # Ideas to Explore
/// - This requires the inputs be concatenated before calling binary merge. We
///   could instead take a `SmallVec` of segments for each input, and perform
///   the concatenation while doing the merge. This may increase cost
///   (additional logic in merge) or reduce cost (less allocations).
/// - N-way merge operation, as implemented in `arrow2`.
pub fn binary_merge<'a>(
    a: BinaryMergeInput<'a>,
    b: BinaryMergeInput<'a>,
) -> anyhow::Result<BinaryMergeResult> {
    // The actual length will be `(a.len() + b.len()) - overlap`, where
    // `overlap` is the number of keys in common between `a` and `b`.
    let max_len = a.len() + b.len();

    let mut time_builder = TimestampNanosecondArray::builder(max_len);
    let mut subsort_builder = UInt64Array::builder(max_len);
    let mut key_hash_builder = UInt64Array::builder(max_len);

    let mut take_a_builder = UInt64Array::builder(max_len);
    let mut take_b_builder = UInt64Array::builder(max_len);

    if a.is_empty() {
        time_builder.append_slice(b.time.values());
        subsort_builder.append_slice(b.subsort.values());
        key_hash_builder.append_slice(b.key_hash.values());

        let len = b.len();
        take_a_builder.append_nulls(len);
        // SAFETY: The range has a trusted length.
        unsafe { take_b_builder.append_trusted_len_iter(0..len as u64) };

        return Ok(BinaryMergeResult {
            time: time_builder.finish(),
            subsort: subsort_builder.finish(),
            key_hash: key_hash_builder.finish(),
            take_a: take_a_builder.finish(),
            take_b: take_b_builder.finish(),
        });
    } else if b.is_empty() {
        time_builder.append_slice(a.time.values());
        subsort_builder.append_slice(a.subsort.values());
        key_hash_builder.append_slice(a.key_hash.values());

        let len = a.len();
        take_b_builder.append_nulls(len);
        // SAFETY: The range has a trusted length.
        unsafe { take_a_builder.append_trusted_len_iter(0..len as u64) };

        return Ok(BinaryMergeResult {
            time: time_builder.finish(),
            subsort: subsort_builder.finish(),
            key_hash: key_hash_builder.finish(),
            take_a: take_a_builder.finish(),
            take_b: take_b_builder.finish(),
        });
    };

    let mut a_index = 0;
    let mut b_index = 0;

    // SAFETY: `a` is not empty and `a_index` is 0.
    let mut a_key = unsafe { a.key_unchecked(a_index) };
    // SAFETY: `b` is not empty and `b_index` is 0.
    let mut b_key = unsafe { b.key_unchecked(b_index) };

    // Handle the main part of the merge.
    loop {
        match a_key.cmp(&b_key) {
            Ordering::Less => {
                // Take elements from a.
                let run_length = a.run_length_lt(a_index, &b_key);

                append_keys(
                    &a,
                    a_index,
                    run_length,
                    &mut time_builder,
                    &mut subsort_builder,
                    &mut key_hash_builder,
                )?;
                append_values(a_index, run_length, &mut take_a_builder)?;
                append_nulls(run_length, &mut take_b_builder)?;

                a_index += run_length;
                if a_index == a.len() {
                    break;
                } else {
                    debug_assert!(a_index < a.len());

                    // SAFETY: `a_index + run_length <= len` (run length can't
                    // exceed remainder of array). Has been increased by run length
                    // but we checked it is not equal to `len`, so it must be less.
                    a_key = unsafe { a.key_unchecked(a_index) };
                }
            }
            Ordering::Equal => {
                let run_length = a.run_length_eq(a_index, &b, b_index);

                append_keys(
                    &a,
                    a_index,
                    run_length,
                    &mut time_builder,
                    &mut subsort_builder,
                    &mut key_hash_builder,
                )?;
                append_values(a_index, run_length, &mut take_a_builder)?;
                append_values(b_index, run_length, &mut take_b_builder)?;

                a_index += run_length;
                b_index += run_length;
                if a_index == a.len() || b_index == b.len() {
                    break;
                } else {
                    debug_assert!(a_index < a.len());
                    debug_assert!(b_index < b.len());

                    // SAFETY: `a_index + run_length <= len` (run length can't
                    // exceed remainder of array). Has been increased by run length
                    // but we checked it is not equal to `len`, so it must be less.
                    a_key = unsafe { a.key_unchecked(a_index) };
                    // SAFETY: `b_index + run_length <= len` (run length can't
                    // exceed remainder of array). Has been increased by run length
                    // but we checked it is not equal to `len`, so it must be less.
                    b_key = unsafe { b.key_unchecked(b_index) };
                }
            }
            Ordering::Greater => {
                // Take elements from b.
                let run_length = b.run_length_lt(b_index, &a_key);
                append_keys(
                    &b,
                    b_index,
                    run_length,
                    &mut time_builder,
                    &mut subsort_builder,
                    &mut key_hash_builder,
                )?;
                append_nulls(run_length, &mut take_a_builder)?;
                append_values(b_index, run_length, &mut take_b_builder)?;

                b_index += run_length;
                if b_index == b.len() {
                    break;
                } else {
                    debug_assert!(b_index < b.len());

                    // SAFETY: `b_index + run_length <= len` (run length can't
                    // exceed remainder of array). Has been increased by run length
                    // but we checked it is not equal to `len`, so it must be less.
                    b_key = unsafe { b.key_unchecked(b_index) };
                }
            }
        }
    }

    // Handle the tail part of the merge.
    if a_index < a.len() {
        // Take remaining values from `a`.
        let run_length = a.len() - a_index;

        append_keys(
            &a,
            a_index,
            run_length,
            &mut time_builder,
            &mut subsort_builder,
            &mut key_hash_builder,
        )?;
        append_values(a_index, run_length, &mut take_a_builder)?;
        append_nulls(run_length, &mut take_b_builder)?;
    } else if b_index < b.len() {
        // Take remaining values from `a`.
        let run_length = b.len() - b_index;

        append_keys(
            &b,
            b_index,
            run_length,
            &mut time_builder,
            &mut subsort_builder,
            &mut key_hash_builder,
        )?;
        append_nulls(run_length, &mut take_a_builder)?;
        append_values(b_index, run_length, &mut take_b_builder)?;
    }

    Ok(BinaryMergeResult {
        time: time_builder.finish(),
        subsort: subsort_builder.finish(),
        key_hash: key_hash_builder.finish(),
        take_a: take_a_builder.finish(),
        take_b: take_b_builder.finish(),
    })
}

/// Append a range of values from the given input to the builders.
#[inline]
fn append_keys(
    input: &BinaryMergeInput<'_>,
    start: usize,
    len: usize,
    time_builder: &mut TimestampNanosecondBuilder,
    subsort_builder: &mut UInt64Builder,
    key_hash_builder: &mut UInt64Builder,
) -> anyhow::Result<()> {
    let end = start + len;

    time_builder.append_slice(&input.time.values()[start..end]);
    subsort_builder.append_slice(&input.subsort.values()[start..end]);
    key_hash_builder.append_slice(&input.key_hash.values()[start..end]);

    Ok(())
}

#[inline]
fn append_nulls(len: usize, take_builder: &mut UInt64Builder) -> anyhow::Result<()> {
    take_builder.append_nulls(len);

    Ok(())
}

#[inline]
fn append_values(
    start: usize,
    len: usize,
    take_builder: &mut UInt64Builder,
) -> Result<(), ArrowError> {
    let start = start as u64;
    let end = start + len as u64;

    // SAFETY: The range has a trusted length.
    unsafe { take_builder.append_trusted_len_iter(start..end) };

    Ok(())
}

#[cfg(test)]
mod tests {

    use arrow_array::{Array, TimestampNanosecondArray, UInt64Array};
    use arrow_select::filter::FilterBuilder;
    use proptest::prelude::*;

    use super::*;
    use crate::old::testing::arb_key_triples;

    #[test]
    fn run_length_lt_test() {
        let time = TimestampNanosecondArray::from_iter_values([0, 0, 0, 0]);
        let subsort = UInt64Array::from_iter_values([0, 0, 0, 0]);
        let key_hash = UInt64Array::from_iter_values([1, 2, 3, 4]);
        let input = BinaryMergeInput::new(&time, &subsort, &key_hash);

        // input[2] = (0, 0, 3)
        // input[3] = (0, 0, 4)
        // Run = 2..3
        assert_eq!(input.run_length_lt(2, &Key::new(0, 0, 4)), 1);
    }

    const TAKE_CHOICES: &[(bool, bool)] = &[(true, true), (false, true), (true, false)];

    fn arb_take_arrays(len: usize) -> impl Strategy<Value = (UInt64Array, UInt64Array)> {
        prop::collection::vec(prop::sample::select(TAKE_CHOICES), len).prop_map(|take_bools| {
            let take_a = take_bools
                .iter()
                .map(|(take_a, _)| *take_a)
                .scan(0, |count, include| {
                    let result = if include {
                        *count += 1;
                        Some(*count - 1)
                    } else {
                        None
                    };
                    Some(result)
                })
                .collect();

            let take_b = take_bools
                .iter()
                .map(|(_, take_b)| *take_b)
                .scan(0, |count, include| {
                    let result = if include {
                        *count += 1;
                        Some(*count - 1)
                    } else {
                        None
                    };
                    Some(result)
                })
                .collect();
            (take_a, take_b)
        })
    }

    /// Randomly generate merge *results* which can be used to compute the merge
    /// inputs.
    fn arb_merge_result(max_len: usize) -> impl Strategy<Value = BinaryMergeResult> {
        (0..max_len)
            .prop_flat_map(|len| (arb_key_triples(len), arb_take_arrays(len)))
            .prop_map(
                |((time, subsort, key_hash), (take_a, take_b))| BinaryMergeResult {
                    time,
                    subsort,
                    key_hash,
                    take_a,
                    take_b,
                },
            )
    }

    fn arb_non_empty_merge_result(max_len: usize) -> impl Strategy<Value = BinaryMergeResult> {
        arb_merge_result(max_len).prop_filter("inputs must be non-empty", |r| {
            r.take_a.null_count() < r.take_a.len() && r.take_b.null_count() < r.take_b.len()
        })
    }

    fn create_input<'a>(
        result: &'a BinaryMergeResult,
        take_indices: &'a UInt64Array,
    ) -> (ArrayRef, ArrayRef, ArrayRef) {
        use arrow_arith::boolean::is_not_null;

        let is_valid = is_not_null(take_indices).unwrap();
        let filter = FilterBuilder::new(&is_valid).optimize().build();

        let time = filter.filter(&result.time).unwrap();
        let subsort = filter.filter(&result.subsort).unwrap();
        let key_hash = filter.filter(&result.key_hash).unwrap();

        (time, subsort, key_hash)
    }

    // Property tests for the run-length function and other helpers on the key
    // triples. Generates arbitrary key sequences, and makes sure that the
    // run-length function at every point returns the correct result.
    proptest! {
        // Verify that key comparisons work as expected.
        #[test]
        fn test_compare_index(keys in arb_key_triples(2..100)) {
            let input = BinaryMergeInput::new(&keys.0, &keys.1, &keys.2);
            for i in 1..(input.len() - 1) {
                let key_i = input.key(i);

                prop_assert_eq!(
                    // SAFETY: i >= 1, so i-1 is in bounds.
                    unsafe { input.compare_indices_unchecked(i - 1, &key_i) }, Ordering::Less,
                    "Key {} < Key {}", i - 1 , i);
                prop_assert_eq!(
                    // SAFETY: i in [1..len - 1), so all indices are in bounds.
                    unsafe { input.compare_indices_unchecked(i, &key_i) }, Ordering::Equal,
                    "Key {} == Key {}", i, i);
                prop_assert_eq!(
                    // SAFETY: i < len - 1, so i + 1 < len, which is in bounds.
                    unsafe { input.compare_indices_unchecked(i + 1, &key_i) }, Ordering::Greater,
                    "Key {} > Key {}", i + 1, i);
            }
        }

        // Verify that when used with keys that are present, the run_length_lt is correct.
        #[test]
        fn test_run_length_lt_present(keys in arb_key_triples(0..100)) {
            let input = BinaryMergeInput::new(&keys.0, &keys.1, &keys.2);
            for i in 0..input.len() {
                for j in (i+1)..input.len() {
                    let key = input.key(j);
                    prop_assert_eq!(input.run_length_lt(i, &key), j - i,
                      "Incorrect run length starting at {} up to {:?}: expected {}",
                      i, key, j - i);
                }
            }
        }

        // Verify that when used with keys that may or may not be present, the run_length_lt is correct.
        // We generate two sets of key triples, and search in one for keys from the other.
        #[test]
        fn test_run_length_lt_random(keys in arb_key_triples(0..100), search in arb_key_triples(0..100)) {
            let input = BinaryMergeInput::new(&keys.0, &keys.1, &keys.2);
            let search = BinaryMergeInput::new(&search.0, &search.1, &search.2);

            let mut search_start = 0;
            for input_index in 0..input.len() {
                let input_key = input.key(input_index);

                // For each input, we consider all keys in `search` that are larger.
                while search_start < search.len() && search.key(search_start) <= input_key {
                    search_start += 1;
                }

                for search_index in search_start..search.len() {
                    let search_key = search.key(search_index);
                    let run_length = input.run_length_lt(input_index, &search_key);

                    let last_included = input.key(input_index + run_length - 1);
                    prop_assert!(last_included < search_key);

                    if input_index + run_length < input.len() {
                        let first_excluded = input.key(input_index + run_length);
                        prop_assert!(first_excluded >= search_key);
                    }
                }
            }
        }

        // TODO: Add property test for `run_length_eq`. This one is tricky since we need to
        // generate nearly equal sequences.
    }

    // Property tests for the merge behavior.
    //
    // Generates arbitrary merge results (including take_a and take_b masks),
    // determines the corresponding inputs (using those masks as filters), and
    // ensures that the result of merging those inputs is the same as the
    // generated result.
    proptest! {

        // 1024 random tests for result lengths 1..100
        #![proptest_config(ProptestConfig {
            cases: 1024, .. ProptestConfig::default()
          })]
        #[test]
        fn test_merge_property_100(merge_result in arb_non_empty_merge_result(100)) {
            // Create the merge inputs from the merge result.
            //
            // To do this, we filter the keys to the non-null rows of the take indices.

            let (a_time, a_subsort, a_key_hash) = create_input(&merge_result, &merge_result.take_a);
            let (b_time, b_subsort, b_key_hash) = create_input(&merge_result, &merge_result.take_b);

            let a_input = BinaryMergeInput::from_array_refs(&a_time, &a_subsort, &a_key_hash).unwrap();
            let b_input = BinaryMergeInput::from_array_refs(&b_time, &b_subsort, &b_key_hash).unwrap();

            prop_assume!(!a_input.is_empty() && !b_input.is_empty());

            let actual = binary_merge(a_input, b_input).unwrap();
            prop_assert_eq!(actual, merge_result)
        }

        // 256 (default) random tests for result lengths 1..1000
        #[test]
        fn test_merge_property_1000(merge_result in arb_non_empty_merge_result(1000)) {
            // Create the merge inputs from the merge result.
            //
            // To do this, we filter the keys to the non-null rows of the take indices.

            let (a_time, a_subsort, a_key_hash) = create_input(&merge_result, &merge_result.take_a);
            let (b_time, b_subsort, b_key_hash) = create_input(&merge_result, &merge_result.take_b);

            let a_input = BinaryMergeInput::from_array_refs(&a_time, &a_subsort, &a_key_hash).unwrap();
            let b_input = BinaryMergeInput::from_array_refs(&b_time, &b_subsort, &b_key_hash).unwrap();

            prop_assume!(!a_input.is_empty() && !b_input.is_empty());

            let actual = binary_merge(a_input, b_input).unwrap();
            prop_assert_eq!(actual, merge_result)
        }
    }
}
