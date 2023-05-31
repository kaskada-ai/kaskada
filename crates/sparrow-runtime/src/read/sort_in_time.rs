use arrow::array::{Array, TimestampNanosecondArray, UInt32Array, UInt64Array};
use itertools::Itertools;
use sparrow_arrow::downcast::downcast_primitive_array;

/// Return indices from a record batch sorted by the three key columns.
///
/// This assumes the batch is already sorted by time and does not contain
/// duplicates.
///
/// This sorts each region of rows with equal times separately rather
/// than sorting the entire batch.
///
/// # Panics
/// In debug builds, this will panic if the batch is not already sorted by time.
pub(crate) fn sort_in_time_indices_dyn(
    time: &dyn Array,
    subsort: &dyn Array,
    key_hash: &dyn Array,
) -> anyhow::Result<UInt32Array> {
    sort_in_time_indices(
        downcast_primitive_array(time)?,
        downcast_primitive_array(subsort)?,
        downcast_primitive_array(key_hash)?,
    )
}

/// Return indices from a record batch sorted by the three key columns.
///
/// This assumes the batch is already sorted by time and does not contain
/// duplicates.
///
/// This sorts each region of rows with equal times separately rather
/// than sorting the entire batch.
///
/// # Panics
/// In debug builds, this will panic if the batch is not already sorted by time.
pub(crate) fn sort_in_time_indices(
    time_array: &TimestampNanosecondArray,
    subsort_array: &UInt64Array,
    key_hash_array: &UInt64Array,
) -> anyhow::Result<UInt32Array> {
    // TODO: Replace with `time.values().is_sorted()` once stabilized.
    debug_assert!(
        time_array
            .values()
            .iter()
            .tuple_windows()
            .all(|(a, b)| a <= b),
        "Expected time to be sorted"
    );

    // Create an iterator over chunks of equal times containing
    // `(start, end)` indices for each chunk. The end is exclusive.
    let time_chunks =
        time_array
            .values()
            .iter()
            .dedup_with_count()
            .scan(0, |state, (len, _time)| {
                let start = *state;
                *state += len;
                let end = *state;
                Some((start, end))
            });

    // Iterate over the chunks, sorting the indices by subsort/key_hash.
    let mut builder = UInt32Array::builder(time_array.len());
    for (start, end) in time_chunks {
        let mut indices: Vec<_> = (start as u32..end as u32).collect();
        debug_assert!(end <= subsort_array.len());
        debug_assert!(end <= key_hash_array.len());

        indices.sort_unstable_by(|m_index, n_index| {
            let m_index = *m_index as usize;
            let n_index = *n_index as usize;
            debug_assert!(m_index < end);
            debug_assert!(n_index < end);
            // SAFETY: `m_index < end <= subsort_array.len()`.
            let m_subsort = unsafe { subsort_array.value_unchecked(m_index) };
            // SAFETY: `n_index < end <= subsort_array.len()`.
            let n_subsort = unsafe { subsort_array.value_unchecked(n_index) };
            m_subsort.cmp(&n_subsort).then_with(|| {
                // SAFETY: `m_index < end <= key_hash_array.len()`.
                let m_key_hash = unsafe { key_hash_array.value_unchecked(m_index) };
                // SAFETY: `n_index < end <= key_hash_array.len()`.
                let n_key_hash = unsafe { key_hash_array.value_unchecked(n_index) };
                m_key_hash.cmp(&n_key_hash)
            })
        });

        builder.append_slice(&indices);
    }

    Ok(builder.finish())
}

#[cfg(test)]
mod tests {

    use arrow::array::{TimestampNanosecondArray, UInt64Array};

    use super::*;

    #[test]
    fn test_sort_in_time_indices() {
        let time = TimestampNanosecondArray::from(vec![1, 1, 1, 2, 3, 3, 5, 6, 6, 7, 7, 7]);
        let subsort = UInt64Array::from(vec![1, 3, 3, 0, 2, 3, 8, 3, 1, 3, 2, 2]);
        let key_hash = UInt64Array::from(vec![1, 3, 2, 1, 1, 1, 1, 1, 1, 1, 2, 1]);

        let indices = sort_in_time_indices(&time, &subsort, &key_hash).unwrap();
        assert_eq!(indices.values(), &[0, 2, 1, 3, 4, 5, 6, 8, 7, 11, 10, 9]);
    }
}
