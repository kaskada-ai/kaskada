use arrow::array::ArrayRef;

/// Returns the time of each value in the `inputs` array. This requires the time
/// column to have no null values and returns a clone of the times array.
pub fn time_of(times: &ArrayRef) -> anyhow::Result<ArrayRef> {
    assert_eq!(0, times.null_count(), "Times should be non-null");
    Ok(times.clone())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, TimestampNanosecondArray};

    use crate::time::time_of;

    #[test]
    fn time_of_no_null_inputs() {
        let times: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![1, 2, 3, 4]));
        let result = time_of(&times).unwrap();

        let expected: TimestampNanosecondArray =
            TimestampNanosecondArray::from(vec![Some(1), Some(2), Some(3), Some(4)]);
        assert_eq!(result.as_ref(), &expected);
    }
}
