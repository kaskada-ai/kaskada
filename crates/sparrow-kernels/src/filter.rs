//! Filter kernel that supports defined behaviors in the presence of null.
//!
//! This is currently a wrapper around the Arrow filter kernel which has
//! undefined behavior when the `BooleanArray` contains `null`.
//!
//! If this is a performance bottleneck we can create a specialized Filter
//! kernel that handles these, and/or use such a kernel when added to Arrow.
//! See: <https://issues.apache.org/jira/browse/ARROW-11846>.

use anyhow::Context;
use arrow::array::{Array, ArrayData, ArrayRef, BooleanArray};
use arrow::buffer::{bitwise_bin_op_helper, buffer_bin_and};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;

pub enum NullBehavior {
    /// Don't include rows where the filter is `null`.
    NullAsFalse,
    /// Include rows where the filter is `null`.
    NullAsTrue,
    /// Arbitrary behavior for rows when the filter is `null`.
    ///
    /// This corresponds to the default behavior of the Arrow filter operation,
    /// in which the null-bits are ignored and the underlying boolean bits
    /// are used regardless of whether they are meaningful.
    NullAsUndefined,
}

pub struct FilterConfig {
    pub null_behavior: NullBehavior,
}

impl Default for FilterConfig {
    fn default() -> Self {
        Self {
            null_behavior: NullBehavior::NullAsUndefined,
        }
    }
}

pub fn filter_with_config(
    array: &dyn Array,
    filter: &BooleanArray,
    config: FilterConfig,
) -> anyhow::Result<ArrayRef> {
    // Arrow's filter is undefined on arrays containing null values. So, we
    // explicitly replace null values with true/false.
    //
    // Opportunity: We could create a filter kernel for doing this without
    // materializing the intermediate boolean array.
    let result = match config.null_behavior {
        NullBehavior::NullAsUndefined => arrow::compute::kernels::filter::filter(array, filter)?,
        _ if filter.null_count() == 0 => arrow::compute::kernels::filter::filter(array, filter)?,
        NullBehavior::NullAsFalse => {
            arrow::compute::kernels::filter::filter(array, &null_to_false(filter)?)?
        }
        NullBehavior::NullAsTrue => {
            arrow::compute::kernels::filter::filter(array, &null_to_true(filter)?)?
        }
    };

    Ok(result)
}

pub fn filter_record_batch_with_config(
    record_batch: &RecordBatch,
    filter: &BooleanArray,
    config: FilterConfig,
) -> anyhow::Result<RecordBatch> {
    // Arrow's filter is undefined on arrays containing null values. So, we
    // explicitly replace null values with true/false.
    //
    // Opportunity: We could create a filter kernel for doing this without
    // materializing the intermediate boolean array.
    let result = match config.null_behavior {
        NullBehavior::NullAsUndefined => {
            arrow::compute::kernels::filter::filter_record_batch(record_batch, filter)?
        }
        _ if filter.null_count() == 0 => {
            arrow::compute::kernels::filter::filter_record_batch(record_batch, filter)?
        }
        NullBehavior::NullAsFalse => arrow::compute::kernels::filter::filter_record_batch(
            record_batch,
            &null_to_false(filter)?,
        )?,
        NullBehavior::NullAsTrue => arrow::compute::kernels::filter::filter_record_batch(
            record_batch,
            &null_to_true(filter)?,
        )?,
    };

    Ok(result)
}

fn null_to_false(boolean: &BooleanArray) -> anyhow::Result<BooleanArray> {
    let boolean_data = boolean.data();
    let valid_buffer = boolean_data
        .null_buffer()
        .context("null count > 0 should have a null buffer")?;
    let data_buffer = boolean.values();

    // The result is `data & !null`, or `data & valid`).
    let result_buffer = buffer_bin_and(data_buffer, 0, valid_buffer, 0, data_buffer.len() * 8);

    let result_data = ArrayData::try_new(
        DataType::Boolean,
        boolean.len(),
        None,
        boolean.offset(),
        vec![result_buffer],
        vec![],
    )?;
    let result = BooleanArray::from(result_data);
    Ok(result)
}

fn null_to_true(boolean: &BooleanArray) -> anyhow::Result<BooleanArray> {
    let boolean_data = boolean.data();
    let valid_buffer = boolean_data
        .null_buffer()
        .context("null count > 0 should have a null buffer")?;
    let data_buffer = boolean.values();

    // The result is `data | null`, or `data | !valid`).
    let result_buffer = bitwise_bin_op_helper(
        data_buffer,
        0,
        valid_buffer,
        0,
        data_buffer.len() * 8,
        |a, b| a | !b,
    );

    let result_data = ArrayData::try_new(
        DataType::Boolean,
        boolean.len(),
        None,
        boolean.offset(),
        vec![result_buffer],
        vec![],
    )?;
    let result = BooleanArray::from(result_data);
    Ok(result)
}

#[cfg(test)]
mod tests {
    use sparrow_arrow::downcast::downcast_boolean_array;

    use super::*;

    #[test]
    fn test_null_to_false() {
        let input = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            None,
            Some(false),
        ]);
        let actual = null_to_false(&input).unwrap();
        assert_eq!(
            actual,
            BooleanArray::from(vec![true, false, false, true, false, false])
        );
    }

    #[test]
    fn test_null_to_false_slice() {
        let input = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            None,
            Some(false),
        ]);
        let input_ref = input.slice(2, 4);
        let input_slice = downcast_boolean_array(input_ref.as_ref()).unwrap();
        let actual = null_to_false(input_slice).unwrap();
        assert_eq!(actual, BooleanArray::from(vec![false, true, false, false]));
    }

    #[test]
    fn test_null_to_true() {
        let input = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            None,
            Some(false),
        ]);
        let actual = null_to_true(&input).unwrap();
        assert_eq!(
            actual,
            BooleanArray::from(vec![true, false, true, true, true, false])
        );
    }

    #[test]
    fn test_null_to_true_slice() {
        let input = BooleanArray::from(vec![
            Some(true),
            Some(false),
            None,
            Some(true),
            None,
            Some(false),
        ]);
        let input_ref = input.slice(2, 4);
        let input_slice = downcast_boolean_array(input_ref.as_ref()).unwrap();
        let actual = null_to_true(input_slice).unwrap();
        assert_eq!(actual, BooleanArray::from(vec![true, true, true, false]));
    }

    // TODO: ADD TESTS FOR BEHAVIOR
    // TODO: ADD TESTS FOR TRUE
}
