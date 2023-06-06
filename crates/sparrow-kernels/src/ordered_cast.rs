use std::sync::Arc;

use anyhow::anyhow;
use arrow::array::{ArrayRef, Int16Array, Int32Array, Int64Array, Int8Array, UInt64Array};
use arrow::datatypes::DataType;
use sparrow_arrow::downcast::downcast_primitive_array;

pub fn order_preserving_cast_to_u64(array: &ArrayRef) -> anyhow::Result<ArrayRef> {
    match array.data_type() {
        DataType::UInt64 => Ok(array.clone()),
        DataType::UInt32 => Ok(arrow::compute::cast(array, &DataType::UInt64)?),
        DataType::UInt16 => Ok(arrow::compute::cast(array, &DataType::UInt64)?),
        DataType::UInt8 => Ok(arrow::compute::cast(array, &DataType::UInt64)?),
        DataType::Int8 => {
            // Convert to u64 preserving ordering.
            // For a signed integer type, we have computed the "offset" to add which
            // corresponds to the amount we need to shift numbers to eliminate negatives.
            //
            // With two bits, unsigned range would be `[0, 3]`:
            //    `00 = 0, 01 = 1, 10 = 2, 11 = 3`.
            //
            // With two bits, signed range would be `[-2, 1]`:
            //    `10 = -2, 11 = -1, 00 = 0, 01 = 1`.
            //
            // We compute the offset as the maximum signed value the type represented plus
            // 1. This corresponds to the absolute difference between the
            // minimum (most negative) value and 0.
            const OFFSET: u64 = (i8::MAX as u64) + 1;
            let input: &Int8Array = downcast_primitive_array(array.as_ref())?;
            let output: UInt64Array =
                arrow::compute::unary(input, |n| (n as u64).wrapping_add(OFFSET));
            Ok(Arc::new(output))
        }
        DataType::Int16 => {
            const OFFSET: u64 = (i16::MAX as u64) + 1;
            let input: &Int16Array = downcast_primitive_array(array.as_ref())?;
            let output: UInt64Array =
                arrow::compute::unary(input, |n| (n as u64).wrapping_add(OFFSET));
            Ok(Arc::new(output))
        }
        DataType::Int32 => {
            const OFFSET: u64 = (i32::MAX as u64) + 1;
            let input: &Int32Array = downcast_primitive_array(array.as_ref())?;
            let output: UInt64Array =
                arrow::compute::unary(input, |n| (n as u64).wrapping_add(OFFSET));
            Ok(Arc::new(output))
        }
        DataType::Int64 => {
            const OFFSET: u64 = (i64::MAX as u64) + 1;
            let input: &Int64Array = downcast_primitive_array(array.as_ref())?;
            let output: UInt64Array =
                arrow::compute::unary(input, |n| (n as u64).wrapping_add(OFFSET));
            Ok(Arc::new(output))
        }
        unsupported => Err(anyhow!(
            "Unsupported input type {:?} to order_preserving_cast_to_u64",
            unsupported
        )),
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;

    #[test]
    fn test_order_preserving_cast_i8_to_u64() {
        let input = Int8Array::from(vec![i8::MIN, -8, -1, 0, 1, i8::MAX]);
        assert!(
            input.values().iter().tuples().all(|(a, b)| a < b),
            "Input should be sorted"
        );
        let input: ArrayRef = Arc::new(input);

        let output = order_preserving_cast_to_u64(&input).unwrap();
        let output: &UInt64Array = downcast_primitive_array(output.as_ref()).unwrap();

        // Verify output is still sorted
        assert!(
            output.values().iter().tuples().all(|(a, b)| a < b),
            "Output should be sorted"
        );

        // Spot check a few values
        assert_eq!(output.values()[0], 0);
        assert_eq!(output.values()[output.len() - 1], (1 << 8) - 1);
    }

    #[test]
    fn test_order_preserving_cast_i64_to_u64() {
        let input = Int64Array::from(vec![i64::MIN, -1000, -8, -1, 0, 1, 1000, i64::MAX]);
        assert!(
            input.values().iter().tuples().all(|(a, b)| a < b),
            "Input should be sorted"
        );
        let input: ArrayRef = Arc::new(input);

        let output = order_preserving_cast_to_u64(&input).unwrap();
        let output: &UInt64Array = downcast_primitive_array(output.as_ref()).unwrap();

        // Verify output is still sorted
        assert!(
            output.values().iter().tuples().all(|(a, b)| a < b),
            "Output should be sorted"
        );

        // Spot check a few values
        assert_eq!(output.values()[0], 0);
        assert_eq!(output.values()[output.len() - 1], u64::MAX);
    }
}
