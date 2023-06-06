use std::sync::Arc;

use arrow::array::{ArrayData, ArrayRef, StructArray};
use arrow::buffer::Buffer;
use arrow::datatypes::{DataType, FieldRef, Fields};

/// Create a StructArray with the given length and fields.
///
/// The field value arrays (if any) should have the same length.
///
/// This works around the fact that the `StructArray::from(fields)`
/// panics when provided 0 fields, which may happen when we don't
/// actually need any of the fields after projection pushdown.
///
/// https://github.com/apache/arrow-rs/issues/1657
pub fn make_struct_array(length: usize, fields: Vec<(FieldRef, ArrayRef)>) -> StructArray {
    // Check the fields have the expected length.
    #[cfg(debug_assertions)]
    {
        for (field, array) in fields.iter() {
            debug_assert_eq!(
                array.len(),
                length,
                "Array for field '{}' had unexpected length",
                field.name()
            );
        }
    }

    if fields.is_empty() {
        // Arrow can't create a struct array with no fields -- it wouldn't know the
        // length. So we create the ArrayData directly.
        let array_data = ArrayData::builder(DataType::Struct(Fields::empty())).len(length);
        // SAFETY: Array Data constructed correctly.
        let array_data = unsafe { array_data.build_unchecked() };
        StructArray::from(array_data)
    } else {
        StructArray::from(fields)
    }
}

/// Work around the fact that `new_null_array` panics on 0-field struct.
///
/// https://github.com/apache/arrow-rs/issues/1657
pub fn make_null_array(data_type: &DataType, length: usize) -> ArrayRef {
    if data_type == &DataType::Struct(Fields::empty()) {
        // Work around the fact that `new_null_array` panics for empty structs.
        // rewrite as `length.div_ceil(8)` once stablized.
        let num_bytes = arrow::util::bit_util::ceil(length, 8);
        let null_buffer = Buffer::from(vec![0x00; num_bytes]);

        let array_data = ArrayData::builder(DataType::Struct(Fields::empty()))
            .null_bit_buffer(Some(null_buffer))
            .len(length);
        // SAFETY: Array Data constructed correctly.
        let array_data = unsafe { array_data.build_unchecked() };
        Arc::new(StructArray::from(array_data))
    } else {
        arrow::array::new_null_array(data_type, length)
    }
}

/// Create a StructArray with the given length and fields.
///
/// The field value arrays (if any) should have the same length.
///
/// This works around the fact that the `StructArray::from(fields)`
/// panics when provided 0 fields, which may happen when we don't
/// actually need any of the fields after projection pushdown.
///
/// https://github.com/apache/arrow-rs/issues/1657
pub fn make_struct_array_null(
    length: usize,
    fields: Vec<(FieldRef, ArrayRef)>,
    null_buffer: Buffer,
) -> StructArray {
    // Check the fields have the expected length.
    #[cfg(debug_assertions)]
    {
        for (field, array) in fields.iter() {
            debug_assert_eq!(
                array.len(),
                length,
                "Array for field '{}' had unexpected length",
                field.name()
            );
        }
    }
    if fields.is_empty() {
        // Arrow can't create a struct array with no fields -- it wouldn't know the
        // length. So we create the ArrayData directly.
        let array_data = ArrayData::builder(DataType::Struct(Fields::empty()))
            .null_bit_buffer(Some(null_buffer))
            .len(length);
        // SAFETY: Array data constructed correctly.
        let array_data = unsafe { array_data.build_unchecked() };
        StructArray::from(array_data)
    } else {
        StructArray::from((fields, null_buffer))
    }
}
