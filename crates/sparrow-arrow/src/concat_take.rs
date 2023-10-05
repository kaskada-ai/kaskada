use arrow_array::{Array, ArrayRef, UInt32Array};

/// Concatenates two arrays and then takes the values at the given indices.
///
/// This method can be improved by not concatenating first, instead determining if
/// the indices are solely in the first or second array and then taking the values from
/// the respective arrays.
pub fn concat_take(
    array1: &dyn Array,
    array2: &dyn Array,
    indices: &UInt32Array,
) -> anyhow::Result<ArrayRef> {
    let combined = arrow_select::concat::concat(&[array1, array2])?;
    Ok(arrow_select::take::take(&combined, indices, None)?)
}
