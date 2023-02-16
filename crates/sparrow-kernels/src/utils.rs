use arrow::array::{Array, BooleanArray};

/// Iterator over the valid bits.
pub struct BitBufferIterator<'a> {
    buffer: &'a [u8],
    /// The current bit position.
    bit_pos: usize,
    /// End when bit_offset == bit_len.
    bit_end: usize,
}

impl<'a> BitBufferIterator<'a> {
    /// Create an iterator over bits for a specific buffer.
    pub fn new(buffer: &'a [u8], bit_offset: usize, bit_len: usize) -> Self {
        Self {
            buffer,
            bit_pos: bit_offset,
            bit_end: bit_offset + bit_len,
        }
    }

    /// Create an iterator over valid-bits for the given array.
    ///
    /// Returns `None` if there is no null-buffer.
    pub fn array_valid_bits(array: &'a dyn Array) -> Option<Self> {
        if array.null_count() == 0 {
            return None;
        }

        array
            .data_ref()
            .null_buffer()
            // The slice already incorporates `offset / 8`.
            .map(|valid_buffer| Self::new(valid_buffer.as_slice(), array.offset(), array.len()))
    }

    /// Create an iterator over the true/false bits of the given iterator.
    ///
    /// NOTE: This ignores the validity of the given array.
    pub fn boolean_array(array: &'a BooleanArray) -> Self {
        Self::new(array.values(), array.offset(), array.len())
    }
}

impl<'a> Iterator for BitBufferIterator<'a> {
    type Item = bool;

    // Inline leads to better performance
    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.bit_pos < self.bit_end {
            let is_set = (self.buffer[self.bit_pos / 8] >> (self.bit_pos % 8)) & 1;
            self.bit_pos += 1;
            Some(is_set != 0)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.bit_end - self.bit_pos;
        (len, Some(len))
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{as_boolean_array, as_primitive_array, UInt64Array};

    use super::*;

    #[test]
    fn test_valid_iterator_no_nulls() {
        let array = UInt64Array::from(vec![54, 87, 816, 763]);
        assert!(BitBufferIterator::array_valid_bits(&array).is_none());
    }

    #[test]
    fn test_valid_iterator() {
        let array = UInt64Array::from(vec![Some(54), None, Some(87), None, Some(816), Some(763)]);
        let actual: Vec<_> = BitBufferIterator::array_valid_bits(&array)
            .unwrap()
            .collect();
        assert_eq!(actual, vec![true, false, true, false, true, true]);
    }

    #[test]
    fn test_valid_iterator_slice_first_byte() {
        let array = UInt64Array::from(vec![Some(54), None, Some(87), None, Some(816), Some(763)]);
        let array = array.slice(1, 4);
        let array: &UInt64Array = as_primitive_array(&array);
        let actual: Vec<_> = BitBufferIterator::array_valid_bits(array)
            .unwrap()
            .collect();
        assert_eq!(actual, vec![false, true, false, true]);
    }

    #[test]
    fn test_valid_iterator_slice_first_byte_boundary() {
        let array = UInt64Array::from(vec![
            Some(54),
            None,
            Some(87),
            None,
            Some(816),
            Some(763),
            None,
            None,
            None,
            Some(875),
            None,
            Some(42),
        ]);
        let array = array.slice(8, 3);
        let array: &UInt64Array = as_primitive_array(&array);
        let actual: Vec<_> = BitBufferIterator::array_valid_bits(array)
            .unwrap()
            .collect();
        assert_eq!(actual, vec![false, true, false]);
    }

    #[test]
    fn test_valid_iterator_slice_second_byte() {
        let array = UInt64Array::from(vec![
            Some(54),
            None,
            Some(87),
            None,
            Some(816),
            Some(763),
            None,
            None,
            None,
            Some(875),
            None,
            Some(42),
        ]);
        let array = array.slice(9, 3);
        let array: &UInt64Array = as_primitive_array(&array);
        let actual: Vec<_> = BitBufferIterator::array_valid_bits(array)
            .unwrap()
            .collect();
        assert_eq!(actual, vec![true, false, true]);
    }

    #[test]
    fn test_boolean_iterator() {
        let array = BooleanArray::from(vec![true, false, true, false, true, true]);
        let actual: Vec<_> = BitBufferIterator::boolean_array(&array).collect();
        assert_eq!(actual, vec![true, false, true, false, true, true]);
    }

    #[test]
    fn test_boolean_iterator_slice_first_byte() {
        let array = BooleanArray::from(vec![true, false, true, false, true, true]);
        let array = array.slice(1, 4);
        let array: &BooleanArray = as_boolean_array(&array);
        let actual: Vec<_> = BitBufferIterator::boolean_array(array).collect();
        assert_eq!(actual, vec![false, true, false, true]);
    }

    #[test]
    fn test_boolean_iterator_slice_first_byte_boundary() {
        let array = BooleanArray::from(vec![
            true, false, true, false, true, true, false, false, false, true, false, true,
        ]);
        let array = array.slice(8, 3);
        let array: &BooleanArray = as_boolean_array(&array);
        let actual: Vec<_> = BitBufferIterator::boolean_array(array).collect();
        assert_eq!(actual, vec![false, true, false]);
    }

    #[test]
    fn test_boolean_iterator_slice_second_byte() {
        let array = BooleanArray::from(vec![
            true, false, true, false, true, true, false, false, false, true, false, true,
        ]);
        let array = array.slice(9, 3);
        let array: &BooleanArray = as_boolean_array(&array);
        let actual: Vec<_> = BitBufferIterator::boolean_array(array).collect();
        assert_eq!(actual, vec![true, false, true]);
    }
}
