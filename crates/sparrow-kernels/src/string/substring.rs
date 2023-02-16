use std::convert::Infallible;

use arrow::array::{Int32Array, StringArray};
use itertools::izip;
use substring::Substring;

/// Return a substring of `base` specified by the `start` and `end` indices.
/// `start` is inclusive and `end` is exclusive. The `start` and `end` may be
/// left `null` to indicate the first and last indices of the string,
/// respectively. Negative indices may also be passed, and are interpreted as
/// counting backwards from the end of the string. E.g. `str[-1] ==
/// str[length-1]`.
///
/// If `end > start`, an empty string is returned.
pub fn substring(
    base: &StringArray,
    start: &Int32Array,
    end: &Int32Array,
) -> Result<StringArray, Infallible> {
    let result: StringArray = izip!(base.iter(), start.iter(), end.iter())
        .map(|(string, start, end)| {
            string.map(|s| {
                let strlen = len(s);
                let start = normalize_index(strlen, start.unwrap_or(0));
                let end = normalize_index(strlen, end.unwrap_or(strlen));
                s.substring(start as usize, end as usize)
            })
        })
        .collect();
    Ok(result)
}

fn len(string: &str) -> i32 {
    string.chars().count() as i32
}

fn normalize_index(strlen: i32, index: i32) -> i32 {
    if index < 0 {
        strlen + index
    } else {
        index
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;

    use super::*;

    #[test]
    fn test_identity() {
        let array = StringArray::from(vec![Some("hello")]);
        let start = Int32Array::from(vec![None]);
        let end = Int32Array::from(vec![None]);
        let expected = StringArray::from(vec![Some("hello")]);
        let actual = substring(&array, &start, &end).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_end_after_start() {
        let array = StringArray::from(vec![Some("hello")]);
        let start = Int32Array::from(vec![Some(3)]);
        let end = Int32Array::from(vec![Some(2)]);
        let expected = StringArray::from(vec![Some("")]);
        let actual = substring(&array, &start, &end).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_implicit_start() {
        let array = StringArray::from(vec![Some("hello")]);
        let start = Int32Array::from(vec![None]);
        let end = Int32Array::from(vec![Some(2)]);
        let expected = StringArray::from(vec![Some("he")]);
        let actual = substring(&array, &start, &end).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_implicit_end() {
        let array = StringArray::from(vec![Some("hello")]);
        let start = Int32Array::from(vec![Some(2)]);
        let end = Int32Array::from(vec![None]);
        let expected = StringArray::from(vec!["llo"]);
        let actual = substring(&array, &start, &end).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_substring() {
        let array = StringArray::from(vec![Some("hello")]);
        let start = Int32Array::from(vec![Some(1)]);
        let end = Int32Array::from(vec![Some(4)]);
        let expected = StringArray::from(vec!["ell"]);
        let actual = substring(&array, &start, &end).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_substrings() {
        let array = StringArray::from(vec![Some("hello"), Some("another")]);
        let start = Int32Array::from(vec![Some(1), Some(3)]);
        let end = Int32Array::from(vec![Some(4), None]);
        let expected = StringArray::from(vec!["ell", "ther"]);
        let actual = substring(&array, &start, &end).unwrap();
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_negative_indices() {
        let array = StringArray::from(vec![Some("hello"), Some("again"), Some("another")]);
        let start = Int32Array::from(vec![Some(-2), None, Some(-4)]);
        let end = Int32Array::from(vec![None, Some(-3), Some(-2)]);
        let expected = StringArray::from(vec!["lo", "ag", "th"]);
        let actual = substring(&array, &start, &end).unwrap();
        assert_eq!(expected, actual);
    }
}
