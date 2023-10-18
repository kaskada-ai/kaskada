use std::sync::Arc;

use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, StringArray};
use itertools::izip;
use substring::Substring;

use sparrow_interfaces::expression::{
    Error, Evaluator, PrimitiveValue, StaticInfo, StringValue, WorkArea,
};

inventory::submit!(sparrow_interfaces::expression::EvaluatorFactory {
    name: "substring",
    create: &create
});

/// Evaluator for substring.
struct SubstringEvaluator {
    input: StringValue,
    start: PrimitiveValue<Int64Type>,
    end: PrimitiveValue<Int64Type>,
}

impl Evaluator for SubstringEvaluator {
    fn evaluate(&self, info: &WorkArea<'_>) -> error_stack::Result<ArrayRef, Error> {
        let input = info.expression(self.input);
        let start = info.expression(self.start);
        let end = info.expression(self.end);
        let result = substring(input, start, end);
        Ok(Arc::new(result))
    }
}

fn create(info: StaticInfo<'_>) -> error_stack::Result<Box<dyn Evaluator>, Error> {
    let (input, start, end) = info.unpack_arguments()?;
    Ok(Box::new(SubstringEvaluator {
        input: input.string()?,
        start: start.primitive()?,
        end: end.primitive()?,
    }))
}

/// Return a substring of `base` specified by the `start` and `end` indices.
/// `start` is inclusive and `end` is exclusive. The `start` and `end` may be
/// left `null` to indicate the first and last indices of the string,
/// respectively. Negative indices may also be passed, and are interpreted as
/// counting backwards from the end of the string. E.g. `str[-1] ==
/// str[length-1]`.
///
/// If `end > start`, an empty string is returned.
pub fn substring(base: &StringArray, start: &Int64Array, end: &Int64Array) -> StringArray {
    izip!(base.iter(), start.iter(), end.iter())
        .map(|(string, start, end)| {
            string.map(|s| {
                let strlen = s.chars().count() as i64;
                let start = normalize_index(strlen, start.unwrap_or(0));
                let end = normalize_index(strlen, end.unwrap_or(strlen));
                s.substring(start as usize, end as usize)
            })
        })
        .collect()
}

fn normalize_index(strlen: i64, index: i64) -> i64 {
    if index < 0 {
        strlen + index
    } else {
        index
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity() {
        let array = StringArray::from(vec![Some("hello")]);
        let start = Int64Array::from(vec![None]);
        let end = Int64Array::from(vec![None]);
        let expected = StringArray::from(vec![Some("hello")]);
        let actual = substring(&array, &start, &end);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_end_after_start() {
        let array = StringArray::from(vec![Some("hello")]);
        let start = Int64Array::from(vec![Some(3)]);
        let end = Int64Array::from(vec![Some(2)]);
        let expected = StringArray::from(vec![Some("")]);
        let actual = substring(&array, &start, &end);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_implicit_start() {
        let array = StringArray::from(vec![Some("hello")]);
        let start = Int64Array::from(vec![None]);
        let end = Int64Array::from(vec![Some(2)]);
        let expected = StringArray::from(vec![Some("he")]);
        let actual = substring(&array, &start, &end);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_implicit_end() {
        let array = StringArray::from(vec![Some("hello")]);
        let start = Int64Array::from(vec![Some(2)]);
        let end = Int64Array::from(vec![None]);
        let expected = StringArray::from(vec!["llo"]);
        let actual = substring(&array, &start, &end);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_substring() {
        let array = StringArray::from(vec![Some("hello")]);
        let start = Int64Array::from(vec![Some(1)]);
        let end = Int64Array::from(vec![Some(4)]);
        let expected = StringArray::from(vec!["ell"]);
        let actual = substring(&array, &start, &end);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_substrings() {
        let array = StringArray::from(vec![Some("hello"), Some("another")]);
        let start = Int64Array::from(vec![Some(1), Some(3)]);
        let end = Int64Array::from(vec![Some(4), None]);
        let expected = StringArray::from(vec!["ell", "ther"]);
        let actual = substring(&array, &start, &end);
        assert_eq!(expected, actual);
    }

    #[test]
    fn test_negative_indices() {
        let array = StringArray::from(vec![Some("hello"), Some("again"), Some("another")]);
        let start = Int64Array::from(vec![Some(-2), None, Some(-4)]);
        let end = Int64Array::from(vec![None, Some(-3), Some(-2)]);
        let expected = StringArray::from(vec!["lo", "ag", "th"]);
        let actual = substring(&array, &start, &end);
        assert_eq!(expected, actual);
    }
}
