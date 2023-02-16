use std::convert::Infallible;

use arrow::array::StringArray;

/// Return the lower-cased string representation of the elements.
pub fn lower(base: &StringArray) -> Result<StringArray, Infallible> {
    Ok(base
        .iter()
        .map(|opt_s| opt_s.map(|s| s.to_lowercase()))
        .collect())
}

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;

    use super::*;

    #[test]
    fn test_hash_string() {
        let array = StringArray::from(vec![
            Some("hELLo"),
            None,
            Some("WORLD"),
            Some("hello"),
            None,
        ]);
        let expected = StringArray::from(vec![
            Some("hello"),
            None,
            Some("world"),
            Some("hello"),
            None,
        ]);

        let actual = lower(&array).unwrap();
        assert_eq!(expected, actual);
    }
}
