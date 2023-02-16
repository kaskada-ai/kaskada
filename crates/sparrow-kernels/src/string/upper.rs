use std::convert::Infallible;

use arrow::array::StringArray;

/// Return the upper-case string representation of each element.
pub fn upper(base: &StringArray) -> Result<StringArray, Infallible> {
    Ok(base
        .iter()
        .map(|opt_s| opt_s.map(|s| s.to_uppercase()))
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
            Some("HELLO"),
            None,
            Some("WORLD"),
            Some("HELLO"),
            None,
        ]);

        let actual = upper(&array).unwrap();
        assert_eq!(expected, actual);
    }
}
