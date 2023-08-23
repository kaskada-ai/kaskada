use edit_distance::edit_distance;
use itertools::Itertools;

/// The nearest matches to a given name.
#[derive(Debug, PartialEq, Clone)]
pub struct NearestMatches<T>(Vec<T>);

impl<T: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static> error_stack::Context
    for NearestMatches<T>
{
}

impl<T> Default for NearestMatches<T> {
    fn default() -> Self {
        Self(vec![])
    }
}

impl<T> NearestMatches<T> {
    pub fn map<T2>(self, f: impl Fn(T) -> T2) -> NearestMatches<T2> {
        NearestMatches(self.0.into_iter().map(f).collect())
    }
}

impl<'a> NearestMatches<&'a str> {
    /// Create a set of nearest matches for a given string.
    pub fn new_nearest_strs(query: &str, items: impl Iterator<Item = &'a str> + 'a) -> Self {
        let nearest_matches: Vec<_> = items
            .map(|item| (edit_distance(query, item), item))
            .k_smallest(5)
            .map(|(_, item)| item)
            .collect();
        Self(nearest_matches)
    }
}

impl NearestMatches<String> {
    /// Create a set of nearest matches for a given string.
    pub fn new_nearest_strings(query: &str, items: impl Iterator<Item = String>) -> Self {
        let nearest_matches: Vec<_> = items
            .map(|item| (edit_distance(query, item.as_ref()), item))
            .k_smallest(5)
            .map(|(_, item)| item)
            .collect();
        Self(nearest_matches)
    }
}

impl<T: std::fmt::Display> std::fmt::Display for NearestMatches<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.0.is_empty() {
            write!(f, "none")
        } else {
            self.0
                .iter()
                .format_with(", ", |e, f| f(&format_args!("'{e}'")))
                .fmt(f)
        }
    }
}

impl<T> From<Vec<T>> for NearestMatches<T> {
    fn from(matches: Vec<T>) -> Self {
        Self(matches)
    }
}

impl<T> NearestMatches<T> {
    pub fn inner(self) -> Vec<T> {
        self.0
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nearest_matches_display() {
        insta::assert_display_snapshot!(NearestMatches::<&'static str>::from(vec![]), @"none");
        insta::assert_display_snapshot!(NearestMatches::from(vec!["foo"]), @"'foo'");
        insta::assert_display_snapshot!(NearestMatches::from(vec!["foo", "bar"]), @"'foo', 'bar'");
        insta::assert_display_snapshot!(NearestMatches::from(vec!["foo", "bar", "baz"]), @"'foo', 'bar', 'baz'");
    }
}
