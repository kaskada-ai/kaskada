use edit_distance::edit_distance;
use itertools::Itertools;

/// Return a vector containing the up-to-5 nearest matches.
pub(crate) fn nearest_matches<T: AsRef<str> + Ord>(
    query: &str,
    items: impl Iterator<Item = T>,
) -> Vec<T> {
    items
        .map(|item| (edit_distance(query, item.as_ref()), item))
        .k_smallest(5)
        .map(|(_, item)| item)
        .collect()
}
