use itertools::Itertools;
use serde::Serialize;
use smallvec::SmallVec;

/// Wrapper around a static string used as a category.
#[derive(Serialize, Eq, PartialEq, Debug)]
#[repr(transparent)]
pub(crate) struct Category(&'static str);

impl From<&'static str> for Category {
    fn from(string: &'static str) -> Self {
        Category(string)
    }
}

/// Vector of categories.
#[derive(Eq, PartialEq, Debug, Default)]
#[repr(transparent)]
pub(crate) struct Categories(SmallVec<[Category; 2]>);

impl Categories {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl<T> From<T> for Categories
where
    T: IntoIterator<Item = &'static str>,
{
    fn from(items: T) -> Self {
        Categories(items.into_iter().map(Category).collect())
    }
}

impl Serialize for Categories {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.collect_str(&self.0.iter().map(|cat| cat.0).format(","))
    }
}
