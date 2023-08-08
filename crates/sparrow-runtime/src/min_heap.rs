use std::collections::BinaryHeap;

/// Trait for elements that have a priority and can be used in a [MinHeap].
pub(crate) trait HasPriority {
    type Priority: Ord + std::fmt::Debug;

    /// Return the priority of a given item.
    fn priority(&self) -> Self::Priority;
}

struct PriorityElement<T: HasPriority> {
    /// The priority associated with the item when it was added to the heap.
    priority: T::Priority,
    element: T,
}

impl<T: HasPriority> PriorityElement<T> {
    fn new(element: T) -> Self {
        let priority = element.priority();
        Self { priority, element }
    }
}

impl<T: HasPriority> PartialEq for PriorityElement<T> {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl<T: HasPriority> Eq for PriorityElement<T> {}

impl<T: HasPriority> PartialOrd for PriorityElement<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<T: HasPriority> Ord for PriorityElement<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.priority.cmp(&self.priority)
    }
}

/// A wrapper around a binary heap providing a min-heap.
#[repr(transparent)]
pub(crate) struct MinHeap<T: HasPriority>(BinaryHeap<PriorityElement<T>>);

impl<T: HasPriority> MinHeap<T> {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn push(&mut self, element: T) {
        self.0.push(PriorityElement::new(element))
    }

    pub fn pop(&mut self) -> Option<T> {
        self.0.pop().map(|t| {
            debug_assert_eq!(
                t.priority,
                t.element.priority(),
                "Priority changed while element was in heap"
            );
            t.element
        })
    }

    pub fn peek(&self) -> Option<&T> {
        self.0.peek().map(|element| &element.element)
    }
}

impl<T: HasPriority> std::iter::FromIterator<T> for MinHeap<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self(iter.into_iter().map(PriorityElement::new).collect())
    }
}

impl<T> From<Vec<T>> for MinHeap<T>
where
    T: HasPriority,
{
    fn from(input: Vec<T>) -> Self {
        input.into_iter().collect()
    }
}
