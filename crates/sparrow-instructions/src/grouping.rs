use arrow::array::{Array, UInt32Array};

/// Information about the grouping indices within a batch.
///
/// This information may be used by an instruction executor to allocate and
/// manage per-group storage.
#[derive(Debug)]
pub struct GroupingIndices {
    /// The total number of groups seen so far.
    num_groups: usize,
    /// For each row in the batch, an index into the group-storage.
    group_indices: UInt32Array,
}

impl GroupingIndices {
    pub fn new(num_groups: usize, group_indices: UInt32Array) -> Self {
        assert_eq!(group_indices.null_count(), 0);
        assert!(num_groups < (u32::MAX as usize));
        Self {
            num_groups,
            group_indices,
        }
    }

    pub fn new_empty() -> Self {
        Self {
            num_groups: 0,
            group_indices: UInt32Array::from_iter_values(vec![]),
        }
    }

    pub fn num_groups(&self) -> usize {
        self.num_groups
    }

    pub fn group_indices(&self) -> &UInt32Array {
        &self.group_indices
    }

    pub fn len(&self) -> usize {
        self.group_indices.len()
    }

    pub fn is_empty(&self) -> bool {
        self.group_indices.is_empty()
    }

    pub fn group_iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.group_indices.values().iter().map(|group| {
            let group = *group as usize;
            debug_assert!(group < self.num_groups());
            group
        })
    }

    pub fn slice(&self, offset: usize, length: usize) -> anyhow::Result<Self> {
        debug_assert!(offset + length <= self.group_indices.len());

        // Unfortunately, we can't call slice directly since that returns an
        // `ArrayRef`, and would never let us get back to an owned
        // `UInt32Array`. We could change this to accept either an `ArcRef` or
        // use lifetimes... but this was minimally invasive at the time.
        let group_indices: UInt32Array = self
            .group_indices
            .to_data()
            .into_builder()
            .offset(self.group_indices.offset() + offset)
            .len(length)
            .build()?
            .into();

        Ok(Self {
            num_groups: self.num_groups,
            group_indices,
        })
    }
}
